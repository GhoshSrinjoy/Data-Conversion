"""Quote-aware CSV records: chunking and tokenizing (the Python twin of the native
``chunk.rs`` and ``tokenize.rs``).

Every engine splits records with the same rules, so they all see the same rows:

* a quote opens a quoted field only at the start of a field (the start of a record or
  just after a delimiter); anywhere else it is a literal character, like the ``"`` in
  ``55" screen``;
* inside a quoted field a doubled quote (RFC 4180) or an escape character followed by
  any character (escape dialects) is literal, and the first other quote closes the
  field; text after the closing quote up to the next delimiter or line break stays
  with the field;
* records end at ``\\n``, ``\\r\\n`` or a lone ``\\r`` outside quotes;
* a quoted field still open at the end of the input, or ``MAX_QUOTED`` characters after
  its record started, is a stray quote: the record ends with the line that holds the
  opening quote and is malformed, and parsing resumes on the next line;
* a record is malformed too when a quoted field that spans lines has text after its
  closing quote: a stray quote paired up with a later one would otherwise merge
  several lines into one value (a one-line ``"12" monitor`` stays a value).

Everything works on ``str`` (the pure engine, the sniffer) and on ``bytes`` (the
arrow engine and DCZ) alike: the special characters are kept in the buffer's type.

Records whose every quote is regular (RFC 4180: it opens a field at its start, or
closes one before a delimiter, a line break or the end, or is doubled inside one) are
found at C speed from the parity of the quotes (:func:`regular_run`); the scans visit
quotes one by one only from the last record boundary before any other quote to the
end of its record.
"""

from __future__ import annotations

import re
from typing import Any, Iterator, List, Optional, Tuple

from .errors import DataError

#: A quoted field still open this many characters after its record started is taken
#: to be a stray quote (the native engine uses the same limit, in bytes).
MAX_QUOTED = 1 << 30
#: Records without any line break are read whole up to this size (or eight chunks).
MAX_LINE = 128 << 20
#: Characters the fast scan checks at a time (small enough to stay in the CPU caches).
CUT_WINDOW = 1 << 15
#: A record longer than this is left to the exact scan by the fast boundary scan.
MAX_FAST = 1 << 20

# Record terminator codes (the same numbering as the native tokenizer and DCZ).
LF, CRLF, CR, EOF = 0, 1, 2, 3
#: Record flags
IRREGULAR = 1  # a quoted field with text after its closing quote, or a stray quote
UNTERMINATED = 2  # a stray quote (never closed, or paired across lines): the record is malformed
MALFORMED = UNTERMINATED


class Dialect:
    """The characters that decide where fields and records end, in the buffer's type."""

    __slots__ = (
        "cr", "delimiter", "double_quote", "doubled", "empty", "escape", "escape_item", "fast", "lines", "nl",
        "odd_quote", "q_or_esc", "quote", "quote_item", "split", "split_cr", "start_items", "starts", "stop", "table",
        "text",
    )  # fmt: skip

    def __init__(
        self,
        delimiter: str = ",",
        quote: Optional[str] = '"',
        escape: Optional[str] = None,
        double_quote: bool = True,
        split: str = "\n",
        text: bool = True,
    ) -> None:
        def conv(c: Optional[str]) -> Any:
            if c is None or c == "":
                return None
            return c if text else c.encode("latin-1")

        self.delimiter = conv(delimiter) or conv(",")
        self.quote = conv(quote)
        esc = conv(escape)
        self.escape = esc if esc != self.quote else None
        self.double_quote = bool(double_quote)
        self.nl, self.cr, self.empty = conv("\n"), conv("\r"), ("" if text else b"")
        self.split = conv("\r") if split == "\r" else conv("\n")
        self.split_cr = split == "\r"
        self.starts = (self.delimiter, self.nl, self.cr)
        # single characters as indexing returns them (an int for bytes): cheaper than slices
        item = (lambda c: c) if text else (lambda c: c[0])
        self.start_items = frozenset(item(c) for c in self.starts)
        self.quote_item = None if self.quote is None else item(self.quote)
        self.escape_item = None if self.escape is None else item(self.escape)
        pat = (lambda s: s) if text else (lambda s: s.encode("latin-1"))
        self.stop = re.compile(pat("[") + re.escape(self.delimiter) + pat("\r\n]"))
        self.lines = re.compile(pat("\r\n|\r|\n"))
        self.q_or_esc = (
            re.compile(pat("[") + re.escape(self.quote) + re.escape(self.escape) + pat("]"))
            if self.quote is not None and self.escape is not None
            else None
        )
        self.text = text
        # a run of doubled quotes and the text between them, inside a quoted field
        self.doubled = (
            re.compile(pat("(?:") + re.escape(self.quote) * 2 + pat("[^") + re.escape(self.quote) + pat("]*)*"))
            if self.quote is not None and self.double_quote and self.escape is None
            else None
        )
        # quote parity gives the state at C speed when quotes are doubled, never escaped
        self.fast = (
            self.quote is not None
            and self.escape is None
            and self.double_quote
            and self.delimiter not in (self.nl, self.cr)
            and self.quote not in (self.delimiter, self.nl, self.cr)
        )
        self.odd_quote = self.table = None
        if self.fast:
            # a quote of the skeleton (see _skeleton) after or before a character that is
            # none of a delimiter, a line break or another quote
            q = re.escape(self.quote)
            others = pat("[^") + re.escape(self.delimiter) + pat("\r\n") + q + pat("]")
            self.odd_quote = re.compile(q + pat("(?:(?<=") + others + q + pat(")|(?=") + others + pat("))"))
            if ord(delimiter or ",") < 128 and ord(quote) < 128:  # type: ignore[arg-type]
                # the same search as two substring searches over the skeleton's bytes
                # mapped to S (delimiter, line break), Q (quote) and x (anything else)
                table = bytearray(b"x" * 256)
                for c in (ord(delimiter or ","), 10, 13):
                    table[c] = ord("S")
                table[ord(quote)] = ord("Q")  # type: ignore[arg-type]
                self.table = bytes(table)

    @classmethod
    def of(cls, d: Any, text: bool = True) -> Dialect:
        """From a sniffed :class:`dataconv._sniff.Dialect`."""
        return cls(d.delimiter, d.quote, d.escape, d.double_quote, "\r" if d.terminator == "\r" else "\n", text)


# ---------------------------------------------------------------------------------
# Record boundaries
# ---------------------------------------------------------------------------------


def _is_cut(buf: Any, t: int, d: Dialect, n: Optional[int] = None) -> bool:
    """A record may end after the split character at ``t``: with CR line endings a CR
    followed by LF is half of a CRLF pair (and at the end of the scan, ``n``, the next
    character is unknown)."""
    if not d.split_cr:
        return True
    if t + 1 >= (len(buf) if n is None else n):
        return False
    return buf[t + 1 : t + 2] != d.nl


def _last_term(buf: Any, start: int, end: int, d: Dialect, n: Optional[int] = None) -> Optional[int]:
    while end > start:
        t = buf.rfind(d.split, start, end)
        if t < 0:
            return None
        if _is_cut(buf, t, d, n):
            return t + 1
        end = t
    return None


def line_end(buf: Any, pos: int, d: Dialect) -> int:
    """Offset just past the first record terminator at or after ``pos`` (or the end)."""
    while True:
        t = buf.find(d.split, pos)
        if t < 0:
            return len(buf)
        if _is_cut(buf, t, d):
            return t + 1
        pos = t + 1


def _regions(
    buf: Any, d: Dialect, start: int = 0, end: Optional[int] = None, until: Optional[int] = None
) -> Tuple[List[int], Optional[int], bool]:
    """Scan ``buf[start:end]`` from a record boundary: (the stretches outside quoted
    fields as a flat [start, end, start, end, ...] list, the offset of an opening quote
    still open at the end, whether any quoted field is irregular or holds escape
    sequences).  With ``until``, the scan stops just past the first line feed outside
    quotes at or after that offset (the last stretch then ends there, before ``end``).

    Only quotes are visited one by one; everything between quoted fields is searched
    at C speed."""
    n = len(buf) if end is None else end
    q = d.quote
    if q is None:
        return [start, n], None, False
    qi, ei, starts = d.quote_item, d.escape_item, d.start_items
    esc_re = d.q_or_esc
    dq, doubled = d.double_quote, d.doubled
    bounds: List[int] = []
    irregular = False
    pos = search = start
    stop = -1 if until is None else -2  # the line feed to stop after (-2: to look for)
    while True:
        # where to stop if no quoted field starts before it (looked for again when a
        # quoted field ran past it)
        if stop == -2 or 0 <= stop < pos:
            stop = buf.find(d.nl, max(pos, until), n)  # type: ignore[type-var]
        # the next quote that opens a field; any other quote is a literal character
        while True:
            j = buf.find(q, search, n if stop < 0 else stop)
            if j <= 0 or buf[j - 1] in starts:
                break
            search = j + 1
        if j < 0 and stop >= 0:
            bounds += (pos, stop + 1)
            return bounds, None, irregular
        if j < 0:
            bounds += (pos, n)
            return bounds, None, irregular
        bounds += (pos, j)
        # the quote that closes it
        i = j + 1
        while True:
            if esc_re is not None:
                m = esc_re.search(buf, i, n)
                k = m.start() if m else -1
            else:
                k = buf.find(q, i, n)
            if k < 0:
                return bounds, j, irregular
            if ei is not None and buf[k] == ei:
                irregular = True
                i = k + 2
                if i > n:
                    return bounds, j, irregular
                continue
            after = k + 1
            if after < n:
                c = buf[after]
                if dq and c == qi:
                    # doubled quotes: the whole run of them at C speed
                    i = doubled.match(buf, k, n).end() if doubled is not None else k + 2
                    continue
                if c not in starts:
                    irregular = True
                    # text after a closing quote of a field that spans lines: the quote at
                    # j was a stray, literal, and its line ends the record; the scan
                    # resumes on the next line (the closing quote is scanned again)
                    t = d.lines.search(buf, j + 1, k)
                    if t is not None:
                        bounds += (j, t.end())
                        pos = search = t.end()
                        break
            pos = search = after
            break


# --- the fast path: quote parity -------------------------------------------------


def _skeleton(w: Any, q: Any) -> Tuple[List[Any], Any, int]:
    """(``w`` split at its quotes, its skeleton, the number of quotes).  The skeleton is
    ``w`` with each quoted stretch (a quote, the text up to the next quote and that
    quote) cut down to one quote, plus one quote for a stretch still open at the end:
    while every quote is regular it is exactly the text outside quoted fields, with a
    quote where each field's quoted part was (two in a row for a doubled quote)."""
    segs = w.split(q)
    if len(segs) == 1:
        return segs, w, 0
    skel = q.join(segs[0::2])
    if len(segs) % 2 == 0:
        skel += q
    return segs, skel, len(segs) - 1


def _first_odd(skel: Any, d: Dialect) -> int:
    """Skeleton index of the first quote after or before a character that is none of a
    delimiter, a line break or another quote (-1 when there is none)."""
    if d.table is not None and (not d.text or skel.isascii()):
        t = (skel.encode("ascii") if d.text else skel).translate(d.table)
        after = t.find(b"xQ")
        before = t.find(b"Qx")
        if after < 0:
            return before
        return after + 1 if before < 0 or after < before else before
    m = d.odd_quote.search(skel)  # type: ignore[union-attr]
    return m.start() if m is not None else -1


def _stop(skel: Any, quotes: int, d: Dialect, at_end: bool) -> Tuple[int, bool]:
    """(skeleton index where the regular stretch ends, whether the quote there is
    irregular): the first quote that parity cannot vouch for, else a quoted field still
    open at the end (never closed if ``at_end``), else the end."""
    if quotes:
        odd = _first_odd(skel, d)
        if odd >= 0:
            return odd, True
        if quotes & 1:
            return len(skel) - 1, at_end
    return len(skel), False


def _offset(segs: List[Any], skel: Any, q: Any, quotes: int, size: int, x: int) -> int:
    """Offset in the window (of ``size`` characters) of skeleton index ``x``, which lies
    outside quoted fields: each quoted stretch before it is longer by its text plus one
    (counted from whichever end has fewer of them)."""
    k = skel.count(q, 0, x)  # quoted stretches before x
    if not k:
        return x
    stretches = (quotes + 1) // 2  # the last one is still open when the count is odd
    if k <= stretches - k:
        return x + k + sum(map(len, segs[1 : 2 * k : 2]))
    after = sum(map(len, segs[2 * k + 1 :: 2])) + (stretches - k) - (quotes & 1)
    return size - (len(skel) - x) - after


def _breaks(skel: Any, d: Dialect, end: int) -> int:
    """Record terminators in ``skel[:end]``."""
    nl, cr = d.nl, d.cr
    return skel.count(nl, 0, end) + skel.count(cr, 0, end) - skel.count(cr + nl, 0, end)


def regular_run(
    data: Any, d: Dialect, pos: int = 0, window: int = 1 << 12, count: bool = False, most: Optional[int] = None
) -> Tuple[int, int]:
    """(end of the run of regular records that starts at the record boundary ``pos``,
    record terminators in it if ``count``).  ``data`` holds whole records, as the
    :class:`Chunker` cuts them; a regular record has only regular quotes (see the
    module notes), so every engine reads it the same way.  The end is ``pos`` itself
    when the record there is not regular, or not for certain (a quote inside an
    unquoted field, which parity cannot tell apart from an irregular one).

    The windows checked grow from ``window`` characters, so a short run costs little;
    with ``most``, the run ends at the first window's end past ``pos + most``."""
    n = len(data)
    breaks = 0
    if not d.fast:
        return pos, 0
    q, nl, cr = d.quote, d.nl, d.cr
    limit = n if most is None else pos + most
    while pos < n:
        end = min(n, pos + window)
        w = data if pos == 0 and end == n else data[pos:end]
        segs, skel, quotes = _skeleton(w, q)
        stop, irregular = _stop(skel, quotes, d, end == n)
        if end == n and stop == len(skel):
            return n, (breaks + _breaks(skel, d, stop) if count else 0)
        # the last record that ends before the stop
        t = max(skel.rfind(nl, 0, stop), skel.rfind(cr, 0, stop)) + 1
        if t == len(skel) and skel[t - 1 : t] == cr and data[end : end + 1] == nl:
            t = max(skel.rfind(nl, 0, t - 1), skel.rfind(cr, 0, t - 1)) + 1  # CRLF split by the window
        if t:
            if count:
                breaks += _breaks(skel, d, t)
            pos += _offset(segs, skel, q, quotes, len(w), t)
        if irregular or pos >= limit:
            break
        # read on with a window twice as large (or as large as one record needs)
        window = window * 2 if not t or window < CUT_WINDOW else window
    return pos, breaks


def _regular_cut(buf: Any, d: Dialect, pos: int, n: int, window: int = CUT_WINDOW) -> Tuple[int, Optional[int]]:
    """(offset just past the last line feed of the regular stretch that starts at the
    record boundary ``pos`` of ``buf[:n]``, 0 when there is none; offset of the quote
    that parity cannot vouch for where the stretch ends, None when it runs to ``n``).
    Up to that quote the state outside quotes is known, at C speed.  A record longer
    than ``MAX_FAST`` characters ends the stretch too: the exact scan reads it whole at
    C speed, where windows would have to grow over it.  The windows checked grow from
    ``window`` characters."""
    q, nl = d.quote, d.nl
    last = 0
    while pos < n:
        end = min(n, pos + window)
        w = buf if pos == 0 and end == len(buf) else buf[pos:end]
        segs, skel, quotes = _skeleton(w, q)
        stop, irregular = _stop(skel, quotes, d, False)
        t = skel.rfind(nl, 0, stop) + 1
        if t:
            last = pos + _offset(segs, skel, q, quotes, len(w), t)
        if irregular:
            return last, pos + _offset(segs, skel, q, quotes, len(w), stop)
        if end == n:
            break
        if t:
            pos = last
            window = min(window * 2, max(window, CUT_WINDOW))
        elif window >= MAX_FAST:
            return last, pos
        else:
            window *= 2  # a record longer than the window
    return last, None


def scan_end(buf: Any, d: Dialect, end: Optional[int] = None) -> Tuple[Optional[int], Optional[int], bool]:
    """(offset just past the last record terminator outside quotes, offset of an
    opening quote still open at the end, whether any quoted field is irregular), for
    ``buf[:end]``.  Regular stretches are found by quote parity; from the last record
    boundary before a quote that parity cannot vouch for, the exact scan reads on to
    the end of that quote's record, and the fast path resumes there.  When stretches
    stay short (a stray quote on every line), the exact scan reads on further each
    time, so that the fast path costs little where it cannot help."""
    n = len(buf) if end is None else end
    fast = d.fast and not d.split_cr
    last: Optional[int] = None
    irregular = False
    pos = ahead = 0
    while True:
        cut, bad = _regular_cut(buf, d, pos, n, 1 << 10 if ahead else CUT_WINDOW) if fast else (0, None)
        if cut:
            last = cut
        until = None
        if bad is not None:
            ahead = 0 if bad - pos >= CUT_WINDOW else min(max(ahead * 2, 1 << 10), MAX_FAST)
            until = bad + ahead
        bounds, open_, irr = _regions(buf, d, cut or pos, n, until if until is None or until < n else None)
        irregular = irregular or irr
        for k in range(len(bounds) - 2, -1, -2):
            t = _last_term(buf, bounds[k], bounds[k + 1], d, n)
            if t is not None:
                last = t
                break
        if until is None or until >= n or open_ is not None or bounds[-1] >= n:
            return last, open_, irregular
        pos = bounds[-1]


def last_record_end(buf: Any, d: Dialect) -> Optional[int]:
    return scan_end(buf, d)[0]


def may_hold_blank(buf: Any, d: Dialect, start: int = 0, end: Optional[int] = None) -> bool:
    """Whether ``buf[start:end]`` can hold a blank line at all (two line breaks in a
    row, or one at the start)."""
    nl, cr = d.nl, d.cr
    end = len(buf) if end is None else end
    return (
        buf[start : start + 1] in (nl, cr)
        or buf.find(nl + nl, start, end) >= 0
        or buf.find(nl + cr, start, end) >= 0
        or buf.find(cr + cr, start, end) >= 0
    )


def count_records(buf: Any, d: Dialect) -> int:
    """Records in ``buf`` (whole records), as :func:`tokenize` splits them: terminators
    outside quotes, plus a last record without one."""
    nl, cr = d.nl, d.cr
    crlf = cr + nl
    n = len(buf)
    total = pos = 0
    while True:
        start, terms = regular_run(buf, d, pos, CUT_WINDOW, count=True)
        bounds, open_, _ = _regions(buf, d, start)
        for k in range(0, len(bounds), 2):
            s, e = bounds[k], bounds[k + 1]
            terms += buf.count(nl, s, e) + buf.count(cr, s, e) - buf.count(crlf, s, e)
        total += terms
        if open_ is None:
            return total + (1 if n > pos and buf[-1:] not in (nl, cr) else 0)
        # a quote never closed: a stray, whose record ends with its own line
        t = d.lines.search(buf, open_ + 1)
        total += 1
        if t is None or t.end() >= n:
            return total
        pos = t.end()


class Chunker:
    """Split a text or byte stream into record-aligned chunks of about ``size``
    characters (the native ``Chunker``).

    After each :meth:`next_chunk`, ``stray`` is the offset of a malformed record that
    ends the chunk (a stray quote), or None, and ``forced`` tells whether that quote was
    taken to be a stray only because its field was still open ``MAX_QUOTED`` characters
    later (a genuine field that large would be split)."""

    def __init__(
        self, stream: Any, size: int, d: Dialect, prefix: Any = None, cut_anywhere: bool = False, known: Any = None
    ) -> None:
        self.stream = stream
        self.size = max(1, size)
        self.d = d
        self.carry = prefix if prefix is not None else d.empty
        self.eof = False
        self.offset = 0
        self.stray: Optional[int] = None
        self.forced = False
        self.irregular = False
        self.max_record = max(self.size * 8, MAX_LINE)
        self.cut_anywhere = cut_anywhere
        # chunks cut before from the same input by a chunker of the same size, by offset:
        # (length, irregular, forced, stray); a later pass reuses them without a scan
        self.known: dict = known if known is not None else {}

    def _fill(self, target: int) -> None:
        if self.eof or len(self.carry) >= target:
            return
        parts = [self.carry]
        total = len(self.carry)
        while total < target:
            data = self.stream.read(target - total)
            if not data:
                self.eof = True
                break
            parts.append(data)
            total += len(data)
        self.carry = self.d.empty.join(parts)

    def _cut(self, target: int) -> Optional[int]:
        buf, d = self.carry, self.d
        n = len(buf)
        self.stray = None
        self.forced = False
        all_read = self.eof and n < target
        if self.cut_anywhere and n >= self.max_record:
            last, _, self.irregular = scan_end(buf, d)
            return last if last is not None else n
        # while more input may follow, trailing quotes are ambiguous (a closing quote, or
        # the first of a doubled pair), so the scan stops before them
        scanned = n
        if not self.eof and d.quote is not None:
            while scanned > 0 and buf[scanned - 1 : scanned] == d.quote:
                scanned -= 1
        last, open_, self.irregular = scan_end(buf, d, scanned)
        if open_ is not None:
            record = last if last is not None else 0
            if self.eof or n - record >= MAX_QUOTED:
                if last is not None and not all_read and n - record < MAX_QUOTED:
                    return last  # the records before it first
                # a stray quote: the chunk ends with the line that holds it
                self.stray = record
                self.forced = not self.eof
                self.irregular = True
                return line_end(buf, open_, d)
            return last
        if all_read:
            return n
        if last is not None:
            return last
        if self.eof:
            return n
        if n >= self.max_record:
            raise DataError(
                f"a record runs for more than {self.max_record >> 20} MiB without a line break "
                "(raise the chunk size if that is intended)"
            )
        return None

    def next_chunk(self) -> Any:
        """The next chunk of whole records, or None at the end of the input."""
        seen = self.known.get(self.offset)
        if seen is not None:
            self._fill(seen[0])
            if len(self.carry) >= seen[0] > 0:
                cut, self.irregular, self.forced, self.stray = seen
                data, self.carry = self.carry[:cut], self.carry[cut:]
                self.offset += cut
                return data
        target = self.size
        while True:
            self._fill(target)
            if not self.carry:
                return None
            cut = self._cut(target)
            if cut is not None:
                data, self.carry = self.carry[:cut], self.carry[cut:]
                self.known[self.offset] = (cut, self.irregular, self.forced, self.stray)
                self.offset += cut
                return data
            # a single record longer than the target: read further, but no further
            # than the limits that decide what happens to it
            limit = self.max_record if self.cut_anywhere else max(self.max_record, MAX_QUOTED + 1)
            target = min(max(len(self.carry) * 2, self.size), max(limit, len(self.carry) + 1))

    def __iter__(self) -> Iterator[Any]:
        while True:
            chunk = self.next_chunk()
            if chunk is None:
                return
            yield chunk


# ---------------------------------------------------------------------------------
# Tokenizing
# ---------------------------------------------------------------------------------

Record = Tuple[List[Any], int, int, int, int]  # (values, start, end, terminator, flags)


def _unescape(content: Any, d: Dialect) -> Any:
    """The value of a quoted field's raw content: escape sequences and doubled quotes
    resolved (a lone escape at the very end stays)."""
    q, esc, dq = d.quote, d.escape, d.double_quote
    if esc is None:
        return content.replace(q + q, q) if dq else content
    out = []
    i = seg = 0
    n = len(content)
    while i < n:
        c = content[i : i + 1]
        if c == esc and i + 1 < n:
            out.append(content[seg:i])
            out.append(content[i + 1 : i + 2])
            i = seg = i + 2
        elif c == q and dq and content[i + 1 : i + 2] == q:
            out.append(content[seg:i])
            out.append(q)
            i = seg = i + 2
        else:
            i += 1
    out.append(content[seg:])
    return d.empty.join(out)


def tokenize(data: Any, d: Dialect, start: int = 0, partial: bool = False) -> Iterator[Record]:
    """Records of ``data`` (whole records, as the :class:`Chunker` cuts them).

    A value is the field's text with escape sequences resolved; an empty unquoted
    field is None and a quoted empty field is ``""``.  Stray quotes make a record
    UNTERMINATED (malformed), and cost their own line only: a quoted field that is
    never closed ends with its line, and so does one that spans lines and has text
    after its closing quote (a stray quote paired with a later one); the next line
    starts a record.  In a ``partial`` sample, which may end inside a record, a quote
    left open may close beyond the data: its field takes the rest of it."""
    n = len(data)
    q, delim, esc, dq, doubled = d.quote, d.delimiter, d.escape, d.double_quote, d.doubled
    qq = q * 2 if q is not None else None
    nl, empty = d.nl, d.empty
    stop, lines, q_or_esc, starts = d.stop, d.lines, d.q_or_esc, d.starts
    pos = start
    while pos < n:
        rec_start = pos
        m = lines.search(data, pos)
        eol = m.start() if m else n
        if q is None or data.find(q, pos, eol) < 0:
            # no quote on the line: the record is the line
            values = [v if v else None for v in data[pos:eol].split(delim)]
            if m is None:
                yield values, rec_start, n, EOF, 0
                return
            t = m.group()
            pos = m.end()
            yield values, rec_start, pos, (LF if t == nl else CRLF if len(t) == 2 else CR), 0
            continue
        values = []
        flags = 0
        while True:
            if data[pos : pos + 1] == q:
                content = i = seg = pos + 1
                parts: List[Any] = []
                closing = -1
                while True:
                    if q_or_esc is not None:
                        mm = q_or_esc.search(data, i)
                        k = mm.start() if mm else -1
                    else:
                        k = data.find(q, i)
                    if k < 0:
                        break
                    if esc is not None and data[k : k + 1] == esc:
                        parts.append(data[seg:k])
                        parts.append(data[k + 1 : k + 2] or esc)  # a lone escape at the very end stays
                        i = seg = k + 2
                        if i > n:
                            break
                        continue
                    if dq and data[k + 1 : k + 2] == q:
                        parts.append(data[seg:k])
                        if doubled is not None:
                            # the whole run of doubled quotes at C speed
                            i = doubled.match(data, k).end()
                            parts.append(data[k:i].replace(qq, q))
                            seg = i
                            continue
                        parts.append(q)
                        i = seg = k + 2
                        continue
                    closing = k
                    break
                after = closing + 1
                text_after = closing >= 0 and after < n and data[after : after + 1] not in starts
                stray = None if closing >= 0 and not text_after else lines.search(data, content, closing if closing >= 0 else n)
                if closing < 0 and partial:
                    # never closed in a sample: the field may close beyond the data
                    parts.append(data[seg:n])
                    values.append(empty.join(parts))
                    flags |= IRREGULAR | UNTERMINATED
                    pos = n
                    break
                if closing < 0 or stray is not None:
                    # a stray quote, never closed or paired across lines with a quote that has
                    # text after it: the field, and the record, end with the stray's own line
                    end = stray.start() if stray is not None else n
                    values.append(_unescape(data[content:end], d))
                    flags |= IRREGULAR | UNTERMINATED
                    pos = end
                    break  # the record ends at that line break (or the end)
                parts.append(data[seg:closing])
                value = empty.join(parts)
                if text_after:
                    # text after the closing quote stays with the field
                    mm = stop.search(data, after)
                    k = mm.start() if mm else n
                    value += data[after:k]
                    flags |= IRREGULAR
                    pos = k
                else:
                    pos = after
                values.append(value)
            else:
                mm = stop.search(data, pos)
                k = mm.start() if mm else n
                v = data[pos:k]
                values.append(v if v else None)
                pos = k
            if pos >= n:
                break
            c = data[pos : pos + 1]
            if c == delim:
                pos += 1
                if pos >= n:
                    values.append(None)  # a trailing delimiter at the very end: one more empty field
                    break
                continue
            break
        if pos >= n:
            yield values, rec_start, n, EOF, flags
            return
        if data[pos : pos + 1] == nl:
            pos += 1
            term = LF
        elif data[pos + 1 : pos + 2] == nl:
            pos += 2
            term = CRLF
        else:
            pos += 1
            term = CR
        yield values, rec_start, pos, term, flags


def forced_warning(at: int) -> str:
    """The report's note on a quote read as a stray only because its field was still
    open ``MAX_QUOTED`` bytes later (``at``: the offset just past that chunk)."""
    return (
        f"a quote before byte {at} was still open {MAX_QUOTED >> 20} MiB later and was read as a stray quote; "
        "if the file really holds a quoted field that large, the rows after it need checking"
    )


def is_blank(values: List[Any]) -> bool:
    """A blank line: a single empty unquoted field."""
    return len(values) == 1 and values[0] is None


def count_lines(raw: Any) -> int:
    """Physical lines a record's text spans, counted as the native engine counts them:
    its line feeds, or its carriage returns when it holds no line feed."""
    nl, cr = ("\n", "\r") if isinstance(raw, str) else (b"\n", b"\r")
    breaks = raw.count(nl) or raw.count(cr)
    return max(1, breaks + (0 if raw.endswith((nl, cr)) else 1))


def skip_records(data: Any, d: Dialect, count: int) -> Tuple[int, int]:
    """(offset after the first ``count`` records of ``data``, records skipped)."""
    pos = skipped = 0
    if count <= 0:
        return 0, 0
    for _, _, end, _, _ in tokenize(data, d):
        pos = end
        skipped += 1
        if skipped >= count:
            break
    return pos, skipped


def sample_records(text: str, d: Dialect, limit: int, complete: bool) -> Tuple[List[List[str]], List[int], int]:
    """Records of a sniffing sample as lists of strings: (rows, terminator counts by
    code, longest record).  A sample that is the whole file is split exactly as a
    conversion splits it (a stray quote costs its own line); in a sample cut off by its
    size a quote left open may close beyond it, and the last record is dropped."""
    rows: List[List[str]] = []
    terms = [0, 0, 0, 0]
    longest = 0
    for values, s, e, term, _ in tokenize(text, d, partial=not complete):
        if term == EOF and not complete:
            break
        terms[term] += 1
        longest = max(longest, e - s)
        rows.append([v if v is not None else "" for v in values])
        if len(rows) >= limit:
            break
    return rows, terms, longest
