"""Pure-Python implementation of the DCZ format (see ``docs/FORMAT.md``).

It reads everything the native engine writes and writes files the native engine
reads, so ``.dcz`` files open on any machine with Python.  numpy speeds up the
byte-plane transforms when it is installed.
"""

from __future__ import annotations

import functools
import io
import json
import os
import re
import struct
import zlib
from collections import Counter
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from . import _compat, _csvtok, codec
from .errors import DataconvError, FormatError, IntegrityError

MAGIC = b"\x89DCZ\r\n\x1a\n"
MAGIC_END = b"\x89DCZEND\n"
VERSION = 1
HEADER_LEN, BLOCK_HEADER_LEN, TRAILER_LEN = 16, 32, 16
KIND_RAW, KIND_CSV, KIND_PACK, KIND_FOOTER = 0, 1, 2, 254
FLAG_PACKED = 1
_BH = struct.Struct("<BBHQQII4x")
TERMS = (b"\n", b"\r\n", b"\r", b"")
M64 = (1 << 64) - 1


def _crc(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF


def wrap64(x: int) -> int:
    return ((x + (1 << 63)) & M64) - (1 << 63)


def truncated() -> IntegrityError:
    return IntegrityError("DCZ data is truncated or corrupt")


# What corrupt input makes the decoders raise; reported as an IntegrityError.
_CORRUPT = (StopIteration, IndexError, KeyError, TypeError, ValueError, OverflowError, struct.error, MemoryError,
            RecursionError, ArithmeticError, AttributeError)


def _guard(fn: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except DataconvError:
            raise
        except _CORRUPT as exc:
            raise IntegrityError(f"DCZ data is truncated or corrupt ({type(exc).__name__})") from None

    return wrapper


# ---------------------------------------------------------------------------------
# varints and integer arrays
# ---------------------------------------------------------------------------------


def put_varint(out: bytearray, v: int) -> None:
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v)


def zigzag(v: int) -> int:
    return (v << 1) if v >= 0 else ((-v) << 1) - 1


def unzigzag(u: int) -> int:
    return (u >> 1) if not u & 1 else -((u + 1) >> 1)


class Reader:
    __slots__ = ("data", "pos")

    def __init__(self, data: bytes, pos: int = 0) -> None:
        self.data = data
        self.pos = pos

    def u8(self) -> int:
        if self.pos >= len(self.data):
            raise truncated()
        b = self.data[self.pos]
        self.pos += 1
        return b

    def varint(self) -> int:
        v = shift = 0
        while True:
            b = self.u8()
            if shift >= 64:
                raise IntegrityError("varint overflow")
            v |= (b & 0x7F) << shift
            if not b & 0x80:
                return v
            shift += 7

    def zigzag(self) -> int:
        return unzigzag(self.varint())

    def take(self, n: int) -> bytes:
        end = self.pos + n
        if n < 0 or end > len(self.data):
            raise truncated()
        s = self.data[self.pos : end]
        self.pos = end
        return bytes(s)

    def done(self) -> bool:
        return self.pos >= len(self.data)


def _width(maximum: int) -> int:
    return (maximum.bit_length() + 7) // 8


def _planes_encode(values: Sequence[int], width: int) -> bytes:
    if width == 0 or not values:
        return b""
    np = _compat.optional("numpy")
    if np is not None:
        arr = np.asarray(values, dtype=np.uint64)
        return np.ascontiguousarray(arr.astype("<u8").view(np.uint8).reshape(-1, 8)[:, :width].T).tobytes()  # type: ignore[no-any-return]
    out = bytearray()
    for k in range(width):
        shift = 8 * k
        out += bytes((v >> shift) & 0xFF for v in values)
    return bytes(out)


def _planes_decode(data: bytes, n: int, width: int) -> List[int]:
    if width == 0:
        return [0] * n
    np = _compat.optional("numpy")
    if np is not None:
        planes = np.frombuffer(data, dtype=np.uint8).reshape(width, n).astype(np.uint64)
        acc = np.zeros(n, dtype=np.uint64)
        for k in range(width):
            acc |= planes[k] << np.uint64(8 * k)
        return [int(x) for x in acc.tolist()]
    vals = list(data[0:n])
    for k in range(1, width):
        shift = 8 * k
        vals = [v | (b << shift) for v, b in zip(vals, data[k * n : (k + 1) * n])]
    return vals


def _put_for(out: bytearray, values: Sequence[int]) -> None:
    base = min(values) if values else 0
    offsets = [(v - base) & M64 for v in values]
    width = _width(max(offsets) if offsets else 0)
    put_varint(out, zigzag(base))
    out.append(width)
    out += _planes_encode(offsets, width)


def _get_for(r: Reader, n: int) -> List[int]:
    base = r.zigzag()
    width = r.u8()
    if width > 8:
        raise truncated()
    offsets = _planes_decode(r.take(n * width), n, width)
    return [wrap64(base + o) for o in offsets]


def _deltas(values: Sequence[int]) -> List[int]:
    return [wrap64(b - a) for a, b in zip(values, values[1:])]


def _estimate(values: Sequence[int]) -> int:
    if not values:
        return 0
    return len(values) * _width((max(values) - min(values)) & M64)


def _runs(values: Sequence[int]) -> int:
    return 1 + sum(1 for a, b in zip(values, values[1:]) if a != b) if values else 0


def encode_ints(values: Sequence[int], transform: Optional[int] = None) -> bytes:
    out = bytearray()
    _encode_ints(out, list(values), transform)
    return bytes(out)


def _encode_ints(out: bytearray, values: List[int], transform: Optional[int] = None) -> None:
    n = len(values)
    if transform is None:
        if n < 4:
            transform = 0
        else:
            plain = _estimate(values)
            delta = _estimate(_deltas(values)) + 9
            runs = _runs(values)
            rle = runs * 3 + 16 if runs * 4 <= n else 1 << 62
            transform = 2 if rle < min(plain, delta) else 1 if delta < plain else 0
    put_varint(out, n)
    out.append(transform)
    if n == 0:
        return
    if transform == 0:
        _put_for(out, values)
    elif transform == 1:
        put_varint(out, zigzag(values[0]))
        _put_for(out, _deltas(values))
    else:
        run_values: List[int] = []
        run_lengths: List[int] = []
        prev = None
        for v in values:
            if run_values and v == prev:
                run_lengths[-1] += 1
            else:
                run_values.append(v)
                run_lengths.append(1)
                prev = v
        put_varint(out, len(run_values))
        tv = 0 if len(run_values) < 4 else (1 if _estimate(_deltas(run_values)) + 9 < _estimate(run_values) else 0)
        _encode_ints(out, run_values, tv)
        _encode_ints(out, run_lengths, 0)


def decode_ints(r: Reader, limit: Optional[int] = None, rle: bool = True) -> List[int]:
    """An int_array; ``limit`` bounds its length where the container knows it (a bit
    flip in a count must not turn into a huge allocation), and the values and lengths
    of an RLE array are never RLE arrays themselves."""
    n = r.varint()
    t = r.u8()
    if n == 0:
        return []
    if n > (1 << 34) or (limit is not None and n > limit) or (t == 2 and not rle):
        raise truncated()
    if t == 0:
        return _get_for(r, n)
    if t == 1:
        first = r.zigzag()
        out = [first]
        acc = first
        for d in _get_for(r, n - 1):
            acc = wrap64(acc + d)
            out.append(acc)
        return out
    if t == 2:
        runs = r.varint()
        if runs > n:
            raise truncated()
        values = decode_ints(r, runs, rle=False)
        lengths = decode_ints(r, runs, rle=False)
        if len(values) != runs or len(lengths) != runs or sum(lengths) != n or any(x < 0 for x in lengths):
            raise truncated()
        out = []
        for v, k in zip(values, lengths):
            out.extend([v] * k)
        return out
    raise FormatError(f"unknown integer transform {t}")


def put_bitmap(out: bytearray, bits: Sequence[bool]) -> None:
    buf = bytearray((len(bits) + 7) // 8)
    for i, b in enumerate(bits):
        if b:
            buf[i >> 3] |= 1 << (i & 7)
    out += buf


def get_bitmap(r: Reader, n: int) -> List[bool]:
    data = r.take((n + 7) // 8)
    return [bool(data[i >> 3] & (1 << (i & 7))) for i in range(n)]


# ---------------------------------------------------------------------------------
# Text columns
# ---------------------------------------------------------------------------------

RAW, CONST, DICT, INT, DECIMAL, TEMPLATE = range(6)
_CANON_INT = re.compile(rb"-?(?:0|[1-9][0-9]{0,17})")
_CANON_DEC = re.compile(rb"(-?)(0|[1-9][0-9]*)(?:\.([0-9]+))?")
_DIGITS = re.compile(rb"[0-9]+")


def canonical_int(v: bytes) -> Optional[int]:
    if not _CANON_INT.fullmatch(v) or v == b"-0":
        return None
    return int(v)


def canonical_decimal(v: bytes) -> Optional[Tuple[int, int]]:
    m = _CANON_DEC.fullmatch(v)
    if not m:
        return None
    sign, ip, fp = m.groups()
    fp = fp or b""
    if len(ip) + len(fp) > 18:
        return None
    mant = int(ip + fp)
    if sign and mant == 0:
        return None
    return (-mant if sign else mant), len(fp)


def write_decimal(m: int, scale: int) -> bytes:
    digits = str(abs(m))
    if scale:
        if len(digits) <= scale:
            digits = "0." + "0" * (scale - len(digits)) + digits
        else:
            digits = digits[:-scale] + "." + digits[-scale:]
    return (("-" if m < 0 else "") + digits).encode()


def _quoting(quoted: Sequence[bool]) -> int:
    if not any(quoted):
        return 0
    return 1 if all(quoted) else 2


def _finish(enc: int, quoted: Sequence[bool], empty: Optional[Sequence[bool]], exceptions: List[Tuple[int, bytes]],
            params: bytes, main: List[bytes]) -> Tuple[bytes, List[bytes]]:
    q = _quoting(quoted)
    has_empty = bool(empty) and any(empty)  # type: ignore[arg-type]
    meta = bytearray([enc, q])
    put_varint(meta, len(exceptions))
    meta.append(1 if has_empty else 0)
    meta += params
    streams: List[bytes] = []
    if q == 2 or has_empty or exceptions:
        aux = bytearray()
        if q == 2:
            put_bitmap(aux, quoted)
        if has_empty:
            put_bitmap(aux, empty)  # type: ignore[arg-type]
        if exceptions:
            aux += encode_ints([p for p, _ in exceptions])
            aux += encode_ints([len(b) for _, b in exceptions])
            for _, b in exceptions:
                aux += b
        streams.append(bytes(aux))
    return bytes(meta), streams + main


def _encode_raw(cells: Sequence[bytes], quoted: Sequence[bool]) -> Tuple[bytes, List[bytes]]:
    return _finish(RAW, quoted, None, [], b"", [encode_ints([len(c) for c in cells]), b"".join(cells)])


def _encode_const(cells: Sequence[bytes], quoted: Sequence[bool]) -> Optional[Tuple[bytes, List[bytes]]]:
    first = cells[0]
    if any(c != first for c in cells):
        return None
    params = bytearray()
    put_varint(params, len(first))
    params += first
    return _finish(CONST, quoted, None, [], bytes(params), [])


def _budget(n: int) -> int:
    return n // 50 + 2


def _encode_int(cells: Sequence[bytes], quoted: Sequence[bool]) -> Optional[Tuple[bytes, List[bytes]]]:
    values: List[int] = []
    empty = [False] * len(cells)
    exc: List[Tuple[int, bytes]] = []
    budget = _budget(len(cells))
    for i, c in enumerate(cells):
        if not c:
            empty[i] = True
            continue
        v = canonical_int(c)
        if v is None:
            exc.append((i, c))
            if len(exc) > budget:
                return None
        else:
            values.append(v)
    if not values:
        return None
    return _finish(INT, quoted, empty, exc, b"", [encode_ints(values)])


def _encode_decimal(cells: Sequence[bytes], quoted: Sequence[bool]) -> Optional[Tuple[bytes, List[bytes]]]:
    mant: List[int] = []
    scales: List[int] = []
    empty = [False] * len(cells)
    exc: List[Tuple[int, bytes]] = []
    budget = _budget(len(cells))
    for i, c in enumerate(cells):
        if not c:
            empty[i] = True
            continue
        d = canonical_decimal(c)
        if d is None:
            exc.append((i, c))
            if len(exc) > budget:
                return None
        else:
            mant.append(d[0])
            scales.append(d[1])
    if not mant or not any(scales):
        return None
    if all(s == scales[0] for s in scales):
        params, main = bytes([scales[0]]), [encode_ints(mant)]
    else:
        params, main = bytes([255]), [encode_ints(scales), encode_ints(mant)]
    return _finish(DECIMAL, quoted, empty, exc, params, main)


def _signature(v: bytes) -> Optional[bytes]:
    if b"\x01" in v:
        return None
    sig, n = _DIGITS.subn(b"\x01", v)
    return sig if 0 < n <= 16 else None


def _encode_template(cells: Sequence[bytes], quoted: Sequence[bool]) -> Optional[Tuple[bytes, List[bytes]]]:
    sample = [c for c in cells if c][:4096]
    counts = Counter(s for s in (_signature(c) for c in sample) if s is not None)
    if not counts:
        return None
    # deterministic, like the native encoder: highest count, then the smallest signature
    best, hits = min(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    if hits * 10 < len(sample) * 9:
        return None
    literals = best.split(b"\x01")  # literal before each field, plus the tail
    nfields = len(literals) - 1
    pattern = re.compile(b"".join(re.escape(lit) + (b"([0-9]{1,18})" if i < nfields else b"") for i, lit in enumerate(literals)))
    empty = [False] * len(cells)
    matches: List[Optional[Tuple[bytes, ...]]] = []
    for i, c in enumerate(cells):
        if not c:
            empty[i] = True
            matches.append(None)
            continue
        m = pattern.fullmatch(c)
        matches.append(m.groups() if m else None)
    runs = [[g[k] for g in matches if g is not None] for k in range(nfields)]
    widths = []
    for field_runs in runs:
        freq = Counter(len(d) for d in field_runs)
        w, n = min(freq.items(), key=lambda kv: (-kv[1], kv[0])) if freq else (0, 0)
        canonical = sum(1 for d in field_runs if len(d) == 1 or d[:1] != b"0")
        widths.append(w if n * 100 >= len(field_runs) * 99 and n >= canonical else 0)
    fields: List[List[int]] = [[] for _ in range(nfields)]
    exc: List[Tuple[int, bytes]] = []
    budget = _budget(len(cells))
    for i, (c, g) in enumerate(zip(cells, matches)):
        if empty[i]:
            continue
        ok = g is not None and all(
            (len(d) == w) if w else (len(d) == 1 or d[:1] != b"0") for d, w in zip(g, widths)
        )
        if ok:
            for k, d in enumerate(g):  # type: ignore[arg-type]
                fields[k].append(int(d))
        else:
            exc.append((i, c))
            if len(exc) > budget:
                return None
    # parts: literal / field alternation as the native engine stores it
    parts = bytearray()
    entries: List[Tuple[int, Any]] = []
    for i, lit in enumerate(literals):
        if lit:
            entries.append((0, lit))
        if i < nfields:
            entries.append((1, widths[i]))
    put_varint(parts, len(entries))
    for kind, val in entries:
        parts.append(kind)
        if kind == 0:
            put_varint(parts, len(val))
            parts += val
        else:
            parts.append(val)
    return _finish(TEMPLATE, quoted, empty, exc, bytes(parts), [encode_ints(f) for f in fields])


def _encode_dict(cells: Sequence[bytes], quoted: Sequence[bool], limit: int) -> Optional[Tuple[bytes, List[bytes]]]:
    index: Dict[bytes, int] = {}
    entries: List[bytes] = []
    ids: List[int] = []
    for c in cells:
        k = index.get(c)
        if k is None:
            k = index[c] = len(entries)
            entries.append(c)
            if len(entries) > limit:
                return None
        ids.append(k)
    params = bytearray()
    put_varint(params, len(entries))
    return _finish(DICT, quoted, None, [], bytes(params), [encode_ints([len(e) for e in entries]), b"".join(entries), encode_ints(ids)])


def column_candidates(cells: Sequence[bytes], quoted: Sequence[bool], exhaustive: bool) -> List[Tuple[bytes, List[bytes]]]:
    if not cells:
        return [_encode_raw(cells, quoted)]
    c = _encode_const(cells, quoted)
    if c is not None:
        return [c]
    out = []
    numeric = _encode_int(cells, quoted) or _encode_decimal(cells, quoted)
    if numeric is not None:
        out.append(numeric)
        if not exhaustive:
            return out
    t = _encode_template(cells, quoted)
    if t is not None:
        out.append(t)
        if not exhaustive:
            return out
    n = len(cells)
    d = _encode_dict(cells, quoted, min((n // 2 if exhaustive else n // 3) + 1, 1 << 20))
    if d is not None:
        out.append(d)
        if not exhaustive:
            return out
    out.append(_encode_raw(cells, quoted))
    return out


def decode_column(meta: Reader, take: Callable[[], bytes], n: int) -> Tuple[List[bytes], List[bool]]:
    enc = meta.u8()
    q = meta.u8()
    n_exc = meta.varint()
    has_empty = meta.u8() != 0
    if n_exc > n:
        raise truncated()
    quoted = [q == 1] * n
    empty = [False] * n
    exceptions: Dict[int, bytes] = {}
    if q == 2 or has_empty or n_exc:
        aux = Reader(take())
        if q == 2:
            quoted = get_bitmap(aux, n)
        if has_empty:
            empty = get_bitmap(aux, n)
        if n_exc:
            positions = decode_ints(aux, n_exc)
            lengths = decode_ints(aux, n_exc)
            if len(positions) != n_exc or len(lengths) != n_exc:
                raise truncated()
            for p, size in zip(positions, lengths):
                if not 0 <= p < n or size < 0:
                    raise truncated()
                exceptions[p] = aux.take(size)

    def emit(produce: Callable[[], bytes]) -> List[bytes]:
        out: List[bytes] = []
        append = out.append
        for i in range(n):
            if empty[i]:
                append(b"")
            elif i in exceptions:
                append(exceptions[i])
            else:
                append(produce())
        return out

    if enc == RAW:
        lengths = decode_ints(Reader(take()), n)
        blob = take()
        it = iter(lengths)
        pos = [0]

        def raw() -> bytes:
            ln = next(it)
            s = blob[pos[0] : pos[0] + ln]
            pos[0] += ln
            return s

        cells = emit(raw)
    elif enc == CONST:
        value = meta.take(meta.varint())
        cells = emit(lambda: value)
    elif enc == DICT:
        entries = meta.varint()
        if entries > max(n, 1):
            raise truncated()
        lengths = decode_ints(Reader(take()), entries)
        blob = take()
        ids = iter(decode_ints(Reader(take()), n))
        if len(lengths) != entries:
            raise truncated()
        values: List[bytes] = []
        pos = 0
        for ln in lengths:
            values.append(blob[pos : pos + ln])
            pos += ln
        cells = emit(lambda: values[next(ids)])
    elif enc == INT:
        ints = iter(decode_ints(Reader(take()), n))
        cells = emit(lambda: str(next(ints)).encode())
    elif enc == DECIMAL:
        scale = meta.u8()
        scales = iter(decode_ints(Reader(take()), n)) if scale == 255 else None
        mants = iter(decode_ints(Reader(take()), n))
        cells = emit(lambda: write_decimal(next(mants), next(scales) if scales is not None else scale))
    elif enc == TEMPLATE:
        nparts = meta.varint()
        if nparts > len(meta.data):
            raise truncated()
        parts: List[Tuple[int, Any]] = []
        for _ in range(nparts):
            kind = meta.u8()
            if kind == 0:
                parts.append((0, meta.take(meta.varint())))
            elif kind == 1:
                parts.append((1, meta.u8()))
            else:
                raise truncated()
        streams = [iter(decode_ints(Reader(take()), n)) for kind, _ in parts if kind == 1]

        def template() -> bytes:
            out = bytearray()
            f = 0
            for kind, val in parts:
                if kind == 0:
                    out += val
                else:
                    out += str(next(streams[f])).zfill(val).encode() if val else str(next(streams[f])).encode()
                    f += 1
            return bytes(out)

        cells = emit(template)
    else:
        raise FormatError(f"unknown DCZ column encoding {enc}")
    return cells, quoted


# ---------------------------------------------------------------------------------
# CSV blocks
# ---------------------------------------------------------------------------------


def _tokenize(data: bytes, delim: int, quote: Optional[int]) -> Iterator[Tuple[int, int, List[Tuple[int, int, bool]], int, bool]]:
    """Yield (start, end, fields[(start, end, quoted)], term_code, regular) per record."""
    n = len(data)
    pos = 0
    special = re.compile(b"[" + re.escape(bytes([delim])) + b"\r\n]")
    qb = bytes([quote]) if quote is not None else b""
    while pos < n:
        start = pos
        fields: List[Tuple[int, int, bool]] = []
        regular = True
        while True:
            if quote is not None and pos < n and data[pos] == quote:
                i = pos + 1
                while True:
                    j = data.find(qb, i)
                    if j < 0:
                        break
                    if j + 1 < n and data[j + 1] == quote:
                        i = j + 2
                        continue
                    break
                if j < 0:
                    fields.append((pos + 1, n, True))
                    pos = n
                    regular = False
                    term = 3
                    break
                fields.append((pos + 1, j, True))
                pos = j + 1
                if pos < n and data[pos] not in (delim, 10, 13):
                    regular = False
                    m = special.search(data, pos)
                    pos = m.start() if m else n
            else:
                m = special.search(data, pos)
                k = m.start() if m else n
                fields.append((pos, k, False))
                pos = k
            if pos >= n:
                term = 3
                break
            b = data[pos]
            if b == delim:
                pos += 1
                if pos >= n:
                    fields.append((n, n, False))
                    term = 3
                    break
                continue
            if b == 10:
                pos += 1
                term = 0
            elif pos + 1 < n and data[pos + 1] == 10:
                pos += 2
                term = 1
            else:
                pos += 1
                term = 2
            break
        yield start, pos, fields, term, regular


def _assemble(streams: Sequence[Tuple[int, int, bytes]]) -> bytes:
    out = bytearray()
    put_varint(out, len(streams))
    for c, raw, data in streams:
        out.append(c)
        put_varint(out, raw)
        put_varint(out, len(data))
    for _, _, data in streams:
        out += data
    return bytes(out)


BROTLI_MAX_STREAM = 1 << 20  # quality 11 runs at about 1 MB/s: small streams only


def _compress_stream(data: bytes, profile: str) -> Tuple[int, int, bytes]:
    if len(data) < 48:
        return codec.NONE, len(data), data
    candidates = codec.stream_candidates(profile)
    if profile == "max" and len(data) <= BROTLI_MAX_STREAM and codec.available(codec.BROTLI):
        candidates = [*candidates, (codec.BROTLI, 11)]
    c, out = codec.compress_best(candidates, data)
    return c, len(data), out


def _quick_size(data: bytes) -> int:
    """Fast size estimate used to rank candidate encodings."""
    if len(data) < 48:
        return len(data)
    fast = codec.ZSTD if codec.available(codec.ZSTD) else codec.ZLIB
    return min(len(data), len(codec.compress(fast, data, 3 if fast == codec.ZSTD else 1)))


def encode_csv_block(data: bytes, delimiter: str = ",", quote: Optional[str] = '"', first: bool = True,
                     profile: str = "balanced") -> Optional[Tuple[bytes, int]]:
    """Columnar payload for a block of CSV text, or None when it does not apply."""
    delim = ord(delimiter)
    q = ord(quote) if quote else None
    records = list(_tokenize(data, delim, q))
    counts = Counter(len(f) for i, (_, _, f, _, reg) in enumerate(records) if reg and not (first and i == 0))
    if not counts:
        return None
    ncols = max(counts, key=lambda k: (counts[k], k))  # same rule as the native encoder
    if counts[ncols] * 2 < len(records):
        return None
    cells: List[List[bytes]] = [[] for _ in range(ncols)]
    quoted: List[List[bool]] = [[] for _ in range(ncols)]
    terms: List[int] = []
    raw_pos: List[int] = []
    raw_len: List[int] = []
    raw_parts: List[bytes] = []
    for i, (s, e, fields, term, reg) in enumerate(records):
        if (first and i == 0) or not reg or len(fields) != ncols:
            raw_pos.append(i)
            raw_len.append(e - s)
            raw_parts.append(data[s:e])
            continue
        for j, (fs, fe, fq) in enumerate(fields):
            cells[j].append(data[fs:fe])
            quoted[j].append(fq)
        terms.append(term)
    rows = len(terms)
    if rows == 0:
        return None
    last_term = terms[-1]
    body = terms[:-1]
    term_mode = last_term if not body else (body[0] if all(t == body[0] for t in body) else 255)
    meta = bytearray([1, delim, q or 0])
    put_varint(meta, len(records))
    put_varint(meta, rows)
    put_varint(meta, ncols)
    meta += bytes([term_mode, last_term])
    put_varint(meta, len(raw_pos))
    streams: List[Tuple[int, int, bytes]] = []
    if raw_pos:
        s = encode_ints(raw_pos) + encode_ints(raw_len) + b"".join(raw_parts)
        streams.append(_compress_stream(s, profile))
    if term_mode == 255:
        streams.append(_compress_stream(encode_ints(terms), profile))
    exhaustive = profile == "max"
    for j in range(ncols):
        options = column_candidates(cells[j], quoted[j], exhaustive)
        # rank with a fast codec; only the winner gets the profile's codecs
        best_meta, best_raw = options[0] if len(options) == 1 else min(
            options, key=lambda o: len(o[0]) + sum(_quick_size(x) + 6 for x in o[1])
        )
        meta += best_meta
        streams.extend(_compress_stream(x, profile) for x in best_raw)
    payload = _assemble([_compress_stream(bytes(meta), profile), *streams])
    try:
        if decode_csv_block(payload, len(data)) != data:
            return None
    except Exception:
        return None
    return payload, len(records)


@_guard
def decode_csv_block(payload: bytes, raw_len: int) -> bytes:
    r = Reader(payload)
    n = r.varint()
    if n > len(payload):
        raise truncated()
    directory = [(r.u8(), r.varint(), r.varint()) for _ in range(n)]
    pos = r.pos
    chunks = []
    for c, raw, stored in directory:
        chunks.append((c, raw, payload[pos : pos + stored]))
        if pos + stored > len(payload):
            raise truncated()
        pos += stored
    it = iter(chunks)

    def take() -> bytes:
        try:
            c, raw, data = next(it)
        except StopIteration:
            raise truncated() from None
        return codec.decompress(c, data, raw)

    meta = Reader(take())
    if meta.u8() != 1:
        raise FormatError("unsupported DCZ block version")
    delim, quote = meta.u8(), meta.u8()
    n_records, rows, ncols = meta.varint(), meta.varint(), meta.varint()
    term_mode, last_term = meta.u8(), meta.u8()
    n_raw = meta.varint()
    # every record holds at least one byte, and a column at least a delimiter's worth
    if rows > n_records or n_raw > n_records or n_records > raw_len + 1 or ncols > raw_len + 1:
        raise truncated()
    raws: Dict[int, bytes] = {}
    if n_raw:
        s = Reader(take())
        positions = decode_ints(s, n_raw)
        lengths = decode_ints(s, n_raw)
        if len(positions) != n_raw or len(lengths) != n_raw:
            raise truncated()
        for p, ln in zip(positions, lengths):
            raws[p] = s.take(ln)
    terms = decode_ints(Reader(take()), rows) if term_mode == 255 else None
    columns = [decode_column(meta, take, rows) for _ in range(ncols)]
    d = bytes([delim])
    qb = bytes([quote])
    out: List[bytes] = []
    row = 0
    for rec in range(n_records):
        raw = raws.get(rec)
        if raw is not None:
            out.append(raw)
            continue
        if row >= rows:
            raise truncated()
        parts = []
        for cells, quoted in columns:
            c = cells[row]
            parts.append(qb + c + qb if quoted[row] else c)
        code = last_term if row + 1 == rows else (terms[row] if terms is not None else term_mode)
        if code > 3:
            raise truncated()
        out.append(d.join(parts) + TERMS[code])
        row += 1
    data = b"".join(out)
    if row != rows or len(data) != raw_len:
        raise IntegrityError(f"DCZ block decoded to {len(data)} bytes, expected {raw_len}")
    return data


# ---------------------------------------------------------------------------------
# Container
# ---------------------------------------------------------------------------------


def _header(flags: int = 0) -> bytes:
    return MAGIC + bytes([VERSION, flags]) + b"\x00" * 6


def _block_header(kind: int, codec_id: int, raw_len: int, payload_len: int, crc: int, records: int) -> bytes:
    return _BH.pack(kind, codec_id, 0, raw_len, payload_len, crc, records)


def encode_block(data: bytes, csv: Optional[Tuple[str, Optional[str]]], first: bool, profile: str) -> Tuple[bytes, bytes, int, int]:
    """(header, payload, kind, records) for one block."""
    crc = _crc(data)
    if csv is not None and profile in ("balanced", "max"):
        enc = encode_csv_block(data, csv[0], csv[1], first, profile)
        if enc is not None:
            payload, records = enc
            if profile == "max":
                yardstick = [(codec.ZSTD, 19)] if codec.available(codec.ZSTD) else codec.stream_candidates("balanced")
                c, plain = codec.compress_best(yardstick, data)
                if len(plain) < len(payload):
                    return _block_header(KIND_RAW, c, len(data), len(plain), crc, 0), plain, KIND_RAW, 0
            return _block_header(KIND_CSV, 0, len(data), len(payload), crc, records), payload, KIND_CSV, records
    c, out = codec.compress_best(codec.stream_candidates(profile), data)
    return _block_header(KIND_RAW, c, len(data), len(out), crc, 0), out, KIND_RAW, 0


@_guard
def decode_block(kind: int, codec_id: int, payload: bytes, raw_len: int, crc: int) -> bytes:
    if kind == KIND_RAW:
        data = codec.decompress(codec_id, payload, raw_len)
    elif kind == KIND_CSV:
        data = decode_csv_block(payload, raw_len)
    else:
        raise FormatError(f"unknown DCZ block kind {kind}")
    if _crc(data) != crc:
        raise IntegrityError("DCZ block checksum mismatch")
    return data


@functools.lru_cache(maxsize=64)
def _scan_dialect(delimiter: str, quote: Optional[str], escape: Optional[str]) -> _csvtok.Dialect:
    return _csvtok.Dialect(delimiter, quote, escape, escape is None, text=False)


def _record_boundary(buf: bytes, quote: Optional[str], delimiter: str = ",", escape: Optional[str] = None) -> int:
    """Offset just after the last record terminator outside quotes (0 when there is
    none), by the quote rules every engine shares: a quote opens a field only at its
    start, and doubled or escaped quotes inside it are literal."""
    if not quote or quote.encode() not in buf:
        return buf.rfind(b"\n") + 1
    return _csvtok.last_record_end(buf, _scan_dialect(delimiter, quote, escape)) or 0


def iter_blocks(stream: Any, block_size: int, csv: Optional[Tuple[str, Optional[str]]],
                escape: Optional[str] = None) -> Iterator[bytes]:
    carry = b""
    while True:
        chunk = stream.read(block_size)
        if not chunk:
            if carry:
                yield carry
            return
        buf = carry + chunk
        if csv is None:
            yield buf
            carry = b""
            continue
        cut = _record_boundary(buf, csv[1], csv[0], escape)
        if cut == 0 and len(buf) < block_size * 8:
            carry = buf
            continue
        if cut == 0:
            cut = len(buf)
        yield buf[:cut]
        carry = buf[cut:]


class DczWriter:
    """Streaming DCZ encoder: ``write()`` bytes, ``close()`` to finish the file.

    Delimited text is cut into record-aligned blocks and encoded column-wise;
    ``csv=None`` stores opaque compressed blocks.
    """

    def __init__(self, out: Any, csv: Optional[Tuple[str, Optional[str]]] = (",", '"'), profile: str = "balanced",
                 block_size: int = 4 << 20, escape: Optional[str] = None) -> None:
        self.out = out
        self.csv = csv
        self.escape = escape
        self.profile = profile
        self.block_size = max(4096, block_size)
        self.pending = bytearray()
        self.blocks: List[Dict[str, int]] = []
        self.crc = 0
        self.raw_offset = 0
        self.offset = HEADER_LEN
        self.footer: Optional[Dict[str, Any]] = None
        out.write(_header())

    def _emit(self, data: bytes) -> None:
        header, payload, kind, records = encode_block(data, self.csv, not self.blocks, self.profile)
        self.out.write(header)
        self.out.write(payload)
        self.blocks.append({"offset": self.offset, "raw_offset": self.raw_offset, "raw_len": len(data),
                            "payload_len": len(payload), "kind": kind, "records": records})
        self.offset += len(header) + len(payload)
        self.raw_offset += len(data)
        self.crc = zlib.crc32(data, self.crc)

    def write(self, data: bytes) -> int:
        self.pending += data
        while len(self.pending) >= self.block_size:
            head = bytes(self.pending[: self.block_size])
            cut = len(head) if self.csv is None else _record_boundary(head, self.csv[1], self.csv[0], self.escape)
            if cut == 0:
                if len(self.pending) < self.block_size * 8:
                    break
                cut = self.block_size
            self._emit(bytes(self.pending[:cut]))
            del self.pending[:cut]
        return len(data)

    def close(self) -> Dict[str, Any]:
        if self.footer is not None:
            return self.footer
        if self.pending:
            self._emit(bytes(self.pending))
            self.pending.clear()
        footer = {
            "format": "dcz", "version": VERSION, "created_by": f"dataconv {_version()} (python)",
            "kind": "csv" if self.csv else "raw", "profile": self.profile, "original_size": self.raw_offset,
            "original_crc32": self.crc & 0xFFFFFFFF,
            "delimiter": self.csv[0] if self.csv else None, "quote": self.csv[1] if self.csv else None,
            "blocks": self.blocks,
        }  # fmt: skip
        js = json.dumps(footer, separators=(",", ":")).encode()
        packed = zlib.compress(js, 9)
        c, body = (codec.ZLIB, packed) if len(packed) < len(js) else (codec.NONE, js)
        self.out.write(_block_header(KIND_FOOTER, c, len(js), len(body), _crc(js), 0))
        self.out.write(body)
        self.out.write(struct.pack("<Q", self.offset) + MAGIC_END)
        self.footer = footer
        return footer


def write_file(src_stream: Any, dst: os.PathLike[str] | str, csv: Optional[Tuple[str, Optional[str]]],
               profile: str = "balanced", block_size: int = 4 << 20, progress: Optional[Callable[[int, int], Any]] = None,
               total: int = 0, escape: Optional[str] = None) -> Dict[str, Any]:
    """Write a DCZ file from a binary stream (sequential encoder)."""
    with open(dst, "wb") as out:
        writer = DczWriter(out, csv, profile, block_size, escape)
        for data in iter_blocks(src_stream, block_size, csv, escape):
            writer.write(data)
            if progress is not None:
                progress(writer.raw_offset + len(writer.pending), total)
        return writer.close()


def _version() -> str:
    from . import __version__

    return __version__


@_guard
def read_footer(path: os.PathLike[str] | str) -> Dict[str, Any]:
    """The footer, with its block index checked against the file: block headers
    carry no checksum of their own, so only an index that tiles the file exactly
    lets their lengths be trusted."""
    with open(path, "rb") as f:
        head = f.read(HEADER_LEN)
        if len(head) < HEADER_LEN or not head.startswith(MAGIC):
            raise FormatError(f"{path} is not a DCZ file")
        if head[8] != VERSION:
            raise FormatError(f"unsupported DCZ version {head[8]}")
        if head[9] & FLAG_PACKED:
            raise FormatError(f"{path} holds packed values; use dataconv.unpack()")
        f.seek(0, os.SEEK_END)
        size = f.tell()
        if size < HEADER_LEN + BLOCK_HEADER_LEN + TRAILER_LEN:
            raise IntegrityError("DCZ file is truncated")
        f.seek(size - TRAILER_LEN)
        trailer = f.read(TRAILER_LEN)
        if trailer[8:] != MAGIC_END:
            raise IntegrityError("DCZ trailer missing: the file is truncated")
        (footer_offset,) = struct.unpack("<Q", trailer[:8])
        if not HEADER_LEN <= footer_offset <= size - TRAILER_LEN - BLOCK_HEADER_LEN:
            raise IntegrityError("DCZ footer is corrupt")
        f.seek(footer_offset)
        kind, c, _, raw_len, payload_len, crc, _ = _BH.unpack(f.read(BLOCK_HEADER_LEN))
        # the footer block runs exactly to the trailer
        if kind != KIND_FOOTER or footer_offset + BLOCK_HEADER_LEN + payload_len != size - TRAILER_LEN or raw_len > (256 << 20):
            raise IntegrityError("DCZ footer is corrupt")
        js = codec.decompress(c, f.read(payload_len), raw_len)
        if _crc(js) != crc:
            raise IntegrityError("DCZ footer checksum mismatch")
    footer = json.loads(js)
    if not isinstance(footer, dict) or not isinstance(footer.get("blocks"), list):
        raise IntegrityError("DCZ footer is corrupt")
    _check_index(footer, footer_offset)
    return footer  # type: ignore[no-any-return]


def _check_index(footer: Dict[str, Any], footer_offset: int) -> None:
    """Blocks follow one another from the file header to the footer, and their raw
    lengths add up to the original size."""
    offset, raw = HEADER_LEN, 0
    for b in footer["blocks"]:
        if not isinstance(b, dict):
            raise IntegrityError("DCZ block index is corrupt")
        fields = [b.get(k) for k in ("offset", "raw_offset", "raw_len", "payload_len", "kind")]
        if any(type(v) is not int or v < 0 for v in fields) or b["kind"] not in (KIND_RAW, KIND_CSV):
            raise IntegrityError("DCZ block index is corrupt")
        if b["offset"] != offset or b["raw_offset"] != raw:
            raise IntegrityError("DCZ block index does not match the file")
        offset += BLOCK_HEADER_LEN + b["payload_len"]
        raw += b["raw_len"]
    if offset != footer_offset or raw != footer.get("original_size"):
        raise IntegrityError("DCZ block index does not match the file")


def iter_decoded(path: os.PathLike[str] | str, verify: bool = True) -> Iterator[bytes]:
    """Yield the original bytes block by block, verifying every checksum."""
    footer = read_footer(path)
    crc = 0
    produced = 0
    with open(path, "rb") as f:
        for b in footer["blocks"]:
            f.seek(b["offset"])
            header = f.read(BLOCK_HEADER_LEN)
            if len(header) != BLOCK_HEADER_LEN:
                raise IntegrityError("DCZ file is truncated")
            kind, c, _, raw_len, payload_len, bcrc, _ = _BH.unpack(header)
            # a header must match its (checksummed) index entry before its lengths are used
            if (kind, raw_len, payload_len) != (b["kind"], b["raw_len"], b["payload_len"]):
                raise IntegrityError("DCZ block header does not match the block index")
            payload = f.read(payload_len)
            if len(payload) != payload_len:
                raise IntegrityError("DCZ file is truncated")
            data = decode_block(kind, c, payload, raw_len, bcrc)
            crc = zlib.crc32(data, crc)
            produced += len(data)
            yield data
    if verify and (produced != footer.get("original_size") or (crc & 0xFFFFFFFF) != footer.get("original_crc32")):
        raise IntegrityError("DCZ content checksum mismatch")


class DczReader(io.RawIOBase):
    """A read-only binary stream over the decoded contents of a DCZ file."""

    def __init__(self, path: os.PathLike[str] | str) -> None:
        super().__init__()
        self._it = iter_decoded(path)
        self._buf = b""

    def _more(self) -> bool:
        try:
            self._buf += next(self._it)
            return True
        except StopIteration:
            return False

    def read(self, n: int = -1) -> bytes:  # type: ignore[override]
        if n is None or n < 0:
            while self._more():
                pass
            out, self._buf = self._buf, b""
            return out
        while len(self._buf) < n and self._more():
            pass
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def readall(self) -> bytes:
        return self.read(-1)

    def readinto(self, b: Any) -> int:
        if not self._buf and not self._more():
            return 0
        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n

    def readable(self) -> bool:
        return True

    def close(self) -> None:
        if not self.closed:
            self._it.close()  # releases the file (Windows cannot delete open files)
        super().close()

    def __enter__(self) -> DczReader:
        return self


# ---------------------------------------------------------------------------------
# Packed values
# ---------------------------------------------------------------------------------

POW10 = [10.0**e for e in range(23)]


def _decimal_mantissa(v: float, e: int) -> Optional[int]:
    if v != v or v in (float("inf"), float("-inf")) or (v == 0.0 and str(v).startswith("-")):
        return None
    scaled = v * POW10[e]
    if abs(scaled) >= 9007199254740992.0:
        return None
    # Python rounds halves to even and the native engine away from zero; either way the
    # candidate is only used when it reproduces the exact bits.
    m = float(round(scaled))
    if struct.pack("<d", m / POW10[e]) == struct.pack("<d", v):
        return int(m)
    return None


def pack_payload(kind: int, values: Sequence[Any], valid: Optional[Sequence[bool]], tag: bytes, profile: str) -> bytes:
    """Kind: 1 int64, 2 float64, 3 str (as bytes), 4 bool.  `values` are the present values."""
    count = len(valid) if valid is not None else len(values)
    meta = bytearray([kind])
    put_varint(meta, count)
    meta.append(1 if valid is not None else 0)
    put_varint(meta, len(tag))
    meta += tag
    streams: List[Tuple[int, int, bytes]] = []
    if valid is not None:
        bm = bytearray()
        put_bitmap(bm, valid)
        streams.append(_compress_stream(bytes(bm), profile))
    if kind == 1:
        streams.append(_compress_stream(encode_ints(values), profile))
    elif kind == 4:
        bm = bytearray()
        put_bitmap(bm, values)
        streams.append(_compress_stream(bytes(bm), profile))
    elif kind == 2:
        floats = [float(v) for v in values]
        step = max(1, len(floats) // 2048)
        sample = floats[::step]
        best = (0, len(sample) + 1)
        for e in range(19):
            misses = sum(1 for v in sample if _decimal_mantissa(v, e) is None)
            if misses < best[1]:
                best = (e, misses)
                if misses == 0:
                    break
        candidates: List[Tuple[bytes, List[Tuple[int, int, bytes]]]] = []
        if best[1] * 4 < max(1, len(floats)):
            e = best[0]
            mant: List[int] = []
            positions: List[int] = []
            bits = bytearray()
            last = 0
            for i, v in enumerate(floats):
                m = _decimal_mantissa(v, e)
                if m is None:
                    mant.append(last)
                    positions.append(i)
                    bits += struct.pack("<d", v)
                else:
                    mant.append(m)
                    last = m
            params = bytearray([0, e])
            put_varint(params, len(positions))
            ss = [_compress_stream(encode_ints(mant), profile)]
            if positions:
                ss.append(_compress_stream(encode_ints(positions) + bytes(bits), profile))
            candidates.append((bytes(params), ss))
        raw = struct.pack(f"<{len(floats)}d", *floats)
        words = struct.unpack(f"<{len(floats)}Q", raw)
        xored = []
        prev = 0
        for w in words:
            xored.append(w ^ prev)
            prev = w
        candidates.append((b"\x01", [_compress_stream(_planes_encode(xored, 8), profile)]))
        params, ss = min(candidates, key=lambda c: len(c[0]) + sum(len(s[2]) + 6 for s in c[1]))
        meta += params
        streams.extend(ss)
    elif kind == 3:
        cells = list(values)
        options = column_candidates(cells, [False] * len(cells), profile == "max")
        best_c = None
        for m, ss in options:
            packed = [_compress_stream(x, profile) for x in ss]
            size = len(m) + sum(len(p[2]) + 6 for p in packed)
            if best_c is None or size < best_c[0]:
                best_c = (size, m, packed)
        assert best_c is not None
        meta += best_c[1]
        streams.extend(best_c[2])
    else:
        raise ValueError(f"unknown packed kind {kind}")
    return _assemble([(codec.NONE, len(meta), bytes(meta)), *streams])


def wrap_packed(payload: bytes) -> bytes:
    out = bytearray(_header(FLAG_PACKED))
    out.append(KIND_PACK)
    put_varint(out, len(payload))
    out += struct.pack("<I", _crc(payload))
    out += payload
    return bytes(out)


@_guard
def unwrap_packed(buf: bytes) -> bytes:
    if len(buf) < HEADER_LEN + 6 or not buf.startswith(MAGIC):
        raise FormatError("not DCZ data")
    if buf[8] != VERSION:
        raise FormatError(f"unsupported DCZ version {buf[8]}")
    if not buf[9] & FLAG_PACKED:
        raise FormatError("DCZ data holds a file, not packed values (decompress it instead)")
    r = Reader(buf, HEADER_LEN)
    if r.u8() != KIND_PACK:
        raise FormatError("unknown packed data kind")
    length = r.varint()
    (crc,) = struct.unpack("<I", r.take(4))
    payload = r.take(length)
    if not r.done():
        raise IntegrityError("trailing bytes after packed data")
    if _crc(payload) != crc:
        raise IntegrityError("packed data checksum mismatch")
    return payload


@_guard
def unpack_payload(payload: bytes) -> Tuple[int, List[Any], Optional[List[bool]], bytes]:
    r = Reader(payload)
    n = r.varint()
    directory = [(r.u8(), r.varint(), r.varint()) for _ in range(n)]
    pos = r.pos
    parts = []
    for c, raw, stored in directory:
        if pos + stored > len(payload):
            raise truncated()
        parts.append(codec.decompress(c, payload[pos : pos + stored], raw))
        pos += stored
    it = iter(parts)

    def take() -> bytes:
        try:
            return next(it)
        except StopIteration:
            raise truncated() from None

    meta = Reader(take())
    kind = meta.u8()
    count = meta.varint()
    has_nulls = meta.u8() != 0
    tag = meta.take(meta.varint())
    valid = get_bitmap(Reader(take()), count) if has_nulls else None
    present = sum(valid) if valid is not None else count
    if kind == 1:
        values: List[Any] = decode_ints(Reader(take()))
    elif kind == 4:
        values = get_bitmap(Reader(take()), present)
    elif kind == 2:
        enc = meta.u8()
        if enc == 0:
            e = meta.u8()
            n_exc = meta.varint()
            if e > 22:
                raise truncated()
            values = [m / POW10[e] for m in decode_ints(Reader(take()))]
            if n_exc:
                er = Reader(take())
                for p in decode_ints(er):
                    (values[p],) = struct.unpack("<d", er.take(8))
        elif enc == 1:
            planes = take()
            if len(planes) != present * 8:
                raise truncated()
            words = _planes_decode(planes, present, 8)
            values = []
            prev = 0
            for w in words:
                prev ^= w
                values.append(prev)
            values = list(struct.unpack(f"<{present}d", struct.pack(f"<{present}Q", *values)))
        else:
            raise FormatError(f"unknown float encoding {enc}")
    elif kind == 3:
        cells, _ = decode_column(meta, take, present)
        values = cells
    else:
        raise FormatError(f"unknown packed type {kind}")
    if len(values) != present:
        raise truncated()
    return kind, values, valid, tag
