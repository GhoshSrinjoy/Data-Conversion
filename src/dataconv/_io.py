"""Opening files with transparent (de)compression, for the Python engines."""

from __future__ import annotations

import codecs
import io
import os
import zipfile
from typing import IO, Any, Optional

from . import _compat, formats
from .errors import DataconvError, FormatError, IntegrityError, MissingDependencyError


def _c1(error: UnicodeError) -> Any:
    # Bytes undefined in cp1252 decode to the matching C1 control, as WHATWG specifies
    # and as the native engine does, so decoding never fails.
    if isinstance(error, UnicodeDecodeError):
        return chr(error.object[error.start]), error.start + 1
    raise error


codecs.register_error("dataconv-c1", _c1)


def text_errors(encoding: str) -> str:
    return "dataconv-c1" if encoding.lower().replace("-", "").replace("_", "") in ("cp1252", "windows1252") else "strict"


# Python codec names -> labels understood by the native engine.  Latin-1 stays true
# ISO-8859-1 (bytes 0x80-0x9F are C1 controls), as Python's codec decodes it: the
# WHATWG label "iso-8859-1" would mean windows-1252.
NATIVE_ENCODINGS = {
    "utf8": "utf-8",
    "utf-8-sig": "utf-8",
    "utf_8": "utf-8",
    "cp1252": "windows-1252",
    "latin-1": "latin-1",
    "latin1": "latin-1",
    "latin_1": "latin-1",
    "iso8859-1": "latin-1",
    "iso8859_1": "latin-1",
    "iso-8859-1": "latin-1",
    "l1": "latin-1",
    "utf-16-le": "utf-16le",
    "utf-16-be": "utf-16be",
    "utf_16_le": "utf-16le",
    "utf_16_be": "utf-16be",
    "shift-jis": "shift_jis",
    "sjis": "shift_jis",
}

#: The latin-1 aliases the native engine reads as ISO-8859-1 itself
_LATIN1 = frozenset({"latin-1", "latin1", "iso-8859-1", "iso8859-1", "l1", "iso-ir-100", "cp819", "ibm819"})

#: Every WHATWG encoding label the native engine accepts (encoding_rs), by the Python
#: codec of the encoding it names (None: no Python codec decodes it)
_WHATWG_LABELS = (
    (
        "cp1252",
        "ansi_x3.4-1968 ascii cp1252 cp819 csisolatin1 ibm819 iso-8859-1 iso-ir-100 iso8859-1 iso88591 "
        "iso_8859-1 iso_8859-1:1987 l1 latin1 us-ascii windows-1252 x-cp1252",
    ),
    ("iso8859-2", "csisolatin2 iso-8859-2 iso-ir-101 iso8859-2 iso88592 iso_8859-2 iso_8859-2:1987 l2 latin2"),
    ("iso8859-3", "csisolatin3 iso-8859-3 iso-ir-109 iso8859-3 iso88593 iso_8859-3 iso_8859-3:1988 l3 latin3"),
    ("iso8859-4", "csisolatin4 iso-8859-4 iso-ir-110 iso8859-4 iso88594 iso_8859-4 iso_8859-4:1988 l4 latin4"),
    ("iso8859-5", "csisolatincyrillic cyrillic iso-8859-5 iso-ir-144 iso8859-5 iso88595 iso_8859-5 iso_8859-5:1988"),
    (
        "iso8859-6",
        "arabic asmo-708 csiso88596e csiso88596i csisolatinarabic ecma-114 iso-8859-6 iso-8859-6-e "
        "iso-8859-6-i iso-ir-127 iso8859-6 iso88596 iso_8859-6 iso_8859-6:1987",
    ),
    (
        "iso8859-7",
        "csisolatingreek ecma-118 elot_928 greek greek8 iso-8859-7 iso-ir-126 iso8859-7 iso88597 "
        "iso_8859-7 iso_8859-7:1987 sun_eu_greek",
    ),
    (
        "iso8859-8",
        "csiso88598e csisolatinhebrew hebrew iso-8859-8 iso-8859-8-e iso-ir-138 iso8859-8 iso88598 "
        "iso_8859-8 iso_8859-8:1988 visual csiso88598i iso-8859-8-i logical",
    ),
    ("iso8859-10", "csisolatin6 iso-8859-10 iso-ir-157 iso8859-10 iso885910 l6 latin6"),
    ("iso8859-13", "iso-8859-13 iso8859-13 iso885913"),
    ("iso8859-14", "iso-8859-14 iso8859-14 iso885914"),
    ("iso8859-15", "csisolatin9 iso-8859-15 iso8859-15 iso885915 iso_8859-15 l9"),
    ("iso8859-16", "iso-8859-16"),
    (
        "cp1254",
        "cp1254 csisolatin5 iso-8859-9 iso-ir-148 iso8859-9 iso88599 iso_8859-9 iso_8859-9:1989 l5 latin5 windows-1254 x-cp1254",
    ),
    ("cp1250", "cp1250 windows-1250 x-cp1250"),
    ("cp1251", "cp1251 windows-1251 x-cp1251"),
    ("cp1253", "cp1253 windows-1253 x-cp1253"),
    ("cp1255", "cp1255 windows-1255 x-cp1255"),
    ("cp1256", "cp1256 windows-1256 x-cp1256"),
    ("cp1257", "cp1257 windows-1257 x-cp1257"),
    ("cp1258", "cp1258 windows-1258 x-cp1258"),
    ("cp874", "dos-874 iso-8859-11 iso8859-11 iso885911 tis-620 windows-874"),
    ("cp866", "866 cp866 csibm866 ibm866"),
    ("mac-roman", "csmacintosh mac macintosh x-mac-roman"),
    ("mac-cyrillic", "x-mac-cyrillic x-mac-ukrainian"),
    ("koi8-r", "cskoi8r koi koi8 koi8-r koi8_r"),
    ("koi8-u", "koi8-ru koi8-u"),
    ("gb18030", "chinese csgb2312 csiso58gb231280 gb2312 gb_2312 gb_2312-80 gbk iso-ir-58 x-gbk gb18030"),
    ("big5hkscs", "big5 big5-hkscs cn-big5 csbig5 x-x-big5"),
    ("utf-8", "unicode-1-1-utf-8 unicode11utf8 unicode20utf8 utf-8 utf8 x-unicode20utf8"),
    ("cp932", "csshiftjis ms932 ms_kanji shift-jis shift_jis sjis windows-31j x-sjis"),
    ("euc-jp", "cseucpkdfmtjapanese euc-jp x-euc-jp"),
    ("iso2022-jp", "csiso2022jp iso-2022-jp"),
    ("cp949", "cseuckr csksc56011987 euc-kr iso-ir-149 korean ks_c_5601-1987 ks_c_5601-1989 ksc5601 ksc_5601 windows-949"),
    ("utf-16-le", "csunicode iso-10646-ucs-2 ucs-2 unicode unicodefeff utf-16 utf-16le"),
    ("utf-16-be", "unicodefffe utf-16be"),
    (None, "csiso2022kr hz-gb-2312 iso-2022-cn iso-2022-cn-ext iso-2022-kr replacement x-user-defined"),
)
_WHATWG = {label: codec for codec, labels in _WHATWG_LABELS for label in labels.split()}


def known_encoding(label: str) -> bool:
    """Whether the native engine accepts an encoding label (a WHATWG label, also with
    ``_`` for ``-`` as Python spells them, or a latin-1 alias), as Python passes it."""
    key = label.strip().lower()
    key = NATIVE_ENCODINGS.get(key, key)
    if key.replace("_", "-") in _LATIN1:
        return True
    return key in _WHATWG or key.replace("_", "-") in _WHATWG


def python_codec(label: str) -> str:
    """The Python codec that decodes text in an encoding label: Python's own reading
    of a label it knows, else that of the WHATWG encoding the label names."""
    try:
        codecs.lookup(label)
        return label
    except LookupError:
        key = label.strip().lower()
        codec = _WHATWG.get(key) or _WHATWG.get(key.replace("_", "-"))
        return codec or label


class _ZstdFrames(io.RawIOBase):
    """Every frame of a zstd stream, failing on a truncated last frame (zstandard's
    stream reader just stops there, silently dropping the rest)."""

    def __init__(self, raw: IO[bytes], zs: Any) -> None:
        super().__init__()
        self.raw, self.zs = raw, zs
        self.dobj = zs.ZstdDecompressor().decompressobj()
        self.pending = b""
        self.in_frame = False
        self.done = False

    def readable(self) -> bool:
        return True

    def _feed(self, data: bytes) -> None:
        out = []
        while data:
            self.in_frame = True
            out.append(self.dobj.decompress(data))
            if not self.dobj.eof:
                break
            # a frame ended: the next one starts with the unused input
            data = self.dobj.unused_data
            self.dobj = self.zs.ZstdDecompressor().decompressobj()
            self.in_frame = False
        self.pending += b"".join(out)

    def readinto(self, b: Any) -> int:
        while not self.pending and not self.done:
            data = self.raw.read(1 << 20)
            if not data:
                if self.in_frame:
                    raise EOFError("the zstd data ends inside a frame")
                self.done = True
                break
            self._feed(data)
        n = min(len(b), len(self.pending))
        b[:n] = self.pending[:n]
        self.pending = self.pending[n:]
        return n


def _zstd_reader(raw: IO[bytes]) -> IO[bytes]:
    std = _compat.optional("compression.zstd")
    if std is not None:
        return std.ZstdFile(raw)  # type: ignore[no-any-return]
    zs = _compat.optional("zstandard")
    if zs is not None:
        return _ZstdFrames(raw, zs)  # type: ignore[return-value]
    pz = _compat.optional("pyzstd")
    if pz is not None:
        return pz.ZstdFile(raw)  # type: ignore[no-any-return]
    pa = _compat.optional("pyarrow")
    if pa is not None:
        return pa.CompressedInputStream(pa.PythonFile(raw, mode="r"), "zstd")  # type: ignore[no-any-return]
    raise MissingDependencyError('reading .zst files needs zstandard or pyarrow: pip install "dataconv[fast]"')


def _zstd_writer(raw: IO[bytes], level: int) -> IO[bytes]:
    std = _compat.optional("compression.zstd")
    if std is not None:
        return std.ZstdFile(raw, "w", level=level)  # type: ignore[no-any-return]
    zs = _compat.optional("zstandard")
    if zs is not None:
        return zs.ZstdCompressor(level=level).stream_writer(raw, closefd=False)  # type: ignore[no-any-return]
    pz = _compat.optional("pyzstd")
    if pz is not None:
        return pz.ZstdFile(raw, "w", level_or_option=level)  # type: ignore[no-any-return]
    raise MissingDependencyError('writing .zst files needs zstandard: pip install "dataconv[fast]"')


class _Closing(io.RawIOBase):
    """Close an outer stream together with the file underneath it.  A decompressor
    failing on truncated or corrupt input raises an IntegrityError naming the file."""

    def __init__(self, inner: Any, *owned: Any, what: str = "") -> None:
        self.inner = inner
        self.owned = owned
        self.what = what

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return hasattr(self.inner, "write")

    def readinto(self, b: Any) -> int:
        try:
            data = self.inner.read(len(b))
        except DataconvError:
            raise
        except Exception as exc:
            raise _corrupt(self.what, exc) from None
        n = len(data)
        b[:n] = data
        return n

    def write(self, b: Any) -> int:
        return self.inner.write(b) or len(b)

    def close(self) -> None:
        if not self.closed:
            try:
                for obj in (self.inner, *self.owned):
                    try:
                        obj.close()
                    except Exception:
                        pass
            finally:
                super().close()


def _corrupt(what: str, exc: BaseException) -> IntegrityError:
    detail = str(exc) or type(exc).__name__
    return IntegrityError(f"{what}: the compressed data is truncated or corrupt ({detail})")


def open_binary(path: os.PathLike[str] | str) -> IO[bytes]:
    """Open for reading, decompressing gzip, bz2, xz, zstd, lz4, brotli and zip
    (every member, stream or frame of concatenated ones)."""
    _, compression = formats.detect(path)
    raw = open(path, "rb")  # noqa: SIM115 - returned to the caller, who closes it
    what = f"{path} ({compression})"
    try:
        if compression is None:
            return raw
        if compression == "gzip":
            import gzip

            return io.BufferedReader(_Closing(gzip.GzipFile(fileobj=raw), raw, what=what), 1 << 20)
        if compression == "bz2":
            import bz2

            return io.BufferedReader(_Closing(bz2.BZ2File(raw), raw, what=what), 1 << 20)
        if compression == "xz":
            import lzma

            return io.BufferedReader(_Closing(lzma.LZMAFile(raw), raw, what=what), 1 << 20)
        if compression == "zstd":
            return io.BufferedReader(_Closing(_zstd_reader(raw), raw, what=what), 1 << 20)
        if compression == "lz4":
            lz4f = _compat.require("lz4.frame", "reading .lz4 files", "fast")
            return io.BufferedReader(_Closing(lz4f.LZ4FrameFile(raw), raw, what=what), 1 << 20)
        if compression == "brotli":
            br = _compat.optional("brotli") or _compat.require("brotlicffi", "reading .br files", "fast")
            try:
                data = br.decompress(raw.read())
            except Exception as exc:
                raise _corrupt(what, exc) from None
            raw.close()
            return io.BytesIO(data)
        if compression == "zip":
            try:
                zf = zipfile.ZipFile(raw)
            except (zipfile.BadZipFile, OSError, ValueError, EOFError) as exc:
                raise _corrupt(what, exc) from None
            members = [m for m in zf.infolist() if not m.is_dir()]
            if len(members) != 1:
                raise FormatError(f"{path}: zip archives must contain exactly one file ({len(members)} found)")
            try:
                member = zf.open(members[0])
            except (zipfile.BadZipFile, OSError, ValueError, EOFError, NotImplementedError, RuntimeError) as exc:
                raise _corrupt(what, exc) from None
            return io.BufferedReader(_Closing(member, zf, raw, what=what), 1 << 20)
    except BaseException:
        raw.close()
        raise
    raw.close()
    raise FormatError(f"unsupported compression {compression!r}")


def is_utf8(encoding: str) -> bool:
    """Whether an encoding label names UTF-8 (in any spelling)."""
    try:
        return codecs.lookup(python_codec(encoding)).name in ("utf-8", "utf-8-sig")
    except LookupError:
        return False


def native_label(encoding: str) -> str:
    """The label the native engine is given for a Python encoding name."""
    key = encoding.lower()
    return NATIVE_ENCODINGS.get(key, key)


class _NoBom(io.TextIOBase):
    """A text stream without the byte order mark it starts with (codecs such as
    utf-16-le keep it as a character)."""

    def __init__(self, inner: IO[str]) -> None:
        super().__init__()
        self.inner = inner
        self.first = True

    @property
    def buffer(self) -> Any:
        return getattr(self.inner, "buffer", None)

    def readable(self) -> bool:
        return True

    def read(self, size: Optional[int] = -1) -> str:
        data = self.inner.read(size)
        if self.first:
            self.first = False
            if data[:1] == "﻿":
                data = data[1:] or self.inner.read(size)
        return data

    def readline(self, size: Optional[int] = -1) -> str:  # type: ignore[override]
        line = self.inner.readline(size)
        if self.first:
            self.first = False
            if line[:1] == "﻿":
                line = line[1:] or self.inner.readline(size)
        return line

    def close(self) -> None:
        if not self.closed:
            self.inner.close()
        super().close()


def text_stream(binary: IO[bytes], encoding: str) -> IO[str]:
    """A text stream of ``binary`` in an encoding label (as :func:`python_codec` reads
    it), without a leading byte order mark in any UTF encoding."""
    encoding = python_codec(encoding)
    name = codecs.lookup(encoding).name
    enc = "utf-8-sig" if name == "utf-8" else encoding
    text = io.TextIOWrapper(binary, encoding=enc, errors=text_errors(enc), newline="")
    if name in ("utf-16-le", "utf-16-be", "utf-32-le", "utf-32-be"):
        return _NoBom(text)  # type: ignore[return-value]
    return text


def open_text(path: os.PathLike[str] | str, encoding: str) -> IO[str]:
    binary = open_binary(path)
    try:
        return text_stream(binary, encoding)
    except BaseException:
        binary.close()
        raise


def open_output(path: os.PathLike[str] | str, compression: Optional[str], level: Optional[int] = None) -> IO[bytes]:
    """Open a binary output stream, compressing according to `compression`."""
    raw = open(path, "wb")  # noqa: SIM115 - returned to the caller, who closes it
    try:
        if compression is None:
            return raw
        if compression == "gzip":
            import gzip

            return _Closing(gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=level or 6, mtime=0), raw)  # type: ignore[return-value]
        if compression == "bz2":
            import bz2

            return _Closing(bz2.BZ2File(raw, "wb", compresslevel=level or 9), raw)  # type: ignore[return-value]
        if compression == "xz":
            import lzma

            return _Closing(lzma.LZMAFile(raw, "wb", preset=level if level is not None else 6), raw)  # type: ignore[return-value]
        if compression == "zstd":
            return _Closing(_zstd_writer(raw, level or 3), raw)  # type: ignore[return-value]
        if compression == "lz4":
            lz4f = _compat.require("lz4.frame", "writing .lz4 files", "fast")
            return _Closing(lz4f.LZ4FrameFile(raw, "wb"), raw)  # type: ignore[return-value]
    except BaseException:
        raw.close()
        raise
    raw.close()
    raise FormatError(f"writing {compression} compressed output is not supported")
