"""Opening files with transparent (de)compression, for the Python engines."""

from __future__ import annotations

import codecs
import io
import os
import zipfile
from typing import IO, Any, Optional

from . import _compat, formats
from .errors import FormatError, MissingDependencyError


def _c1(error: UnicodeError) -> Any:
    # Bytes undefined in cp1252 decode to the matching C1 control, as WHATWG specifies
    # and as the native engine does, so decoding never fails.
    if isinstance(error, UnicodeDecodeError):
        return chr(error.object[error.start]), error.start + 1
    raise error


codecs.register_error("dataconv-c1", _c1)


def text_errors(encoding: str) -> str:
    return "dataconv-c1" if encoding.lower().replace("-", "") in ("cp1252", "windows1252") else "strict"


def _zstd_reader(raw: IO[bytes]) -> IO[bytes]:
    std = _compat.optional("compression.zstd")
    if std is not None:
        return std.ZstdFile(raw)  # type: ignore[no-any-return]
    zs = _compat.optional("zstandard")
    if zs is not None:
        return zs.ZstdDecompressor().stream_reader(raw, read_across_frames=True)  # type: ignore[no-any-return]
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
    """Close an outer stream together with the file underneath it."""

    def __init__(self, inner: Any, *owned: Any) -> None:
        self.inner = inner
        self.owned = owned

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return hasattr(self.inner, "write")

    def readinto(self, b: Any) -> int:
        data = self.inner.read(len(b))
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


def open_binary(path: os.PathLike[str] | str) -> IO[bytes]:
    """Open for reading, decompressing gzip, bz2, xz, zstd, lz4, brotli and zip."""
    _, compression = formats.detect(path)
    raw = open(path, "rb")  # noqa: SIM115 - returned to the caller, who closes it
    try:
        if compression is None:
            return raw
        if compression == "gzip":
            import gzip

            return io.BufferedReader(_Closing(gzip.GzipFile(fileobj=raw), raw), 1 << 20)
        if compression == "bz2":
            import bz2

            return io.BufferedReader(_Closing(bz2.BZ2File(raw), raw), 1 << 20)
        if compression == "xz":
            import lzma

            return io.BufferedReader(_Closing(lzma.LZMAFile(raw), raw), 1 << 20)
        if compression == "zstd":
            return io.BufferedReader(_Closing(_zstd_reader(raw), raw), 1 << 20)
        if compression == "lz4":
            lz4f = _compat.require("lz4.frame", "reading .lz4 files", "fast")
            return io.BufferedReader(_Closing(lz4f.LZ4FrameFile(raw), raw), 1 << 20)
        if compression == "brotli":
            br = _compat.optional("brotli") or _compat.require("brotlicffi", "reading .br files", "fast")
            data = br.decompress(raw.read())
            raw.close()
            return io.BytesIO(data)
        if compression == "zip":
            zf = zipfile.ZipFile(raw)
            members = [m for m in zf.infolist() if not m.is_dir()]
            if len(members) != 1:
                raise FormatError(f"{path}: zip archives must contain exactly one file ({len(members)} found)")
            return io.BufferedReader(_Closing(zf.open(members[0]), zf, raw), 1 << 20)
    except BaseException:
        raw.close()
        raise
    raw.close()
    raise FormatError(f"unsupported compression {compression!r}")


def open_text(path: os.PathLike[str] | str, encoding: str) -> IO[str]:
    binary = open_binary(path)
    enc = "utf-8-sig" if encoding.lower() in ("utf-8", "utf8") else encoding
    return io.TextIOWrapper(binary, encoding=enc, errors=text_errors(enc), newline="")


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
