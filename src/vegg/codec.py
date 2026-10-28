"""Byte codecs with automatic backend selection (the Python twin of ``codec.rs``).

Each codec uses the best implementation available on this machine:

=======  ===============================================================
zstd     ``compression.zstd`` (Python 3.14+) > zstandard > pyzstd > pyarrow
lz4      lz4 > pyarrow (LZ4 frame format)
brotli   brotli > brotlicffi > pyarrow
zlib     standard library
lzma     standard library (the ``.lzma``/LZMA_Alone container)
=======  ===============================================================

All backends of a codec produce interchangeable streams.  The numeric ids are part
of the DCZ file format.
"""

from __future__ import annotations

import io
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from . import _compat
from .errors import IntegrityError, MissingDependencyError, OptionsError

NONE, ZLIB, LZMA, ZSTD, LZ4, BROTLI = 0, 1, 3, 4, 5, 6
NAMES = {NONE: "none", ZLIB: "zlib", LZMA: "lzma", ZSTD: "zstd", LZ4: "lz4", BROTLI: "brotli"}
IDS = {v: k for k, v in NAMES.items()}
ALIASES = {"gzip": "zlib", "deflate": "zlib", "xz": "lzma", "zst": "zstd", "br": "brotli", "raw": "none", "store": "none"}

# level per profile, mirroring the native engine
PROFILE_LEVELS = {
    "fast": {ZSTD: 1, ZLIB: 1, LZMA: 1, BROTLI: 2, LZ4: 0, NONE: 0},
    "balanced": {ZSTD: 9, ZLIB: 6, LZMA: 6, BROTLI: 7, LZ4: 0, NONE: 0},
    "max": {ZSTD: 19, ZLIB: 9, LZMA: 9, BROTLI: 11, LZ4: 0, NONE: 0},
}

Compressor = Callable[[bytes, int], bytes]
#: (data, declared size) -> at most size + 1 decoded bytes: a stream is never
#: expanded further than its declared length allows
Decompressor = Callable[[bytes, int], bytes]
_cache: Dict[int, Optional[Tuple[str, Compressor, Decompressor]]] = {}


def _read_up_to(reader: Any, limit: int) -> bytes:
    parts = []
    while limit > 0:
        chunk = reader.read(limit)
        if not chunk:
            break
        parts.append(chunk)
        limit -= len(chunk)
    return b"".join(parts)


def _zstd() -> Optional[Tuple[str, Compressor, Decompressor]]:
    std = _compat.optional("compression.zstd")
    if std is not None:
        return "stdlib", lambda d, lvl: std.compress(d, level=lvl), lambda d, n: std.ZstdDecompressor().decompress(d, max_length=n + 1)
    zs = _compat.optional("zstandard")
    if zs is not None:
        return (
            "zstandard",
            lambda d, lvl: zs.ZstdCompressor(level=lvl).compress(d),
            lambda d, n: _read_up_to(zs.ZstdDecompressor().stream_reader(io.BytesIO(d)), n + 1),
        )
    pz = _compat.optional("pyzstd")
    if pz is not None:
        return "pyzstd", lambda d, lvl: pz.compress(d, lvl), lambda d, n: pz.ZstdDecompressor().decompress(d, max_length=n + 1)
    return _pyarrow("zstd")


def _pyarrow(name: str) -> Optional[Tuple[str, Compressor, Decompressor]]:
    pa = _compat.optional("pyarrow")
    if pa is None or not pa.Codec.is_available(name):
        return None

    def comp(d: bytes, lvl: int) -> bytes:
        try:
            codec = pa.Codec(name, compression_level=lvl) if name != "lz4" else pa.Codec(name)
        except (ValueError, TypeError):
            codec = pa.Codec(name)
        return codec.compress(d, asbytes=True)  # type: ignore[no-any-return]

    def decomp(d: bytes, n: int) -> bytes:
        if n == 0:
            return b""
        return pa.Codec(name).decompress(d, decompressed_size=n, asbytes=True)  # type: ignore[no-any-return]

    return "pyarrow", comp, decomp


def _lz4() -> Optional[Tuple[str, Compressor, Decompressor]]:
    f = _compat.optional("lz4.frame")
    if f is not None:
        return "lz4", lambda d, lvl: f.compress(d), lambda d, n: f.LZ4FrameDecompressor().decompress(d, max_length=n + 1)
    return _pyarrow("lz4")


def _brotli() -> Optional[Tuple[str, Compressor, Decompressor]]:
    br = _compat.optional("brotli") or _compat.optional("brotlicffi")
    if br is not None:

        def decomp(d: bytes, n: int) -> bytes:
            try:  # brotli >= 1.1 can stop early
                return br.Decompressor().process(d, output_buffer_limit=n + 1)  # type: ignore[no-any-return]
            except TypeError:
                return br.decompress(d)  # type: ignore[no-any-return]

        return "brotli", lambda d, lvl: br.compress(d, quality=lvl), decomp
    return _pyarrow("brotli")


def _zlib() -> Optional[Tuple[str, Compressor, Decompressor]]:
    import zlib

    return "stdlib", lambda d, lvl: zlib.compress(d, lvl), lambda d, n: zlib.decompressobj().decompress(d, n + 1)


def _lzma() -> Optional[Tuple[str, Compressor, Decompressor]]:
    lzma = _compat.optional("lzma")
    if lzma is None:
        return None

    def comp(d: bytes, lvl: int) -> bytes:
        preset = (9 | lzma.PRESET_EXTREME) if lvl >= 9 else max(0, lvl)
        dict_size = 1 << 16
        while dict_size < (1 << 26) and dict_size < len(d):
            dict_size <<= 1
        filters = [{"id": lzma.FILTER_LZMA1, "preset": preset, "dict_size": dict_size}]
        return lzma.compress(d, format=lzma.FORMAT_ALONE, filters=filters)  # type: ignore[no-any-return]

    return "stdlib", comp, lambda d, n: lzma.LZMADecompressor(format=lzma.FORMAT_ALONE).decompress(d, max_length=n + 1)


_FACTORIES = {ZSTD: _zstd, LZ4: _lz4, BROTLI: _brotli, ZLIB: _zlib, LZMA: _lzma}


def backend(codec: int) -> Optional[Tuple[str, Compressor, Decompressor]]:
    if codec == NONE:
        return "builtin", lambda d, lvl: bytes(d), lambda d, n: bytes(d)
    if codec not in _cache:
        factory = _FACTORIES.get(codec)
        _cache[codec] = factory() if factory else None
    return _cache[codec]


def available(codec: int) -> bool:
    return backend(codec) is not None


def available_names() -> Dict[str, Optional[str]]:
    return {NAMES[c]: (backend(c) or (None,))[0] for c in NAMES}


def codec_id(name: str) -> int:
    key = ALIASES.get(name.lower(), name.lower())
    if key not in IDS:
        raise OptionsError(f"unknown codec {name!r} (choose from {', '.join(IDS)})")
    return IDS[key]


def compress(codec: int, data: bytes, level: int) -> bytes:
    b = backend(codec)
    if b is None:
        raise MissingDependencyError(f"{NAMES.get(codec, codec)} compression is not available here; "
                                     'install it with: pip install "vegg[fast]"')
    return b[1](bytes(data), level)


def decompress(codec: int, data: bytes, size: int) -> bytes:
    if codec not in NAMES:  # a codec id no writer uses: corrupt data, not a missing package
        raise IntegrityError(f"unknown codec id {codec}: the data is corrupt")
    b = backend(codec)
    if b is None:
        raise MissingDependencyError(f"this data uses {NAMES.get(codec, codec)} compression, which is not "
                                     'available here; install it with: pip install "vegg[fast]"')
    try:
        out = b[2](bytes(data), size)
    except MissingDependencyError:
        raise
    except Exception as exc:
        raise IntegrityError(f"{NAMES[codec]} stream is corrupt: {exc}") from None
    if len(out) > size:
        raise IntegrityError(f"{NAMES[codec]} stream expands beyond the expected {size} bytes")
    if len(out) != size:
        raise IntegrityError(f"{NAMES[codec]} stream expanded to {len(out)} bytes, expected {size}")
    return out


def stream_candidates(profile: str) -> List[Tuple[int, int]]:
    """Codecs tried for a DCZ stream: the native choices that are available here."""
    levels = PROFILE_LEVELS[profile]
    if profile == "max":
        wanted = [ZSTD, LZMA]
    elif profile == "fast":
        wanted = [ZSTD, LZ4, ZLIB]
    else:
        wanted = [ZSTD, ZLIB]
    found = [(c, levels[c]) for c in wanted if available(c)]
    if profile != "max" and found:
        found = found[:1]
    return found or [(ZLIB, levels[ZLIB])]


def compress_best(candidates: Sequence[Tuple[int, int]], data: bytes) -> Tuple[int, bytes]:
    best: Optional[Tuple[int, bytes]] = None
    for codec, level in candidates:
        out = compress(codec, data, level)
        if best is None or len(out) < len(best[1]):
            best = (codec, out)
    if best is None or len(best[1]) >= len(data):
        return NONE, bytes(data)
    return best
