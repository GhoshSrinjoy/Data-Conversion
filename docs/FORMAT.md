# DCZ file format, version 1

DCZ stores a file **losslessly**: decompression always reproduces the original
bytes exactly, and every block carries a checksum of its decoded contents.
Delimited text (CSV, TSV, logs) is encoded column by column; anything else is
stored as compressed raw blocks. This document is the normative description;
`native/core/src/dcz/` (Rust) and `src/dataconv/dcz.py` (Python) implement it and
read each other's files.

All integers are little-endian. `varint` is unsigned LEB128; `zigzag` maps a
signed integer to an unsigned one (`(v << 1) ^ (v >> 63)`) before varint coding.

## Layout

```text
file    := header block* footer trailer
header  := magic[8] version:u8 flags:u8 reserved[6]          (16 bytes)
block   := block_header payload[payload_len]
footer  := block_header(kind = 254) payload                    (JSON, see below)
trailer := footer_offset:u64 magic_end[8]                       (16 bytes)

magic     = 89 44 43 5A 0D 0A 1A 0A      ("\x89DCZ\r\n\x1a\n")
magic_end = 89 44 43 5A 45 4E 44 0A      ("\x89DCZEND\n")
version   = 1
flags     : bit 0 set = packed values (see "Packed values"); other bits 0
```

The magic bytes follow the PNG convention, so newline translation or 7-bit
transfer corrupts them detectably.

### Block header (32 bytes)

| offset | field         | type | meaning                                              |
|-------:|---------------|------|------------------------------------------------------|
| 0      | `kind`        | u8   | 0 raw, 1 columnar CSV, 254 footer                    |
| 1      | `codec`       | u8   | codec of a raw block or of the footer (see Codecs)   |
| 2      | reserved      | u16  | 0                                                    |
| 4      | `raw_len`     | u64  | decoded length in bytes                              |
| 12     | `payload_len` | u64  | stored length in bytes                               |
| 20     | `crc32`       | u32  | CRC-32 (IEEE) of the decoded bytes                   |
| 24     | `records`     | u32  | records in a columnar block (informational)          |
| 28     | reserved      | u32  | 0                                                    |

Blocks are self-delimiting: a reader may stream them sequentially until it meets
the footer, or read the trailer and footer first and decode blocks in parallel.
A block's decoded bytes are the bytes of the original file at `raw_offset`;
concatenating all blocks in order reproduces the file.

Block headers carry no checksum of their own, so a reader checks them against
the footer's block index, which is checksummed: the blocks must follow one
another from the end of the file header to the footer, every header must match
its index entry (`kind`, `raw_len`, `payload_len`), and the `raw_len` values must
add up to `original_size`. Only then are lengths taken from a header trusted for
allocation.

### Footer

A UTF-8 JSON object, compressed with `codec` (zlib or none):

```json
{
  "format": "dcz", "version": 1, "created_by": "dataconv 1.0.0",
  "kind": "csv" | "raw", "profile": "balanced",
  "original_size": 142107433, "original_crc32": 1234567890,
  "delimiter": ",", "quote": "\"",
  "blocks": [{"offset": 16, "raw_offset": 0, "raw_len": 8388608,
              "payload_len": 1203344, "kind": 1, "records": 117000}]
}
```

Readers must ignore unknown keys. `original_crc32` covers the whole original
file and must be verified after decoding all blocks.

## Codecs

| id | codec  | stream format                                         |
|---:|--------|-------------------------------------------------------|
| 0  | none   | stored                                                |
| 1  | zlib   | RFC 1950 zlib stream                                  |
| 2  | -      | reserved, never written                               |
| 3  | lzma   | legacy `.lzma` (LZMA_Alone): 13-byte header, LZMA1 data |
| 4  | zstd   | Zstandard frame                                       |
| 5  | lz4    | LZ4 frame                                             |
| 6  | brotli | raw Brotli stream                                     |

Decoders always know the decoded length of a stream and must reject a stream
that expands to any other length.

## Raw blocks (kind 0)

`payload` is the block's bytes compressed with `codec`.

## Columnar CSV blocks (kind 1)

A block contains complete records (the last block of a file may end without a
record terminator). Records are classified:

* a **row** is a record with the block's dominant field count whose fields are
  all either unquoted or cleanly quoted (a quoted field closes with a quote that
  is followed by the delimiter, a terminator or the end of the data). A row's
  bytes are exactly `field (delimiter field)* terminator`, where each field is its
  content, wrapped in quote characters when it was quoted. The content is kept as
  written (escape sequences such as `""` are not decoded);
* every other record (and the first record of the first block, usually the
  header) is stored **verbatim** as a raw record.

### Payload

```text
payload := n:varint (codec:u8 raw_len:varint stored_len:varint){n} stream_bytes...
```

The directory lists `n` streams, which follow in order; stream `i` decodes to
`raw_len` bytes with `codec`. Stream 0 is the block's **meta** stream:

```text
meta := version:u8(=1) delimiter:u8 quote:u8 records:varint rows:varint
        columns:varint term_mode:u8 last_term:u8 raw_records:varint
        column_meta{columns}
```

Terminator codes: 0 `\n`, 1 `\r\n`, 2 `\r`, 3 none (end of data). Unless
`term_mode` is 255, every row except the last ends with `term_mode`; the last row
ends with `last_term`.

After the meta stream come, in order:

1. if `raw_records > 0`: one stream `positions:int_array lengths:int_array bytes`
   holding the raw records (record indices, byte lengths, concatenated bytes);
2. if `term_mode == 255`: one stream `int_array` with every row's terminator code;
3. the streams of each column, in column order.

To decode, walk records `0..records`: emit the next raw record when its index
comes up, otherwise emit the next row.

### Integer arrays

```text
int_array := count:varint transform:u8 body
  0 PLAIN  base:zigzag width:u8 planes
  1 DELTA  first:zigzag base:zigzag width:u8 planes      (count-1 deltas)
  2 RLE    runs:varint values:int_array lengths:int_array
```

`planes` holds `count` unsigned `width`-byte numbers (`width` 0-8), stored as
byte planes: all first bytes, then all second bytes, and so on. A value is
`base + number`; DELTA arrays store consecutive differences, starting from
`first`. All arithmetic wraps at 64 bits. The `values` and `lengths` of an RLE
array are never RLE arrays themselves; decoders reject nested RLE.

### Column meta and streams

```text
column_meta := encoding:u8 quoting:u8 exceptions:varint has_empty:u8 params
```

`quoting` is 0 (no cell quoted), 1 (every cell quoted) or 2 (per-cell bitmap).
When `quoting == 2`, `has_empty == 1` or `exceptions > 0`, the column's first
stream is an **aux** stream:

```text
aux := [quote bitmap] [empty bitmap] [positions:int_array lengths:int_array bytes]
```

Bitmaps are LSB-first, one bit per row. An *empty* cell has no content. An
*exception* is a cell stored verbatim at the given row. Encoders produce values
only for the remaining cells, in row order.

| id | encoding | params | streams | cell content |
|---:|----------|--------|---------|--------------|
| 0  | RAW      | -      | lengths:int_array, data | concatenated bytes |
| 1  | CONST    | len:varint bytes | - | the same bytes in every cell |
| 2  | DICT     | entries:varint | lengths:int_array, data, indices:int_array | dictionary entry |
| 3  | INT      | - | values:int_array | decimal text of the integer |
| 4  | DECIMAL  | scale:u8 (255 = per value) | [scales:int_array] mantissas:int_array | see below |
| 5  | TEMPLATE | parts:varint part* | one int_array per field | literals and numbers |

* **INT** cells are canonical integers `-?(0|[1-9][0-9]*)` of at most 18 digits
  (never `-0`).
* **DECIMAL** cells are `-?(0|[1-9][0-9]*)(\.[0-9]+)?` with at most 18 digits;
  the mantissa is the digits without the point, the scale the number of
  fractional digits. A cell is written as the mantissa's digits, left-padded
  with zeros to `scale + 1` digits, with a point inserted before the last
  `scale` digits, and a leading `-` for negative mantissas.
* **TEMPLATE** parts are `0 len:varint bytes` (a literal) or `1 width:u8` (a
  number field). A cell is its parts concatenated; a number field is written in
  decimal, left-padded with zeros to `width` digits (`width` 0 means no padding).
  For example `2024-03-07 12:05:00` is `field(4) "-" field(2) "-" field(2) " "
  field(2) ":" field(2) ":" field(2)`.

### Guarantee

Encoders must decode every block they produce and compare the result with the
input; if it differs, they must write the block as a raw block instead. A
conforming encoder therefore never produces a file that does not decompress to
its input, whatever the input bytes are.

## Packed values

`pack()` produces a compact frame with header flag bit 0 set:

```text
packed := header(flags = 1) kind:u8(=2) payload_len:varint crc32:u32 payload
payload := streams, as in columnar blocks; stream 0 (meta, stored) :=
  type:u8 count:varint has_nulls:u8 tag_len:varint tag
  type 1 int64, 2 float64, 3 UTF-8 strings, 4 bool
```

If `has_nulls`, the next stream is a validity bitmap of `count` bits (1 =
present); the remaining streams cover the present values only:

* int64: `values:int_array`;
* bool: a bitmap;
* strings: a text column (`column_meta` continues the meta stream, as above);
* float64: the meta continues with `encoding:u8`:
  * 0, decimal: `exponent:u8 exceptions:varint`; streams `mantissas:int_array`
    and, if there are exceptions, `positions:int_array bits:u64*`. A value is
    `mantissa / 10^exponent` in IEEE-754 double arithmetic (exact powers of ten);
    each exception replaces the value at its position with the stored bits. An
    encoder uses this only when the division reproduces the original bits.
  * 1, XOR planes: one stream of `count` 8-byte numbers in byte planes; each is
    the XOR of a value's bits with the previous value's bits.

`tag` is opaque caller metadata (dataconv stores the container type and numpy
dtype and shape there, as JSON).
