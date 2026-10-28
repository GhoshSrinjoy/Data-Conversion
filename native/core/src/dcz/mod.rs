//! DCZ: a lossless, block-parallel container for delimited text and raw bytes.
//!
//! ```text
//! file    := header block* footer trailer
//! header  := magic[8] = 89 'D' 'C' 'Z' 0D 0A 1A 0A, version:u8 = 1, flags:u8, reserved[6]
//! block   := kind:u8 codec:u8 reserved:u16 raw_len:u64 payload_len:u64 crc32:u32 records:u32
//!            reserved:u32 payload
//!            kind 0 = raw bytes compressed with `codec`
//!            kind 1 = columnar CSV (see block.rs), kind 254 = footer (JSON, codec applies)
//! trailer := footer_offset:u64 magic_end[8] = 89 'D' 'C' 'Z' 'E' 'N' 'D' 0A
//! ```
//! All integers are little-endian.  `crc32` (IEEE) covers the decoded bytes of the
//! block, and the footer records the CRC-32 of the whole original file, so every
//! decode is verified end to end.  Blocks are self-delimiting: a reader can stream
//! sequentially, or use the footer index to decode blocks in parallel.

pub mod block;
pub mod ints;
pub mod text;

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::chunk::Chunker;
use crate::codec::{self, CodecId, Profile};
use crate::convert::{Hooks, Report};
use crate::error::{Error, Result};
use crate::io::{self, AtomicFile};
use crate::resources;
use crate::sniff::{self, SniffOptions};
use crate::tokenize::Dialect;

pub const MAGIC: &[u8; 8] = b"\x89DCZ\r\n\x1a\n";
pub const MAGIC_END: &[u8; 8] = b"\x89DCZEND\n";
pub const VERSION: u8 = 1;
const HEADER_LEN: usize = 16;
const BLOCK_HEADER_LEN: usize = 32;
const TRAILER_LEN: usize = 16;

pub const KIND_RAW: u8 = 0;
pub const KIND_CSV: u8 = 1;
pub const KIND_FOOTER: u8 = 254;

#[derive(Clone, Debug)]
pub struct EncodeOptions {
    pub profile: Profile,
    /// 0 = all usable CPUs.
    pub threads: usize,
    pub block_size: Option<usize>,
    /// Memory budget in bytes (None: from the machine).
    pub memory: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockInfo {
    pub offset: u64,
    pub raw_offset: u64,
    pub raw_len: u64,
    pub payload_len: u64,
    pub kind: u8,
    pub records: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Footer {
    pub format: String,
    pub version: u8,
    pub created_by: String,
    /// "csv" (columnar text) or "raw"
    pub kind: String,
    pub profile: String,
    pub original_size: u64,
    pub original_crc32: u32,
    #[serde(default)]
    pub delimiter: Option<String>,
    #[serde(default)]
    pub quote: Option<String>,
    pub blocks: Vec<BlockInfo>,
}

struct BlockHeader {
    kind: u8,
    codec: u8,
    raw_len: u64,
    payload_len: u64,
    crc32: u32,
    records: u32,
}

impl BlockHeader {
    fn write(&self, out: &mut Vec<u8>) {
        out.push(self.kind);
        out.push(self.codec);
        out.extend_from_slice(&[0, 0]);
        out.extend_from_slice(&self.raw_len.to_le_bytes());
        out.extend_from_slice(&self.payload_len.to_le_bytes());
        out.extend_from_slice(&self.crc32.to_le_bytes());
        out.extend_from_slice(&self.records.to_le_bytes());
        out.extend_from_slice(&[0, 0, 0, 0]);
    }

    fn parse(b: &[u8; BLOCK_HEADER_LEN]) -> BlockHeader {
        BlockHeader {
            kind: b[0],
            codec: b[1],
            raw_len: u64::from_le_bytes(b[4..12].try_into().unwrap()),
            payload_len: u64::from_le_bytes(b[12..20].try_into().unwrap()),
            crc32: u32::from_le_bytes(b[20..24].try_into().unwrap()),
            records: u32::from_le_bytes(b[24..28].try_into().unwrap()),
        }
    }
}

fn file_header() -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[..8].copy_from_slice(MAGIC);
    h[8] = VERSION;
    h
}

/// One encoded block ready to be written.
struct EncodedBlock {
    header: Vec<u8>,
    payload: Vec<u8>,
    raw_len: u64,
    kind: u8,
    records: u32,
}

fn encode_block(data: &[u8], csv: Option<Dialect>, first: bool, profile: Profile) -> Result<EncodedBlock> {
    let crc = crc32fast::hash(data);
    let columnar = match (csv, profile) {
        (Some(d), Profile::Balanced | Profile::Max) => block::encode(data, d, first, profile)?,
        _ => None,
    };
    let (kind, codec_id, payload, records) = match columnar {
        Some(enc) => {
            let mut chosen = (KIND_CSV, CodecId::None, enc.payload, enc.records);
            if profile == Profile::Max {
                // Never lose to plain compression of the same bytes (zstd -19 is the
                // yardstick: lzma or brotli on a whole raw block would cost minutes).
                let (c, plain) = codec::compress_best(&[(CodecId::Zstd, 19)], data)?;
                if plain.len() < chosen.2.len() {
                    chosen = (KIND_RAW, c, plain, 0);
                }
            }
            chosen
        }
        None => {
            let candidates = match profile {
                Profile::Fast => vec![(CodecId::Zstd, 1)],
                _ => block::stream_codecs(profile),
            };
            let (c, bytes) = codec::compress_best(&candidates, data)?;
            (KIND_RAW, c, bytes, 0)
        }
    };
    let mut header = Vec::with_capacity(BLOCK_HEADER_LEN);
    BlockHeader {
        kind,
        codec: codec_id as u8,
        raw_len: data.len() as u64,
        payload_len: payload.len() as u64,
        crc32: crc,
        records,
    }
    .write(&mut header);
    Ok(EncodedBlock { header, payload, raw_len: data.len() as u64, kind, records })
}

fn decode_block(kind: u8, codec_id: u8, payload: &[u8], raw_len: u64, crc: u32) -> Result<Vec<u8>> {
    let raw_len = usize::try_from(raw_len).map_err(|_| ints::truncated())?;
    let data = match kind {
        KIND_RAW => codec::decompress(CodecId::from_u8(codec_id)?, payload, raw_len)?,
        KIND_CSV => block::decode(payload, raw_len)?,
        other => return Err(Error::Format(format!("unknown DCZ block kind {other}"))),
    };
    if crc32fast::hash(&data) != crc {
        return Err(Error::Integrity("DCZ block checksum mismatch".into()));
    }
    Ok(data)
}

/// Streaming writer: buffers text, cuts record-aligned blocks, encodes them in
/// parallel waves and writes them in order.  `finish` writes the footer.
pub struct DczWriter {
    out: Option<BufWriter<File>>,
    pending: Vec<u8>,
    opts: EncodeOptions,
    csv: Option<Dialect>,
    block_size: usize,
    wave: usize,
    offset: u64,
    raw_offset: u64,
    crc: crc32fast::Hasher,
    blocks: Vec<BlockInfo>,
    first: bool,
    pool: rayon::ThreadPool,
}

impl DczWriter {
    pub fn new(mut out: BufWriter<File>, opts: EncodeOptions) -> Result<Self> {
        out.write_all(&file_header())?;
        let plan = resources::plan(None, (opts.threads > 0).then_some(opts.threads), opts.memory, None);
        let block_size = opts.block_size.unwrap_or(match opts.profile {
            Profile::Fast => 8 << 20,
            Profile::Balanced => 8 << 20,
            Profile::Max => 16 << 20,
        });
        // Each block in flight costs about five times its size while encoding.
        let wave = ((plan.memory_budget as usize / 5) / block_size.max(1)).clamp(1, plan.threads.max(1) * 2);
        Ok(DczWriter {
            out: Some(out),
            pending: Vec::new(),
            csv: Some(Dialect::default()),
            block_size: block_size.max(4096),
            wave,
            offset: HEADER_LEN as u64,
            raw_offset: 0,
            crc: crc32fast::Hasher::new(),
            blocks: Vec::new(),
            first: true,
            pool: resources::thread_pool(plan.threads),
            opts,
        })
    }

    /// Treat input as opaque bytes (no columnar encoding).
    pub fn raw(mut self) -> Self {
        self.csv = None;
        self
    }

    pub fn with_dialect(mut self, d: Dialect) -> Self {
        self.csv = Some(d);
        self
    }

    fn cut_blocks(&mut self, all: bool) -> Vec<Vec<u8>> {
        let mut blocks = Vec::new();
        loop {
            if self.pending.is_empty() || (!all && self.pending.len() < self.block_size) {
                break;
            }
            let limit = self.pending.len().min(self.block_size);
            let cut = if limit == self.pending.len() && all {
                limit
            } else {
                match self.csv {
                    Some(d) => crate::chunk::last_record_end(&self.pending[..limit], &d, b'\n')
                        .or_else(|| memchr::memrchr(b'\n', &self.pending[..limit]).map(|i| i + 1))
                        .unwrap_or(limit),
                    None => limit,
                }
            };
            let rest = self.pending.split_off(cut);
            blocks.push(std::mem::replace(&mut self.pending, rest));
        }
        blocks
    }

    fn write_blocks(&mut self, blocks: &[Vec<u8>]) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }
        let first = self.first;
        let csv = self.csv;
        let profile = self.opts.profile;
        let encoded: Vec<EncodedBlock> = self.pool.install(|| {
            blocks
                .par_iter()
                .enumerate()
                .map(|(i, b)| encode_block(b, csv, first && i == 0, profile))
                .collect::<Result<_>>()
        })?;
        self.first = false;
        let out = self.out.as_mut().expect("writer open");
        for (b, raw) in encoded.into_iter().zip(blocks) {
            self.crc.update(raw);
            out.write_all(&b.header)?;
            out.write_all(&b.payload)?;
            self.blocks.push(BlockInfo {
                offset: self.offset,
                raw_offset: self.raw_offset,
                raw_len: b.raw_len,
                payload_len: b.payload.len() as u64,
                kind: b.kind,
                records: b.records,
            });
            self.offset += (b.header.len() + b.payload.len()) as u64;
            self.raw_offset += b.raw_len;
        }
        Ok(())
    }

    fn drain(&mut self, all: bool) -> Result<()> {
        let blocks = self.cut_blocks(all);
        for wave in blocks.chunks(self.wave.max(1)) {
            self.write_blocks(wave)?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<(u64, u64)> {
        self.drain(true)?;
        let footer = Footer {
            format: "dcz".into(),
            version: VERSION,
            created_by: format!("vegg {}", crate::VERSION),
            kind: if self.csv.is_some() { "csv".into() } else { "raw".into() },
            profile: format!("{:?}", self.opts.profile).to_lowercase(),
            original_size: self.raw_offset,
            original_crc32: self.crc.clone().finalize(),
            delimiter: self.csv.map(|d| (d.delimiter as char).to_string()),
            quote: self.csv.and_then(|d| d.quote).map(|q| (q as char).to_string()),
            blocks: self.blocks.clone(),
        };
        let json = serde_json::to_vec(&footer)?;
        let (codec_id, packed) = codec::compress_best(&[(CodecId::Zlib, 9)], &json)?;
        let mut out = self.out.take().expect("writer open");
        let footer_offset = self.offset;
        let mut header = Vec::with_capacity(BLOCK_HEADER_LEN);
        BlockHeader {
            kind: KIND_FOOTER,
            codec: codec_id as u8,
            raw_len: json.len() as u64,
            payload_len: packed.len() as u64,
            crc32: crc32fast::hash(&json),
            records: 0,
        }
        .write(&mut header);
        out.write_all(&header)?;
        out.write_all(&packed)?;
        out.write_all(&footer_offset.to_le_bytes())?;
        out.write_all(MAGIC_END)?;
        out.flush()?;
        let total = footer_offset + (header.len() + packed.len() + TRAILER_LEN) as u64;
        Ok((self.raw_offset, total))
    }
}

impl Write for DczWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.pending.extend_from_slice(buf);
        if self.pending.len() >= self.block_size * self.wave {
            self.drain(false).map_err(|e| std::io::Error::other(e.to_string()))?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl crate::tables::FinishWrite for DczWriter {
    fn finish_box(self: Box<Self>) -> std::io::Result<()> {
        (*self).finish().map(|_| ()).map_err(|e| std::io::Error::other(e.to_string()))
    }
}

/// Read the footer of a DCZ file and check that its block index describes the file.
pub fn read_footer(path: &Path) -> Result<Footer> {
    if path.is_dir() {
        return Err(Error::Invalid(format!("{} is a directory, not a file", path.display())));
    }
    let mut f = File::open(path).map_err(|e| Error::io(e, path))?;
    let len = f.metadata()?.len();
    if len < (HEADER_LEN + TRAILER_LEN) as u64 {
        return Err(Error::Format(format!("{} is not a DCZ file", path.display())));
    }
    let mut head = [0u8; HEADER_LEN];
    f.read_exact(&mut head)?;
    if &head[..8] != MAGIC {
        return Err(Error::Format(format!("{} is not a DCZ file", path.display())));
    }
    if head[8] != VERSION {
        return Err(Error::Format(format!("unsupported DCZ version {}", head[8])));
    }
    if head[9] & FLAG_PACKED != 0 {
        return Err(Error::Format(format!("{} holds packed values, not a file (use unpack)", path.display())));
    }
    f.seek(SeekFrom::End(-(TRAILER_LEN as i64)))?;
    let mut trailer = [0u8; TRAILER_LEN];
    f.read_exact(&mut trailer)?;
    if &trailer[8..] != MAGIC_END {
        return Err(Error::Integrity("DCZ trailer missing: the file is truncated".into()));
    }
    let footer_offset = u64::from_le_bytes(trailer[..8].try_into().unwrap());
    let footer_end = len - TRAILER_LEN as u64;
    if footer_offset < HEADER_LEN as u64 || footer_offset.saturating_add(BLOCK_HEADER_LEN as u64) > footer_end {
        return Err(Error::Integrity("DCZ footer is corrupt".into()));
    }
    f.seek(SeekFrom::Start(footer_offset))?;
    let mut bh = [0u8; BLOCK_HEADER_LEN];
    f.read_exact(&mut bh)?;
    let h = BlockHeader::parse(&bh);
    // the footer runs exactly to the trailer
    if h.kind != KIND_FOOTER || footer_offset + BLOCK_HEADER_LEN as u64 + h.payload_len.min(len) != footer_end {
        return Err(Error::Integrity("DCZ footer is corrupt".into()));
    }
    let mut payload = vec![0u8; h.payload_len as usize];
    f.read_exact(&mut payload)?;
    let raw_len = usize::try_from(h.raw_len).map_err(|_| ints::truncated())?;
    let json = codec::decompress(CodecId::from_u8(h.codec)?, &payload, raw_len)?;
    if crc32fast::hash(&json) != h.crc32 {
        return Err(Error::Integrity("DCZ footer checksum mismatch".into()));
    }
    let footer: Footer = serde_json::from_slice(&json)?;
    validate_index(&footer, footer_offset)?;
    Ok(footer)
}

/// The block index must tile the file exactly: blocks follow one another from the
/// header to the footer, and their decoded sizes add up to the original size.
/// (Block headers are not checksummed; matching them against this checksummed
/// index is what makes their lengths safe to allocate.)
fn validate_index(footer: &Footer, footer_offset: u64) -> Result<()> {
    let bad = || Error::Integrity("DCZ block index is corrupt".into());
    let (mut offset, mut raw) = (HEADER_LEN as u64, 0u64);
    for b in &footer.blocks {
        if b.offset != offset || b.raw_offset != raw {
            return Err(bad());
        }
        offset =
            offset.checked_add(BLOCK_HEADER_LEN as u64).and_then(|o| o.checked_add(b.payload_len)).ok_or_else(bad)?;
        raw = raw.checked_add(b.raw_len).ok_or_else(bad)?;
    }
    if offset != footer_offset || raw != footer.original_size {
        return Err(bad());
    }
    Ok(())
}

/// A block header must agree with its entry in the index.
fn check_header(h: &BlockHeader, b: &BlockInfo) -> Result<()> {
    if h.kind != b.kind || h.raw_len != b.raw_len || h.payload_len != b.payload_len {
        return Err(Error::Integrity("DCZ block header does not match the block index".into()));
    }
    Ok(())
}

/// Sequential streaming reader of the original bytes, verifying every block.
pub struct DczReader {
    inner: BufReader<File>,
    current: Vec<u8>,
    pos: usize,
    done: bool,
    crc: crc32fast::Hasher,
    footer: Footer,
    next: usize,
    produced: u64,
    consumed: Arc<AtomicU64>,
}

impl DczReader {
    pub fn open(path: &Path) -> Result<Self> {
        let footer = read_footer(path)?;
        let mut inner = BufReader::with_capacity(1 << 20, File::open(path).map_err(|e| Error::io(e, path))?);
        let mut head = [0u8; HEADER_LEN];
        inner.read_exact(&mut head)?;
        if &head[..8] != MAGIC {
            return Err(Error::Format(format!("{} is not a DCZ file", path.display())));
        }
        Ok(DczReader {
            inner,
            current: Vec::new(),
            pos: 0,
            done: false,
            crc: crc32fast::Hasher::new(),
            footer,
            next: 0,
            produced: 0,
            consumed: Arc::new(AtomicU64::new(HEADER_LEN as u64)),
        })
    }

    pub fn consumed_counter(&self) -> Arc<AtomicU64> {
        self.consumed.clone()
    }

    fn next_block(&mut self) -> Result<bool> {
        let mut bh = [0u8; BLOCK_HEADER_LEN];
        if let Err(e) = self.inner.read_exact(&mut bh) {
            return Err(if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::Integrity("DCZ file is truncated".into())
            } else {
                e.into()
            });
        }
        let h = BlockHeader::parse(&bh);
        if h.kind == KIND_FOOTER {
            if self.next != self.footer.blocks.len()
                || self.produced != self.footer.original_size
                || self.crc.clone().finalize() != self.footer.original_crc32
            {
                return Err(Error::Integrity("DCZ content checksum mismatch".into()));
            }
            return Ok(false);
        }
        let info = self.footer.blocks.get(self.next).ok_or_else(ints::truncated)?;
        check_header(&h, info)?;
        self.next += 1;
        let mut payload = vec![0u8; usize::try_from(h.payload_len).map_err(|_| ints::truncated())?];
        self.inner.read_exact(&mut payload).map_err(|_| Error::Integrity("DCZ file is truncated".into()))?;
        self.consumed.fetch_add((BLOCK_HEADER_LEN + payload.len()) as u64, Ordering::Relaxed);
        let data = decode_block(h.kind, h.codec, &payload, h.raw_len, h.crc32)?;
        self.crc.update(&data);
        self.produced += data.len() as u64;
        self.current = data;
        self.pos = 0;
        Ok(true)
    }
}

impl Read for DczReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.pos >= self.current.len() {
            if self.done {
                return Ok(0);
            }
            match self.next_block() {
                Ok(true) => {}
                Ok(false) => {
                    self.done = true;
                    return Ok(0);
                }
                Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
            }
        }
        let n = (self.current.len() - self.pos).min(buf.len());
        buf[..n].copy_from_slice(&self.current[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// The dialect and record terminator byte of delimited text, or None for other data.
fn looks_delimited(head: &[u8]) -> Option<(Dialect, u8)> {
    if head.is_empty() || head.contains(&0) {
        return None;
    }
    let d = sniff::sniff(head, head.len() < (1 << 20), &SniffOptions::default()).ok()?;
    if d.encoding != "utf-8" && d.encoding != "windows-1252" {
        return None;
    }
    // A single column is still worth columnar encoding (e.g. one number per line).
    Some((d.tokenizer_dialect(), d.split_byte()))
}

/// Compress a file into DCZ byte for byte: decompressing gives back exactly the
/// file, even one that is itself compressed (`x.csv.gz` stays gzip).  Delimited text
/// is encoded column-wise; anything else is stored as compressed raw blocks.
pub fn compress_file(
    src: &Path,
    dst: &Path,
    opts: &EncodeOptions,
    overwrite: bool,
    hooks: &dyn Hooks,
) -> Result<Report> {
    compress_input(src, io::open_raw(src)?, dst, opts, overwrite, hooks)
}

/// Compress the decompressed content of a (possibly compressed) delimited file:
/// converting `x.csv.gz` to DCZ stores the CSV.
pub fn compress_content(
    src: &Path,
    dst: &Path,
    opts: &EncodeOptions,
    overwrite: bool,
    hooks: &dyn Hooks,
) -> Result<Report> {
    compress_input(src, io::open_input(src)?, dst, opts, overwrite, hooks)
}

fn compress_input(
    src: &Path,
    input: io::Input,
    dst: &Path,
    opts: &EncodeOptions,
    overwrite: bool,
    hooks: &dyn Hooks,
) -> Result<Report> {
    let started = Instant::now();
    let size = input.file_size;
    let mut reader = input.reader;
    let head = io::read_up_to(&mut reader, 1 << 20)?;
    let dialect = looks_delimited(&head);
    let mut reader: Box<dyn Read + Send> = Box::new(std::io::Cursor::new(head).chain(reader));
    let input = io::Input { reader: Box::new(std::io::empty()), ..input };
    let mut out = AtomicFile::create(dst, overwrite)?;
    let mut writer = DczWriter::new(out.take_writer(), opts.clone())?;
    writer = match dialect {
        Some((d, _)) => writer.with_dialect(d),
        None => writer.raw(),
    };
    let consumed = input.consumed.clone();
    let block_size = writer.block_size;
    // Delimited text is cut at record boundaries (or anywhere once a block reaches
    // twice its size without one, e.g. text without line breaks); opaque bytes at
    // fixed sizes.  Blocks decode byte-exactly wherever they are cut.
    let mut chunker = dialect.map(|(d, split)| {
        Chunker::new(std::mem::replace(&mut reader, Box::new(std::io::empty())), block_size, d.quote, split)
            .with_dialect(d)
            .cut_at(block_size.saturating_mul(2))
    });
    loop {
        let mut wave = Vec::new();
        while wave.len() < writer.wave {
            let next = match chunker.as_mut() {
                Some(c) => c.next_chunk()?.map(|c| c.data),
                None => {
                    let buf = io::read_up_to(&mut reader, block_size)?;
                    (!buf.is_empty()).then_some(buf)
                }
            };
            match next {
                Some(data) => wave.push(data),
                None => break,
            }
        }
        if wave.is_empty() {
            break;
        }
        writer.write_blocks(&wave)?;
        if !hooks.progress(consumed.load(Ordering::Relaxed), input.file_size) {
            return Err(Error::Cancelled);
        }
    }
    let threads = writer.pool.current_num_threads();
    let profile = writer.opts.profile;
    let kind = if writer.csv.is_some() { "dcz/csv" } else { "dcz/raw" };
    writer.finish()?;
    let bytes_out = out.commit(None)?;
    Ok(Report {
        source: src.display().to_string(),
        destination: dst.display().to_string(),
        source_format: if dialect.is_some() { "csv".into() } else { "bytes".into() },
        destination_format: "dcz".into(),
        engine: "native",
        rows: 0,
        bytes_in: size,
        bytes_out,
        seconds: started.elapsed().as_secs_f64(),
        threads,
        compression: format!("{kind}/{}", format!("{profile:?}").to_lowercase()),
        ..Report::default()
    })
}

/// Decompress a DCZ file, decoding blocks in parallel and verifying every checksum.
pub fn decompress_file(
    src: &Path,
    dst: &Path,
    overwrite: bool,
    threads: usize,
    memory: Option<u64>,
    hooks: &dyn Hooks,
) -> Result<Report> {
    let started = Instant::now();
    let footer = read_footer(src)?;
    let plan = resources::plan(Some(footer.original_size), (threads > 0).then_some(threads), memory, None);
    let pool = resources::thread_pool(plan.threads);
    let mut out = AtomicFile::create(dst, overwrite)?;
    let mut f = File::open(src).map_err(|e| Error::io(e, src))?;
    let total = f.metadata()?.len();
    // a wave holds at least one block, however small the budget
    let wave_bytes = plan.memory_budget / 4;
    let mut crc = crc32fast::Hasher::new();
    let mut produced = 0u64;
    let mut i = 0;
    while i < footer.blocks.len() {
        let mut group = Vec::new();
        let mut bytes = 0u64;
        while i < footer.blocks.len()
            && (group.is_empty() || bytes + footer.blocks[i].raw_len <= wave_bytes)
            && group.len() < plan.threads * 2
        {
            let b = &footer.blocks[i];
            f.seek(SeekFrom::Start(b.offset))?;
            let mut bh = [0u8; BLOCK_HEADER_LEN];
            f.read_exact(&mut bh)?;
            let h = BlockHeader::parse(&bh);
            check_header(&h, b)?;
            let mut payload = vec![0u8; usize::try_from(h.payload_len).map_err(|_| ints::truncated())?];
            f.read_exact(&mut payload).map_err(|_| Error::Integrity("DCZ file is truncated".into()))?;
            bytes += h.raw_len;
            group.push((h, payload));
            i += 1;
        }
        let decoded: Vec<Vec<u8>> = pool.install(|| {
            group.par_iter().map(|(h, p)| decode_block(h.kind, h.codec, p, h.raw_len, h.crc32)).collect::<Result<_>>()
        })?;
        let w = out.writer();
        for d in decoded {
            crc.update(&d);
            produced += d.len() as u64;
            w.write_all(&d)?;
        }
        let done = footer.blocks.get(i).map_or(total, |b| b.offset);
        if !hooks.progress(done, total) {
            return Err(Error::Cancelled);
        }
    }
    if produced != footer.original_size || crc.finalize() != footer.original_crc32 {
        return Err(Error::Integrity("DCZ content checksum mismatch".into()));
    }
    let bytes_out = out.commit(None)?;
    Ok(Report {
        source: src.display().to_string(),
        destination: dst.display().to_string(),
        source_format: "dcz".into(),
        destination_format: if footer.kind == "csv" { "csv".into() } else { "bytes".into() },
        engine: "native",
        bytes_in: total,
        bytes_out,
        seconds: started.elapsed().as_secs_f64(),
        threads: plan.threads,
        compression: format!("dcz/{}/{}", footer.kind, footer.profile),
        ..Report::default()
    })
}

/// Compress an in-memory buffer (used by `pack` and tests).
pub fn compress_bytes(data: &[u8], csv: bool, profile: Profile) -> Result<Vec<u8>> {
    let mut out = file_header().to_vec();
    let block = encode_block(data, csv.then(Dialect::default), true, profile)?;
    out.extend_from_slice(&block.header);
    out.extend_from_slice(&block.payload);
    let footer = Footer {
        format: "dcz".into(),
        version: VERSION,
        created_by: format!("vegg {}", crate::VERSION),
        kind: if csv { "csv".into() } else { "raw".into() },
        profile: format!("{profile:?}").to_lowercase(),
        original_size: data.len() as u64,
        original_crc32: crc32fast::hash(data),
        delimiter: csv.then(|| ",".to_string()),
        quote: csv.then(|| "\"".to_string()),
        blocks: vec![BlockInfo {
            offset: HEADER_LEN as u64,
            raw_offset: 0,
            raw_len: data.len() as u64,
            payload_len: block.payload.len() as u64,
            kind: block.kind,
            records: block.records,
        }],
    };
    let json = serde_json::to_vec(&footer)?;
    let footer_offset = out.len() as u64;
    let mut h = Vec::new();
    BlockHeader {
        kind: KIND_FOOTER,
        codec: 0,
        raw_len: json.len() as u64,
        payload_len: json.len() as u64,
        crc32: crc32fast::hash(&json),
        records: 0,
    }
    .write(&mut h);
    out.extend_from_slice(&h);
    out.extend_from_slice(&json);
    out.extend_from_slice(&footer_offset.to_le_bytes());
    out.extend_from_slice(MAGIC_END);
    Ok(out)
}

/// Header flag: the file is a single packed-values frame instead of blocks + footer.
pub const FLAG_PACKED: u8 = 1;

/// A compact DCZ frame around one self-describing payload (used by `pack`):
/// `header(flags = FLAG_PACKED) kind:u8 payload_len:varint crc32:u32 payload`.
pub fn wrap_single_block(kind: u8, payload: &[u8], _meta: &[u8]) -> Result<Vec<u8>> {
    let mut out = file_header().to_vec();
    out[9] = FLAG_PACKED;
    out.push(kind);
    ints::put_varint(&mut out, payload.len() as u64);
    out.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

/// The verified payload of a packed frame, which must be of `kind`.
pub fn single_block_payload(buf: &[u8], kind: u8) -> Result<Vec<u8>> {
    if buf.len() < HEADER_LEN + 6 || &buf[..8] != MAGIC {
        return Err(Error::Format("not DCZ data".into()));
    }
    if buf[8] != VERSION {
        return Err(Error::Format(format!("unsupported DCZ version {}", buf[8])));
    }
    if buf[9] & FLAG_PACKED == 0 {
        return Err(Error::Format("DCZ data holds a file, not packed values (decompress it instead)".into()));
    }
    let mut c = ints::Cursor::new(&buf[HEADER_LEN..]);
    let found = c.u8()?;
    if found != kind {
        return Err(Error::Format(format!("expected packed data of kind {kind}, found {found}")));
    }
    let len = c.usize()?;
    let crc = u32::from_le_bytes(c.bytes(4)?.try_into().unwrap());
    let payload = c.bytes(len)?;
    if !c.is_empty() {
        return Err(Error::Integrity("trailing bytes after packed data".into()));
    }
    if crc32fast::hash(payload) != crc {
        return Err(Error::Integrity("packed data checksum mismatch".into()));
    }
    Ok(payload.to_vec())
}

/// Decompress an in-memory DCZ buffer, verifying the footer and every checksum.
pub fn decompress_bytes(buf: &[u8]) -> Result<Vec<u8>> {
    if buf.len() < HEADER_LEN + TRAILER_LEN || &buf[..8] != MAGIC {
        return Err(Error::Format("not DCZ data".into()));
    }
    if buf[8] != VERSION {
        return Err(Error::Format(format!("unsupported DCZ version {}", buf[8])));
    }
    if buf[9] & FLAG_PACKED != 0 {
        return Err(Error::Format("DCZ data holds packed values (use unpack)".into()));
    }
    let footer_end = buf.len() - TRAILER_LEN;
    let trailer = &buf[footer_end..];
    if &trailer[8..] != MAGIC_END {
        return Err(Error::Integrity("DCZ trailer missing: the data is truncated".into()));
    }
    let footer_offset =
        usize::try_from(u64::from_le_bytes(trailer[..8].try_into().unwrap())).map_err(|_| ints::truncated())?;
    let fstart =
        footer_offset.checked_add(BLOCK_HEADER_LEN).filter(|&s| s <= footer_end).ok_or_else(ints::truncated)?;
    let fh = BlockHeader::parse(buf[footer_offset..fstart].try_into().unwrap());
    // the footer runs exactly to the trailer
    if fh.kind != KIND_FOOTER || footer_offset < HEADER_LEN || fh.payload_len != (footer_end - fstart) as u64 {
        return Err(Error::Integrity("DCZ footer is corrupt".into()));
    }
    let raw_len = usize::try_from(fh.raw_len).map_err(|_| ints::truncated())?;
    let json = codec::decompress(CodecId::from_u8(fh.codec)?, &buf[fstart..footer_end], raw_len)?;
    if crc32fast::hash(&json) != fh.crc32 {
        return Err(Error::Integrity("DCZ footer is corrupt".into()));
    }
    let footer: Footer = serde_json::from_slice(&json)?;
    validate_index(&footer, footer_offset as u64)?;
    let mut out = codec::buffer(usize::try_from(footer.original_size).map_err(|_| ints::truncated())?)?;
    for b in &footer.blocks {
        // the validated index keeps every block inside the buffer
        let start = b.offset as usize + BLOCK_HEADER_LEN;
        let h = BlockHeader::parse(buf[b.offset as usize..start].try_into().unwrap());
        check_header(&h, b)?;
        let payload = &buf[start..start + b.payload_len as usize];
        out.extend_from_slice(&decode_block(h.kind, h.codec, payload, h.raw_len, h.crc32)?);
    }
    if out.len() as u64 != footer.original_size || crc32fast::hash(&out) != footer.original_crc32 {
        return Err(Error::Integrity("DCZ content checksum mismatch".into()));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convert::NoHooks;

    /// A directory for one test, removed when the test passes.
    struct TestDir(std::path::PathBuf);

    impl TestDir {
        fn new(name: &str) -> TestDir {
            let dir = std::env::temp_dir().join(format!("vegg-dcz-{}-{name}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            TestDir(dir)
        }

        fn tmp(&self, name: &str) -> std::path::PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            if !std::thread::panicking() {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }

    #[test]
    fn file_roundtrip_all_profiles() {
        let mut csv = String::from("id,amount,city,ts\n");
        for i in 0..120_000 {
            csv.push_str(&format!(
                "{},{}.{:02},{},2024-01-01T{:02}:{:02}:00\n",
                i,
                i % 977,
                i % 100,
                ["Paris", "Pune", "\"Rome, IT\""][i % 3],
                i / 60 % 24,
                i % 60
            ));
        }
        let dir = TestDir::new("roundtrip");
        let src = dir.tmp("in.csv");
        std::fs::write(&src, &csv).unwrap();
        for profile in [Profile::Fast, Profile::Balanced, Profile::Max] {
            let dst = dir.tmp(&format!("out-{profile:?}.dcz"));
            let back = dir.tmp(&format!("back-{profile:?}.csv"));
            let opts = EncodeOptions { profile, threads: 4, block_size: Some(256 << 10), memory: None };
            let r = compress_file(&src, &dst, &opts, true, &NoHooks).unwrap();
            assert!(r.bytes_out < r.bytes_in / 4, "{profile:?}: {} -> {}", r.bytes_in, r.bytes_out);
            decompress_file(&dst, &back, true, 4, None, &NoHooks).unwrap();
            assert_eq!(std::fs::read(&back).unwrap(), csv.as_bytes());
            let mut streamed = Vec::new();
            DczReader::open(&dst).unwrap().read_to_end(&mut streamed).unwrap();
            assert_eq!(streamed, csv.as_bytes());
            let footer = read_footer(&dst).unwrap();
            assert_eq!(footer.original_size, csv.len() as u64);
            assert!(footer.blocks.len() > 1);
        }
    }

    #[test]
    fn detects_corruption() {
        let data = b"a,b\n1,2\n3,4\n".repeat(1000);
        let packed = compress_bytes(&data, true, Profile::Balanced).unwrap();
        assert_eq!(decompress_bytes(&packed).unwrap(), data);
        let mut bad = packed.clone();
        let mid = HEADER_LEN + BLOCK_HEADER_LEN + 3;
        bad[mid] ^= 0x55;
        assert!(decompress_bytes(&bad).is_err());
        assert!(decompress_bytes(&packed[..packed.len() / 2]).is_err());
    }

    #[test]
    fn raw_binary_files() {
        let data: Vec<u8> = (0..300_000u32).flat_map(|i| (i.wrapping_mul(2654435761) >> 7).to_le_bytes()).collect();
        let dir = TestDir::new("raw");
        let src = dir.tmp("blob.bin");
        std::fs::write(&src, &data).unwrap();
        let dst = dir.tmp("blob.dcz");
        let back = dir.tmp("blob.back");
        compress_file(
            &src,
            &dst,
            &EncodeOptions { profile: Profile::Balanced, threads: 2, block_size: Some(100_000), memory: None },
            true,
            &NoHooks,
        )
        .unwrap();
        decompress_file(&dst, &back, true, 2, None, &NoHooks).unwrap();
        assert_eq!(std::fs::read(&back).unwrap(), data);
        assert_eq!(read_footer(&dst).unwrap().kind, "raw");
    }
}
