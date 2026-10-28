//! Record-aligned chunks of a plain UTF-8 file, read without a sequential copy.
//!
//! The general [`Chunker`] copies its input into a buffer and scans every byte for
//! quotes on one thread, which held a whole conversion to a few GB/s.  When the input
//! is a plain file whose records end at line feeds, reader threads instead read blocks
//! straight into the buffers the chunks will live in, each at its offset, and note
//! whether a block holds a quote or escape byte and where its last line feed is.  For
//! a block without such bytes every line feed ends a record, so its chunk is the
//! partial record left over from the block before (copied into room kept free at the
//! front of the buffer) and the block up to its last line feed: the sequential path
//! copies and scans nothing else.  The first block that holds a quote or escape, or a
//! record longer than a block, hands the rest of the file to a [`Chunker`], which
//! applies the full quoting rules from the start of the chunk it was at.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;

use crate::chunk::{Chunk, Chunker};
use crate::error::{Error, Result};
use crate::tokenize::Dialect;

/// Free space at the front of every block for the partial record carried over.
const ROOM: usize = 256 << 10;
const READERS: usize = 4;

/// A plain UTF-8 file whose data records start at `start` (after a BOM and the header).
pub struct PlainFile {
    pub path: PathBuf,
    pub start: u64,
    /// Bytes per block, about one chunk.
    pub block: usize,
    pub dialect: Dialect,
    /// File bytes taken so far, for progress.
    pub count: Arc<AtomicU64>,
}

/// One block as a reader thread left it: `buf[ROOM..ROOM + len]` holds the bytes.
struct Block {
    buf: Vec<u8>,
    len: usize,
    /// A quote or escape byte occurs in the block.
    special: bool,
    /// Offset of the block's last line feed (from the start of its bytes).
    last_lf: Option<usize>,
}

/// Chunks of `file` in order, at most `depth` waiting, read on threads of their own.
pub fn spawn(file: PlainFile, depth: usize) -> Receiver<Result<Chunk>> {
    let (tx, rx) = sync_channel::<Result<Chunk>>(depth.max(1));
    std::thread::Builder::new()
        .name("vegg-reader".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| produce(&file, &tx)));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    let _ = tx.send(Err(e));
                }
                Err(_) => {
                    let _ = tx.send(Err(Error::Data("internal error while reading the input".into())));
                }
            }
        })
        .expect("failed to start reader thread");
    rx
}

fn start_readers(file: &PlainFile) -> Result<Vec<Receiver<std::io::Result<Block>>>> {
    let special: Vec<u8> = file.dialect.quote.into_iter().chain(file.dialect.escape).collect();
    let mut lanes = Vec::with_capacity(READERS);
    for first in 0..READERS {
        let (tx, rx) = sync_channel::<std::io::Result<Block>>(2);
        // a handle per thread: Windows serialises reads through a shared one
        let handle = File::open(&file.path).map_err(|e| Error::io(e, &file.path))?;
        let (start, size, special) = (file.start, file.block, special.clone());
        std::thread::Builder::new()
            .name("vegg-read-block".into())
            .spawn(move || {
                let mut index = first;
                loop {
                    let mut buf = vec![0u8; ROOM + size];
                    let read = read_at(&handle, &mut buf[ROOM..], start + (index * size) as u64);
                    let block = read.map(|len| {
                        let bytes = &buf[ROOM..ROOM + len];
                        let special = match special[..] {
                            [] => false,
                            [a] => memchr::memchr(a, bytes).is_some(),
                            [a, b, ..] => memchr::memchr2(a, b, bytes).is_some(),
                        };
                        let last_lf = memchr::memrchr(b'\n', bytes);
                        Block { buf, len, special, last_lf }
                    });
                    let last = !matches!(&block, Ok(b) if b.len == size);
                    if tx.send(block).is_err() || last {
                        return;
                    }
                    index += READERS;
                }
            })
            .map_err(|e| Error::io(e, &file.path))?;
        lanes.push(rx);
    }
    Ok(lanes)
}

/// Fill `buf` from `offset` on, short only at the end of the file.
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    #[cfg(unix)]
    use std::os::unix::fs::FileExt;
    #[cfg(windows)]
    use std::os::windows::fs::FileExt;
    let mut filled = 0;
    while filled < buf.len() {
        let at = offset + filled as u64;
        #[cfg(unix)]
        let got = file.read_at(&mut buf[filled..], at);
        #[cfg(windows)]
        let got = file.seek_read(&mut buf[filled..], at);
        match got {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

fn produce(file: &PlainFile, tx: &SyncSender<Result<Chunk>>) -> Result<()> {
    let lanes = start_readers(file)?;
    // the partial record at the end of the blocks taken so far
    let mut tail: Vec<u8> = Vec::new();
    let (mut offset, mut index) = (0u64, 0usize);
    for k in 0.. {
        let block = match lanes[k % lanes.len()].recv() {
            Ok(block) => block.map_err(|e| Error::io(e, &file.path))?,
            Err(_) => return Err(Error::Data("a reader thread stopped".into())),
        };
        file.count.fetch_add(block.len as u64, Ordering::Relaxed);
        let last = block.len < file.block;
        if block.special || (block.last_lf.is_none() && !last) {
            // the quoting rules decide from here on (or a record outgrew the block)
            let mut head = tail;
            head.extend_from_slice(&block.buf[ROOM..ROOM + block.len]);
            let rest = file.start + (k * file.block + block.len) as u64;
            return hand_over(file, head, rest, offset, index, tx);
        }
        // a block without quotes: every line feed in it ends a record
        let cut = if last { block.len } else { block.last_lf.map_or(block.len, |lf| lf + 1) };
        if cut + tail.len() > 0 {
            let next_tail = block.buf[ROOM + cut..ROOM + block.len].to_vec();
            let (data, start) = if tail.len() <= ROOM {
                let mut buf = block.buf;
                let start = ROOM - tail.len();
                buf[start..ROOM].copy_from_slice(&tail);
                buf.truncate(ROOM + cut);
                (buf, start)
            } else {
                // a record longer than the room before the block: copy it all
                let mut data = std::mem::take(&mut tail);
                data.extend_from_slice(&block.buf[ROOM..ROOM + cut]);
                (data, 0)
            };
            let len = (data.len() - start) as u64;
            if tx.send(Ok(Chunk { data, start, offset, index, forced: false })).is_err() {
                return Ok(());
            }
            offset += len;
            index += 1;
            tail = next_tail;
        }
        if last {
            return Ok(());
        }
    }
    Ok(())
}

/// Continue with the general chunker: `head` holds the input from the current chunk's
/// start to `rest`, where the file goes on.
fn hand_over(
    file: &PlainFile,
    head: Vec<u8>,
    rest: u64,
    offset: u64,
    index: usize,
    tx: &SyncSender<Result<Chunk>>,
) -> Result<()> {
    let mut f = File::open(&file.path).map_err(|e| Error::io(e, &file.path))?;
    f.seek(SeekFrom::Start(rest)).map_err(|e| Error::io(e, &file.path))?;
    let reader = crate::io::CountingReader::new(std::io::BufReader::with_capacity(1 << 20, f), file.count.clone());
    let reader = std::io::Cursor::new(head).chain(reader);
    let d = file.dialect;
    let mut chunker = Chunker::new(reader, file.block, d.quote, b'\n').with_dialect(d).with_position(offset, index);
    while let Some(chunk) = chunker.next_chunk()? {
        if tx.send(Ok(chunk)).is_err() {
            break;
        }
    }
    Ok(())
}

/// Where the data records of `path` start: past a UTF-8 BOM and the `leading` records
/// (preamble and header), split as [`crate::convert`] splits them.
pub fn data_start(path: &Path, d: Dialect, leading: usize) -> Result<u64> {
    let mut f = File::open(path).map_err(|e| Error::io(e, path))?;
    let head = crate::io::read_up_to(&mut f, 3).map_err(|e| Error::io(e, path))?;
    let bom = if head == b"\xEF\xBB\xBF" { 3 } else { 0 };
    f.seek(SeekFrom::Start(bom)).map_err(|e| Error::io(e, path))?;
    let count = Arc::new(AtomicU64::new(0));
    let reader: Box<dyn Read + Send> = Box::new(crate::io::CountingReader::new(f, count.clone()));
    let (_, prefix) = crate::convert::skip_leading(reader, d, b'\n', leading)?;
    Ok(bom + count.load(Ordering::Relaxed) - prefix.len() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The chunks `spawn` makes of `data`, as (start offset, bytes).
    fn chunks(data: &[u8], block: usize, d: Dialect) -> Vec<(u64, Vec<u8>)> {
        let dir = std::env::temp_dir().join(format!("vegg-blocks-{}-{block}-{}", std::process::id(), data.len()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("in.csv");
        std::fs::write(&path, data).unwrap();
        let count = Arc::new(AtomicU64::new(0));
        let file = PlainFile { path: path.clone(), start: 0, block, dialect: d, count: count.clone() };
        let out: Vec<(u64, Vec<u8>)> =
            spawn(file, 4).into_iter().map(|c| c.unwrap()).map(|c| (c.offset, c.bytes().to_vec())).collect();
        assert_eq!(count.load(Ordering::Relaxed), data.len() as u64);
        std::fs::remove_dir_all(&dir).unwrap();
        out
    }

    /// Records split at every line feed, as the general chunker splits plain lines.
    fn check(data: &[u8], block: usize, d: Dialect) {
        let got = chunks(data, block, d);
        let mut joined = Vec::new();
        for (i, (offset, bytes)) in got.iter().enumerate() {
            assert_eq!(*offset, joined.len() as u64, "chunk {i} offset");
            joined.extend_from_slice(bytes);
            if i + 1 < got.len() {
                assert_eq!(bytes.last(), Some(&b'\n'), "chunk {i} ends inside a record");
            }
        }
        assert_eq!(joined, data);
        // the same records as the general chunker finds
        let tokens = |chunks: Vec<Vec<u8>>| -> Vec<Vec<u8>> {
            let mut records = Vec::new();
            for c in chunks {
                crate::tokenize::Tokenizer::new(d).for_each(&c, |r| {
                    records.push(r.raw().to_vec());
                    true
                });
            }
            records
        };
        let mut general = Chunker::new(data, block, d.quote, b'\n').with_dialect(d);
        let mut reference = Vec::new();
        while let Some(c) = general.next_chunk().unwrap() {
            reference.push(c.data);
        }
        assert_eq!(tokens(got.into_iter().map(|(_, b)| b).collect()), tokens(reference));
    }

    fn lines(n: usize, width: usize) -> Vec<u8> {
        let mut s = Vec::new();
        for i in 0..n {
            s.extend_from_slice(format!("{i},{}\n", "x".repeat(i * 7 % width)).as_bytes());
        }
        s
    }

    #[test]
    fn plain_lines_split_at_line_feeds() {
        let d = Dialect::default();
        for block in [64, 100, 1000, 4096] {
            let data = lines(3000, 90);
            check(&data, block, d);
            // no final line feed, and a file of exactly whole blocks
            check(&data[..data.len() - 1], block, d);
            check(&data[..block * 7], block, d);
        }
        check(b"", 64, d);
        check(b"a\n", 64, d);
    }

    #[test]
    fn quotes_hand_over_to_the_general_chunker() {
        let d = Dialect::default();
        let mut data = lines(2000, 50);
        data.extend_from_slice(b"7,\"a quoted\nfield\",x\n");
        data.extend_from_slice(&lines(500, 50));
        data.extend_from_slice(b"8,55\" screen,x\n9,\"never closed\n");
        data.extend_from_slice(&lines(500, 50));
        for block in [64, 333, 4096] {
            check(&data, block, d);
        }
    }

    #[test]
    fn long_records_still_split_exactly() {
        let d = Dialect::default();
        let mut data = lines(100, 30);
        data.extend_from_slice(format!("1,{}\n", "y".repeat(5000)).as_bytes());
        data.extend_from_slice(&lines(100, 30));
        // longer than a block: handed over; longer than the room: copied
        check(&data, 1000, d);
        data.extend_from_slice(format!("2,{}\n", "z".repeat(ROOM + 10)).as_bytes());
        data.extend_from_slice(&lines(100, 30));
        check(&data, ROOM * 2, d);
    }

    #[test]
    fn data_start_skips_bom_and_header() {
        let dir = std::env::temp_dir().join(format!("vegg-blocks-start-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("h.csv");
        let d = Dialect::default();
        std::fs::write(&path, b"\xEF\xBB\xBFname,\"quoted\nheader\"\n1,2\n").unwrap();
        assert_eq!(data_start(&path, d, 1).unwrap(), 24);
        assert_eq!(data_start(&path, d, 0).unwrap(), 3);
        std::fs::write(&path, b"a,b\n1,2\n").unwrap();
        assert_eq!(data_start(&path, d, 1).unwrap(), 4);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
