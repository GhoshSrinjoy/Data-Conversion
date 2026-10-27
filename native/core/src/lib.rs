//! dataconv-core: the native engine behind dataconv.
//!
//! * [`tokenize`] / [`chunk`]: a quote-aware CSV tokenizer and record-aligned chunker.
//! * [`types`]: lossless type inference (leading zeros, big integers and float
//!   precision are never silently lost).
//! * [`convert`]: parallel, memory-budgeted conversion between CSV, Parquet, Arrow IPC
//!   and JSON Lines.
//! * [`dcz`]: the DCZ container, a byte-exact columnar compressor for delimited text.

pub mod chunk;
pub mod codec;
pub mod convert;
pub mod csvout;
pub mod dcz;
pub mod decode;
pub mod error;
pub mod inspect;
pub mod io;
pub mod pack;
pub mod resources;
pub mod sniff;
pub mod tables;
pub mod tokenize;
pub mod types;

pub use error::{Error, Result};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
