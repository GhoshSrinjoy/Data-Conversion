//! Resource detection and execution planning.
//!
//! Thread counts come from `available_parallelism`, which honours CPU affinity and
//! cgroup quotas.  Memory comes from the OS, capped by any cgroup limit, so the
//! engine sizes itself to the container it runs in, not to the host machine.

use serde::Serialize;

pub const KIB: u64 = 1 << 10;
pub const MIB: u64 = 1 << 20;
pub const GIB: u64 = 1 << 30;

#[derive(Clone, Debug, Serialize)]
pub struct SystemInfo {
    pub os: &'static str,
    pub arch: &'static str,
    pub cpu_logical: usize,
    pub cpu_usable: usize,
    pub mem_total: u64,
    pub mem_available: u64,
    pub cgroup_limit: Option<u64>,
    pub pointer_width: u32,
}

pub fn detect() -> SystemInfo {
    let cpu_usable = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let cpu_logical = num_logical().max(cpu_usable);
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let mut total = sys.total_memory();
    let mut available = sys.available_memory();
    let mut cgroup_limit = None;
    if let Some(limits) = sys.cgroup_limits() {
        if limits.total_memory > 0 && (total == 0 || limits.total_memory < total) {
            cgroup_limit = Some(limits.total_memory);
            total = limits.total_memory;
            available = available.min(limits.free_memory);
        }
    }
    if total == 0 {
        // Unknown platform: assume a modest machine rather than an unlimited one.
        total = 4 * GIB;
        available = GIB;
    }
    if available == 0 || available > total {
        available = total / 2;
    }
    let pointer_width = usize::BITS;
    if pointer_width < 64 {
        // A 32-bit process cannot address more than a few GiB regardless of RAM.
        total = total.min(3 * GIB);
        available = available.min(3 * GIB / 2);
    }
    SystemInfo {
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        cpu_logical,
        cpu_usable,
        mem_total: total,
        mem_available: available,
        cgroup_limit,
        pointer_width,
    }
}

fn num_logical() -> usize {
    let mut sys = sysinfo::System::new();
    sys.refresh_cpu_list(sysinfo::CpuRefreshKind::nothing());
    sys.cpus().len()
}

/// How a job will use the machine.
#[derive(Clone, Debug, Serialize)]
pub struct Plan {
    pub threads: usize,
    pub memory_budget: u64,
    /// Bytes of input per parallel work unit (record-aligned chunk / DCZ block).
    pub chunk_size: usize,
    /// Chunks processed concurrently per wave; bounds peak memory.
    pub wave_chunks: usize,
    pub row_group_rows: usize,
    pub low_memory: bool,
}

/// Estimated peak bytes held in memory per input byte in flight: the raw chunk, its
/// decoded columns, and the next wave being prefetched.
const EXPANSION: f64 = 4.5;

pub fn plan(input_size: Option<u64>, threads: Option<usize>, memory: Option<u64>, chunk: Option<usize>) -> Plan {
    let info = detect();
    plan_with(&info, input_size, threads, memory, chunk)
}

pub fn plan_with(
    info: &SystemInfo,
    input_size: Option<u64>,
    threads: Option<usize>,
    memory: Option<u64>,
    chunk: Option<usize>,
) -> Plan {
    let threads = threads.filter(|&t| t > 0).unwrap_or(info.cpu_usable).max(1);
    let budget =
        memory.filter(|&m| m > 0).unwrap_or_else(|| (info.mem_available / 2).min(info.mem_total * 4 / 5)).max(64 * MIB);
    let wave_bytes = ((budget as f64) / EXPANSION) as u64;
    // User-supplied chunk sizes are clamped: a typo must not become a 256 GB allocation.
    let mut chunk_size = chunk.map(|c| (c as u64).clamp(4 * KIB, 2 * GIB)).unwrap_or_else(|| {
        let per_thread = wave_bytes / threads as u64;
        per_thread.clamp(MIB, 64 * MIB)
    });
    if let Some(size) = input_size {
        if chunk.is_none() && size > 0 {
            // Small inputs: split evenly so every thread has work, but keep chunks
            // large enough that per-chunk overhead stays negligible.
            let even = size.div_ceil(threads as u64);
            chunk_size = chunk_size.min(even.max(256 * KIB));
        }
    }
    let chunk_size = chunk_size.max(4 * KIB) as usize;
    let wave_chunks = ((wave_bytes / chunk_size as u64) as usize).clamp(1, threads.max(1) * 2);
    let low_memory = budget < 512 * MIB;
    Plan {
        threads,
        memory_budget: budget,
        chunk_size,
        wave_chunks,
        row_group_rows: if low_memory { 128 * 1024 } else { 1024 * 1024 },
        low_memory,
    }
}

/// A rayon pool sized by the plan (the global pool is left alone so embedding
/// applications keep their own configuration).
pub fn thread_pool(threads: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads.max(1))
        .thread_name(|i| format!("vegg-{i}"))
        .build()
        .expect("failed to start worker threads")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake(cpus: usize, avail: u64) -> SystemInfo {
        SystemInfo {
            os: "test",
            arch: "test",
            cpu_logical: cpus,
            cpu_usable: cpus,
            mem_total: avail * 2,
            mem_available: avail,
            cgroup_limit: None,
            pointer_width: 64,
        }
    }

    #[test]
    fn detect_is_sane() {
        let info = detect();
        assert!(info.cpu_usable >= 1);
        assert!(info.mem_total >= info.mem_available);
        assert!(info.mem_available > 0);
    }

    #[test]
    fn plan_scales_with_resources() {
        let big = plan_with(&fake(32, 64 * GIB), Some(100 * GIB), None, None, None);
        assert_eq!(big.threads, 32);
        assert_eq!(big.chunk_size as u64, 64 * MIB);
        let tiny = plan_with(&fake(1, 128 * MIB), Some(10 * GIB), None, None, None);
        assert_eq!(tiny.threads, 1);
        assert!(tiny.low_memory);
        assert!((tiny.chunk_size as u64) <= 64 * MIB);
        assert!(tiny.wave_chunks >= 1);
        // the whole wave must fit the budget
        assert!((tiny.chunk_size * tiny.wave_chunks) as u64 <= tiny.memory_budget);
    }

    #[test]
    fn small_inputs_spread_across_threads() {
        let p = plan_with(&fake(8, 16 * GIB), Some(8 * MIB), None, None, None);
        assert!(p.chunk_size as u64 <= MIB);
        let p = plan_with(&fake(8, 16 * GIB), Some(10), None, None, None);
        assert_eq!(p.chunk_size as u64, 256 * KIB);
    }

    #[test]
    fn overrides_win() {
        let p = plan_with(&fake(8, 16 * GIB), None, Some(3), Some(GIB), Some(2 * 1024 * 1024));
        assert_eq!(p.threads, 3);
        assert_eq!(p.memory_budget, GIB);
        assert_eq!(p.chunk_size, 2 * 1024 * 1024);
    }
}
