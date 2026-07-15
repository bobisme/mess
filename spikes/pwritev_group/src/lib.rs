//! Isolated positioned-write kernels for the bn-1zv6 decision spike.
//!
//! This crate is deliberately outside the production workspace. It owns no
//! log semantics: callers hand it complete, adjacent canonical batch buffers.
//! The module exists to make short-write/iovec cursor behavior deterministic
//! and exhaustively testable before any real-file measurement is trusted.

use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, IoSlice};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::FileExt;
use std::sync::OnceLock;

use mess_log::encode::{BatchEncoder, BatchInput, EncodeError, Subframe};
use mess_log::format::{
    HEADER_LEN, MARKER_LEN, MAX_BATCH_LEN, SUBFRAME_HDR_LEN,
};

/// Per-operation counters emitted alongside every measurement row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteStats {
    pub syscalls:         u64,
    pub bytes_written:    u64,
    pub copied_bytes:     u64,
    pub short_writes:     u64,
    pub interrupted:      u64,
    pub max_iovecs:       usize,
    pub completed_bufs:   usize,
    pub partial_buf_byte: usize,
}

/// Terminal positioned-I/O failure with the exact completed batch prefix.
#[derive(Debug)]
pub struct WriteFailure {
    pub source: io::Error,
    pub stats:  WriteStats,
}

impl std::fmt::Display for WriteFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "positioned write failed after {} bytes ({} complete buffers, {} \
             bytes into next): {}",
            self.stats.bytes_written,
            self.stats.completed_bufs,
            self.stats.partial_buf_byte,
            self.source,
        )
    }
}

impl std::error::Error for WriteFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Minimal syscall seam. Fault tests script this; measurements use
/// [`RealSink`]. `bufs` is nonempty and contains no empty slice.
pub trait VectoredSink {
    fn pwritev(
        &mut self,
        offset: u64,
        bufs: &[IoSlice<'_>],
    ) -> io::Result<usize>;
}

trait PositionedSink {
    fn pwrite(&mut self, offset: u64, buf: &[u8]) -> io::Result<usize>;
}

impl PositionedSink for &File {
    fn pwrite(&mut self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        self.write_at(buf, offset)
    }
}

/// Minimal durability seam shared by the production-shaped driver and the
/// deterministic fault report. It deliberately makes exactly one call.
pub trait DataSync {
    fn sync_data_once(&self) -> io::Result<()>;
}

impl DataSync for File {
    fn sync_data_once(&self) -> io::Result<()> { self.sync_data() }
}

pub fn fdatasync_once<S: DataSync + ?Sized>(sink: &S) -> io::Result<()> {
    sink.sync_data_once()
}

/// Real Linux positioned I/O, matching `RealFile::pwrite`'s file descriptor
/// and offset semantics while exposing `pwritev(2)` only to this spike.
pub struct RealSink {
    file: File,
}

impl RealSink {
    pub fn new(file: File) -> Self { Self { file } }

    pub fn file(&self) -> &File { &self.file }

    pub fn into_file(self) -> File { self.file }
}

impl VectoredSink for RealSink {
    fn pwritev(
        &mut self,
        offset: u64,
        bufs: &[IoSlice<'_>],
    ) -> io::Result<usize> {
        let offset = i64::try_from(offset).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "offset exceeds off_t")
        })?;
        let count = i32::try_from(bufs.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "iov count exceeds c_int",
            )
        })?;
        // IoSlice is ABI-compatible with iovec on Unix. Each slice stays live
        // for this call and the owned File keeps the fd valid.
        let rc = unsafe {
            libc::pwritev(
                self.file.as_raw_fd(),
                bufs.as_ptr().cast::<libc::iovec>(),
                count,
                offset,
            )
        };
        if rc < 0 { Err(io::Error::last_os_error()) } else { Ok(rc as usize) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cursor {
    buf:   usize,
    intra: usize,
}

impl Cursor {
    fn advance(&mut self, buffers: &[&[u8]], mut n: usize) {
        while n != 0 {
            let remaining = buffers[self.buf].len() - self.intra;
            if n < remaining {
                self.intra += n;
                return;
            }
            n -= remaining;
            self.buf += 1;
            self.intra = 0;
        }
    }

    fn stats(self, mut stats: WriteStats) -> WriteStats {
        stats.completed_bufs = self.buf;
        stats.partial_buf_byte = self.intra;
        stats
    }
}

fn invalid(message: &'static str, stats: WriteStats) -> WriteFailure {
    WriteFailure {
        source: io::Error::new(io::ErrorKind::InvalidInput, message),
        stats,
    }
}

/// Runtime iovec ceiling. Linux reports 1024 on the reference host. A
/// positive conservative fallback is used if `sysconf` is unavailable.
pub fn runtime_iov_max() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        let n = unsafe { libc::sysconf(libc::_SC_IOV_MAX) };
        if n > 0 { n as usize } else { 1024 }
    })
}

/// One maximal same-file run. A gathered write may operate only within this
/// range; `roll_before` means production must sync/allocate/header-sync before
/// its first batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentPartition {
    pub start:       usize,
    pub end:         usize,
    pub roll_before: bool,
}

/// Pure production roll planner used by the fault/eligibility oracle.
///
/// It never splits a physical batch and never admits a batch that cannot fit
/// in an empty segment. Callers execute one gathered strategy per returned
/// partition, with lifecycle barriers around each `roll_before` boundary.
pub fn segment_partitions(
    batch_lengths: &[u64],
    mut remaining: u64,
    empty_capacity: u64,
) -> Result<Vec<SegmentPartition>, usize> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut roll_before = false;
    for (index, &len) in batch_lengths.iter().enumerate() {
        if len == 0 || len > empty_capacity {
            return Err(index);
        }
        if len > remaining {
            if start != index {
                out.push(SegmentPartition { start, end: index, roll_before });
            }
            start = index;
            roll_before = true;
            remaining = empty_capacity;
        }
        remaining -= len;
    }
    if start != batch_lengths.len() {
        out.push(SegmentPartition {
            start,
            end: batch_lengths.len(),
            roll_before,
        });
    }
    Ok(out)
}

/// Write adjacent buffers with bounded `pwritev` calls.
///
/// `iov_scratch` is retained by the harness between groups so constructing
/// syscall arguments does not introduce a per-group allocation. Empty input
/// slices are ignored; production prepared batches are never empty.
pub fn pwritev_all<'a, S: VectoredSink>(
    sink: &mut S,
    offset: u64,
    buffers: &[&'a [u8]],
    configured_iov_cap: usize,
    byte_cap: usize,
    iov_scratch: &mut Vec<IoSlice<'a>>,
) -> Result<WriteStats, WriteFailure> {
    pwritev_all_with_runtime_cap(
        sink,
        offset,
        buffers,
        configured_iov_cap,
        byte_cap,
        runtime_iov_max(),
        iov_scratch,
    )
}

fn pwritev_all_with_runtime_cap<'a, S: VectoredSink>(
    sink: &mut S,
    offset: u64,
    buffers: &[&'a [u8]],
    configured_iov_cap: usize,
    byte_cap: usize,
    runtime_cap: usize,
    iov_scratch: &mut Vec<IoSlice<'a>>,
) -> Result<WriteStats, WriteFailure> {
    let iov_cap = configured_iov_cap.min(runtime_cap);
    if iov_cap == 0 {
        return Err(invalid("iov cap must be positive", WriteStats::default()));
    }
    if byte_cap == 0 || byte_cap > isize::MAX as usize {
        return Err(invalid(
            "byte cap must be in 1..=isize::MAX",
            WriteStats::default(),
        ));
    }
    if offset > i64::MAX as u64 {
        return Err(invalid("offset exceeds off_t", WriteStats::default()));
    }

    if buffers.iter().any(|buffer| buffer.is_empty()) {
        return Err(invalid(
            "physical batch buffers must be nonempty",
            WriteStats::default(),
        ));
    }
    let mut cursor = Cursor { buf: 0, intra: 0 };
    let mut current_offset = offset;
    let mut stats = WriteStats::default();
    iov_scratch.clear();
    if iov_scratch.capacity() < iov_cap {
        iov_scratch.reserve(iov_cap - iov_scratch.capacity());
    }

    while cursor.buf < buffers.len() {
        iov_scratch.clear();
        let mut offered = 0usize;
        let mut index = cursor.buf;
        let mut intra = cursor.intra;
        while index < buffers.len()
            && iov_scratch.len() < iov_cap
            && offered < byte_cap
        {
            let available = &buffers[index][intra..];
            let take = available.len().min(byte_cap - offered);
            iov_scratch.push(IoSlice::new(&available[..take]));
            offered += take;
            if take != available.len() {
                break;
            }
            index += 1;
            intra = 0;
        }
        if offered == 0 {
            return Err(invalid("empty vectored call", cursor.stats(stats)));
        }
        stats.syscalls += 1;
        stats.max_iovecs = stats.max_iovecs.max(iov_scratch.len());
        let n = match sink.pwritev(current_offset, iov_scratch) {
            Ok(0) => {
                return Err(WriteFailure {
                    source: io::Error::new(
                        io::ErrorKind::WriteZero,
                        "pwritev made no progress",
                    ),
                    stats:  cursor.stats(stats),
                });
            }
            Ok(n) if n > offered => {
                return Err(invalid(
                    "pwritev reported more bytes than offered",
                    cursor.stats(stats),
                ));
            }
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {
                stats.interrupted += 1;
                continue;
            }
            Err(source) => {
                return Err(WriteFailure { source, stats: cursor.stats(stats) });
            }
        };
        if n < offered {
            stats.short_writes += 1;
        }
        cursor.advance(buffers, n);
        stats.bytes_written =
            stats.bytes_written.checked_add(n as u64).ok_or_else(|| {
                invalid("written byte count overflow", cursor.stats(stats))
            })?;
        current_offset = current_offset
            .checked_add(n as u64)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| {
                invalid("positioned offset overflow", cursor.stats(stats))
            })?;
    }
    Ok(cursor.stats(stats))
}

/// Current production kernel: one positioned write-all loop per physical
/// batch. Empty buffers are ignored only for symmetry with [`pwritev_all`].
pub fn k_pwrite_all(
    file: &File,
    offset: u64,
    buffers: &[&[u8]],
) -> Result<WriteStats, WriteFailure> {
    let mut file = file;
    k_pwrite_all_with(&mut file, offset, buffers)
}

fn k_pwrite_all_with<S: PositionedSink>(
    sink: &mut S,
    offset: u64,
    buffers: &[&[u8]],
) -> Result<WriteStats, WriteFailure> {
    let mut current_offset = offset;
    let mut stats = WriteStats::default();
    if buffers.iter().any(|buffer| buffer.is_empty()) {
        return Err(invalid(
            "physical batch buffers must be nonempty",
            WriteStats::default(),
        ));
    }
    for (index, buffer) in buffers.iter().copied().enumerate() {
        let mut rest = buffer;
        while !rest.is_empty() {
            stats.syscalls += 1;
            let n = match sink.pwrite(current_offset, rest) {
                Ok(0) => {
                    stats.completed_bufs = index;
                    stats.partial_buf_byte = buffer.len() - rest.len();
                    return Err(WriteFailure {
                        source: io::Error::new(
                            io::ErrorKind::WriteZero,
                            "pwrite made no progress",
                        ),
                        stats,
                    });
                }
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {
                    stats.interrupted += 1;
                    continue;
                }
                Err(source) => {
                    stats.completed_bufs = index;
                    stats.partial_buf_byte = buffer.len() - rest.len();
                    return Err(WriteFailure { source, stats });
                }
            };
            if n < rest.len() {
                stats.short_writes += 1;
            }
            stats.bytes_written += n as u64;
            current_offset = current_offset
                .checked_add(n as u64)
                .ok_or_else(|| invalid("positioned offset overflow", stats))?;
            rest = &rest[n..];
        }
        stats.completed_bufs = index + 1;
        stats.partial_buf_byte = 0;
    }
    Ok(stats)
}

/// Contiguous-copy candidate. Groups larger than `copy_cap` are partitioned
/// only at physical batch boundaries. A single batch larger than the cap uses
/// the current no-copy kernel, which is the predeclared large-batch fallback.
pub fn copy_contiguous_all(
    file: &File,
    offset: u64,
    buffers: &[&[u8]],
    copy_cap: usize,
    scratch: &mut Vec<u8>,
) -> Result<WriteStats, WriteFailure> {
    let mut file = file;
    copy_contiguous_all_with(&mut file, offset, buffers, copy_cap, scratch)
}

fn copy_contiguous_all_with<S: PositionedSink>(
    sink: &mut S,
    offset: u64,
    buffers: &[&[u8]],
    copy_cap: usize,
    scratch: &mut Vec<u8>,
) -> Result<WriteStats, WriteFailure> {
    if copy_cap == 0 {
        return Err(invalid("copy cap must be positive", WriteStats::default()));
    }
    if buffers.iter().any(|buffer| buffer.is_empty()) {
        return Err(invalid(
            "physical batch buffers must be nonempty",
            WriteStats::default(),
        ));
    }
    let mut stats = WriteStats::default();
    let mut offset = offset;
    let mut start = 0usize;
    while start < buffers.len() {
        if buffers[start].len() > copy_cap {
            let one = match k_pwrite_all_with(
                sink,
                offset,
                &buffers[start..start + 1],
            ) {
                Ok(one) => one,
                Err(failure) => {
                    return Err(failure_with_prefix(failure, stats, start))
                }
            };
            accumulate(&mut stats, one);
            stats.completed_bufs = start + 1;
            stats.partial_buf_byte = 0;
            offset = offset
                .checked_add(one.bytes_written)
                .ok_or_else(|| invalid("positioned offset overflow", stats))?;
            start += 1;
            continue;
        }
        let mut end = start;
        let mut bytes = 0usize;
        while end < buffers.len() && buffers[end].len() <= copy_cap - bytes {
            bytes += buffers[end].len();
            end += 1;
        }
        scratch.clear();
        if scratch.capacity() < bytes {
            scratch.reserve(bytes - scratch.capacity());
        }
        for buffer in &buffers[start..end] {
            scratch.extend_from_slice(buffer);
        }
        stats.copied_bytes += bytes as u64;
        let one = match k_pwrite_all_with(sink, offset, &[scratch.as_slice()]) {
            Ok(one) => one,
            Err(mut failure) => {
                let local_written = failure.stats.bytes_written as usize;
                failure = failure_with_prefix(failure, stats, 0);
                let cursor =
                    cursor_after_prefix(&buffers[start..end], local_written);
                failure.stats.completed_bufs = start + cursor.buf;
                failure.stats.partial_buf_byte = cursor.intra;
                return Err(failure)
            }
        };
        accumulate(&mut stats, one);
        stats.completed_bufs = end;
        stats.partial_buf_byte = 0;
        offset = offset
            .checked_add(one.bytes_written)
            .ok_or_else(|| invalid("positioned offset overflow", stats))?;
        start = end;
    }
    Ok(stats)
}

fn accumulate(total: &mut WriteStats, one: WriteStats) {
    total.syscalls += one.syscalls;
    total.bytes_written += one.bytes_written;
    total.short_writes += one.short_writes;
    total.interrupted += one.interrupted;
    total.max_iovecs = total.max_iovecs.max(one.max_iovecs);
}

fn failure_with_prefix(
    mut failure: WriteFailure,
    prefix: WriteStats,
    completed_base: usize,
) -> WriteFailure {
    failure.stats.syscalls += prefix.syscalls;
    failure.stats.bytes_written += prefix.bytes_written;
    failure.stats.copied_bytes += prefix.copied_bytes;
    failure.stats.short_writes += prefix.short_writes;
    failure.stats.interrupted += prefix.interrupted;
    failure.stats.max_iovecs = failure.stats.max_iovecs.max(prefix.max_iovecs);
    failure.stats.completed_bufs += completed_base;
    failure
}

fn cursor_after_prefix(buffers: &[&[u8]], mut written: usize) -> Cursor {
    let mut cursor = Cursor { buf: 0, intra: 0 };
    while cursor.buf < buffers.len() && written != 0 {
        let available = buffers[cursor.buf].len() - cursor.intra;
        let take = written.min(available);
        cursor.advance(buffers, take);
        written -= take;
    }
    cursor
}

/// Outcome of one deterministic correctness case executed by the release
/// binary that will execute the timing matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrectnessStatus {
    Pass,
    Fail,
}

impl CorrectnessStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
        }
    }
}

/// One named same-binary correctness result. `details` is deliberately plain
/// text so the driver can serialize it without adding a serde dependency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrectnessCase {
    pub name:    &'static str,
    pub status:  CorrectnessStatus,
    pub details: String,
}

/// Complete deterministic syscall/cap/roll correctness report.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CorrectnessReport {
    pub cases: Vec<CorrectnessCase>,
}

impl CorrectnessReport {
    #[must_use]
    pub fn all_passed(&self) -> bool {
        self.cases.iter().all(|case| case.status == CorrectnessStatus::Pass)
    }

    #[must_use]
    pub fn passed(&self) -> usize {
        self.cases
            .iter()
            .filter(|case| case.status == CorrectnessStatus::Pass)
            .count()
    }

    #[must_use]
    pub fn failed(&self) -> usize { self.cases.len() - self.passed() }

    fn record<F>(&mut self, name: &'static str, check: F)
    where
        F: FnOnce() -> Result<String, String>,
    {
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(check));
        let (status, details) = match result {
            Ok(Ok(details)) => (CorrectnessStatus::Pass, details),
            Ok(Err(details)) => (CorrectnessStatus::Fail, details),
            Err(payload) => {
                let message = payload
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| {
                        payload.downcast_ref::<String>().map(String::as_str)
                    })
                    .unwrap_or("non-string panic");
                (CorrectnessStatus::Fail, format!("panic: {message}"))
            }
        };
        self.cases.push(CorrectnessCase { name, status, details });
    }
}

macro_rules! require_case {
    ($condition:expr, $($arg:tt)*) => {
        if !($condition) {
            return Err(format!($($arg)*));
        }
    };
}

/// Run the correctness oracle inside the exact executable used for timed
/// rows. This deliberately covers only behavior the isolated harness can
/// execute: positioned-I/O progress/faults, eligibility caps, copy fallback,
/// roll partitioning, and the canonical batch limit. Production-owner
/// cancellation remains an out-of-scope engine-integration prerequisite;
/// driver-level cases cover only the barrier seams they actually execute.
#[must_use]
pub fn same_binary_correctness_report() -> CorrectnessReport {
    let mut report = CorrectnessReport::default();

    report
        .record("pwritev.short.within_first_iovec", || short_progress_case(1));
    report.record("pwritev.short.on_first_iovec_boundary", || {
        short_progress_case(3)
    });
    report.record("pwritev.short.across_first_iovec_boundary", || {
        short_progress_case(4)
    });
    report.record("pwritev.short.on_second_iovec_boundary", || {
        short_progress_case(8)
    });
    report
        .record("pwritev.short.within_last_iovec", || short_progress_case(14));
    report.record("pwritev.short.repeated_partials", repeated_partials_case);
    report.record("pwritev.eintr.before_progress", eintr_before_progress_case);
    report.record("pwritev.eintr.after_progress", eintr_after_progress_case);
    report.record(
        "pwritev.zero_progress.exact_cursor_no_retry",
        zero_progress_case,
    );
    report.record("pwritev.eio.before_progress_no_retry", || {
        terminal_error_case(libc::EIO, None)
    });
    report.record("pwritev.eio.after_partial_no_retry", || {
        terminal_error_case(libc::EIO, Some(6))
    });
    report.record("pwritev.enospc.before_progress_no_retry", || {
        terminal_error_case(libc::ENOSPC, None)
    });
    report.record("pwritev.enospc.after_partial_no_retry", || {
        terminal_error_case(libc::ENOSPC, Some(6))
    });

    report.record("pwritev.iov_max.minus_one", || iov_limit_case(-1));
    report.record("pwritev.iov_max.equal", || iov_limit_case(0));
    report.record("pwritev.iov_max.plus_one", || iov_limit_case(1));
    report.record("pwritev.runtime_iov_below_configured.full", || {
        injected_runtime_iov_case(usize::MAX)
    });
    report.record("pwritev.runtime_iov_below_configured.64", || {
        injected_runtime_iov_case(64)
    });
    report.record("pwritev.runtime_iov_below_configured.256", || {
        injected_runtime_iov_case(256)
    });

    report.record("pwritev.byte_cap.64k_below", || {
        byte_limit_case(64 * 1024, 64 * 1024 - 1)
    });
    report.record("pwritev.byte_cap.64k_equal", || {
        byte_limit_case(64 * 1024, 64 * 1024)
    });
    report.record("pwritev.byte_cap.64k_above", || {
        byte_limit_case(64 * 1024, 64 * 1024 + 1)
    });
    report.record("pwritev.byte_cap.1m_below", || {
        byte_limit_case(1024 * 1024, 1024 * 1024 - 1)
    });
    report.record("pwritev.byte_cap.1m_equal", || {
        byte_limit_case(1024 * 1024, 1024 * 1024)
    });
    report.record("pwritev.byte_cap.1m_above", || {
        byte_limit_case(1024 * 1024, 1024 * 1024 + 1)
    });
    report.record("pwritev.byte_cap.4m_below", || {
        byte_limit_case(4 * 1024 * 1024, 4 * 1024 * 1024 - 1)
    });
    report.record("pwritev.byte_cap.4m_equal", || {
        byte_limit_case(4 * 1024 * 1024, 4 * 1024 * 1024)
    });
    report.record("pwritev.byte_cap.4m_above", || {
        byte_limit_case(4 * 1024 * 1024, 4 * 1024 * 1024 + 1)
    });
    report.record("pwritev.byte_cap.8m_below", || {
        byte_limit_case(8 * 1024 * 1024, 8 * 1024 * 1024 - 1)
    });
    report.record("pwritev.byte_cap.8m_equal", || {
        byte_limit_case(8 * 1024 * 1024, 8 * 1024 * 1024)
    });
    report.record("pwritev.byte_cap.8m_above", || {
        byte_limit_case(8 * 1024 * 1024, 8 * 1024 * 1024 + 1)
    });

    report.record("pwritev.limit.zero_iov_cap_before_syscall", || {
        invalid_limit_case(0, 1, 0)
    });
    report.record("pwritev.limit.zero_byte_cap_before_syscall", || {
        invalid_limit_case(1, 0, 0)
    });
    report.record("pwritev.limit.byte_cap_over_isize_before_syscall", || {
        invalid_limit_case(1, isize::MAX as usize + 1, 0)
    });
    report.record("pwritev.offset.over_off_t_before_syscall", || {
        invalid_limit_case(1, 1, u64::MAX)
    });
    report.record(
        "pwritev.offset.progress_overflow_exact_cursor",
        offset_overflow_case,
    );

    report.record("copy.cap.partitions_at_batch_boundary", copy_partition_case);
    report.record(
        "copy.cap.oversize_batch_uses_no_copy_fallback",
        copy_fallback_case,
    );
    report.record(
        "copy.failure.prior_progress_exact_cursor_no_retry",
        copy_prior_progress_fault_case,
    );

    report.record("fdatasync.eio.single_attempt", fdatasync_eio_case);

    report.record("roll.exact_remaining_stays_old_file", roll_exact_case);
    report.record("roll.before_first_batch", roll_before_first_case);
    report.record("roll.old_and_new_file_partitions", roll_old_new_case);
    report.record(
        "roll.fault.old_prefix_preserved_new_eio_no_retry",
        roll_fault_prefix_case,
    );
    report.record("roll.oversize_batch_rejected", roll_oversize_case);

    report.record("canonical_batch.64m_legal", canonical_limit_legal_case);
    report.record(
        "canonical_batch.64m_plus_one_rejected",
        canonical_limit_plus_one_case,
    );

    report
}

fn short_progress_case(first: usize) -> Result<String, String> {
    let expected = b"abcdefghijklmno";
    let (result, sink) =
        scripted_run([Action::Bytes(first), Action::Bytes(usize::MAX)], 8, 99);
    let stats = result.map_err(|failure| failure.to_string())?;
    require_case!(
        stats.bytes_written == 15,
        "bytes_written={}",
        stats.bytes_written
    );
    require_case!(
        (stats.completed_bufs, stats.partial_buf_byte) == (3, 0),
        "cursor={}:{}",
        stats.completed_bufs,
        stats.partial_buf_byte
    );
    require_case!(
        stats.short_writes == 1,
        "short_writes={}",
        stats.short_writes
    );
    require_case!(sink.calls.len() == 2, "calls={}", sink.calls.len());
    require_case!(
        sink.calls[1].offset == 7 + first as u64,
        "retry_offset={}",
        sink.calls[1].offset
    );
    require_case!(
        sink.image.get(7..) == Some(expected.as_slice()),
        "image mismatch"
    );
    Ok(format!("first_progress={first}; cursor=3:0; exact_bytes=15"))
}

fn repeated_partials_case() -> Result<String, String> {
    let expected = b"abcdefghijklmno";
    let (result, sink) = scripted_run(
        [
            Action::Bytes(2),
            Action::Bytes(1),
            Action::Bytes(4),
            Action::Bytes(usize::MAX),
        ],
        8,
        99,
    );
    let stats = result.map_err(|failure| failure.to_string())?;
    require_case!(
        stats.short_writes == 3,
        "short_writes={}",
        stats.short_writes
    );
    require_case!(stats.syscalls == 4, "syscalls={}", stats.syscalls);
    let offsets: Vec<u64> = sink.calls.iter().map(|call| call.offset).collect();
    require_case!(offsets == [7, 9, 10, 14], "offsets={offsets:?}");
    require_case!(
        sink.image.get(7..) == Some(expected.as_slice()),
        "image mismatch"
    );
    Ok("partials=2,1,4; offsets=7,9,10,14; exact_bytes=15".to_owned())
}

fn eintr_before_progress_case() -> Result<String, String> {
    let (result, sink) = scripted_run(
        [Action::Err(libc::EINTR), Action::Bytes(usize::MAX)],
        8,
        99,
    );
    let stats = result.map_err(|failure| failure.to_string())?;
    require_case!(stats.interrupted == 1, "interrupted={}", stats.interrupted);
    require_case!(stats.syscalls == 2, "syscalls={}", stats.syscalls);
    require_case!(sink.calls.len() == 2, "calls={}", sink.calls.len());
    require_case!(sink.calls[0] == sink.calls[1], "EINTR changed cursor");
    require_case!(
        sink.image.get(7..) == Some(b"abcdefghijklmno"),
        "image mismatch"
    );
    Ok("interrupted=1; retry_cursor=0:0; exact_bytes=15".to_owned())
}

fn eintr_after_progress_case() -> Result<String, String> {
    let (result, sink) = scripted_run(
        [Action::Bytes(4), Action::Err(libc::EINTR), Action::Bytes(usize::MAX)],
        8,
        99,
    );
    let stats = result.map_err(|failure| failure.to_string())?;
    require_case!(stats.interrupted == 1, "interrupted={}", stats.interrupted);
    require_case!(
        stats.short_writes == 1,
        "short_writes={}",
        stats.short_writes
    );
    require_case!(stats.syscalls == 3, "syscalls={}", stats.syscalls);
    let offsets: Vec<u64> = sink.calls.iter().map(|call| call.offset).collect();
    require_case!(offsets == [7, 11, 11], "offsets={offsets:?}");
    require_case!(
        sink.image.get(7..) == Some(b"abcdefghijklmno"),
        "image mismatch"
    );
    Ok("progress=4; interrupted=1; retry_offset=11; exact_bytes=15".to_owned())
}

fn zero_progress_case() -> Result<String, String> {
    let (result, sink) = scripted_run(
        [Action::Bytes(4), Action::Zero, Action::Bytes(usize::MAX)],
        8,
        99,
    );
    let failure = match result {
        Ok(stats) => {
            return Err(format!("expected WriteZero failure, got {stats:?}"))
        }
        Err(failure) => failure,
    };
    require_case!(
        failure.source.kind() == io::ErrorKind::WriteZero,
        "error_kind={:?}",
        failure.source.kind()
    );
    require_case!(
        failure.stats.bytes_written == 4,
        "bytes={}",
        failure.stats.bytes_written
    );
    require_case!(
        (failure.stats.completed_bufs, failure.stats.partial_buf_byte)
            == (1, 1),
        "cursor={}:{}",
        failure.stats.completed_bufs,
        failure.stats.partial_buf_byte
    );
    require_case!(
        sink.calls.len() == 2,
        "terminal zero retried; calls={}",
        sink.calls.len()
    );
    Ok("error=WriteZero; bytes=4; cursor=1:1; calls=2".to_owned())
}

fn terminal_error_case(
    errno: i32,
    partial: Option<usize>,
) -> Result<String, String> {
    let (result, sink, expected_bytes, expected_cursor, expected_calls) =
        if let Some(bytes) = partial {
            let (result, sink) = scripted_run(
                [
                    Action::Bytes(bytes),
                    Action::Err(errno),
                    Action::Bytes(usize::MAX),
                ],
                8,
                99,
            );
            (result, sink, bytes as u64, (1, bytes - 3), 2)
        } else {
            let (result, sink) = scripted_run(
                [Action::Err(errno), Action::Bytes(usize::MAX)],
                8,
                99,
            );
            (result, sink, 0, (0, 0), 1)
        };
    let failure = match result {
        Ok(stats) => {
            return Err(format!("expected terminal I/O failure, got {stats:?}"))
        }
        Err(failure) => failure,
    };
    require_case!(
        failure.source.raw_os_error() == Some(errno),
        "errno={:?}",
        failure.source.raw_os_error()
    );
    require_case!(
        failure.stats.bytes_written == expected_bytes,
        "bytes={}",
        failure.stats.bytes_written
    );
    require_case!(
        (failure.stats.completed_bufs, failure.stats.partial_buf_byte)
            == expected_cursor,
        "cursor={}:{}",
        failure.stats.completed_bufs,
        failure.stats.partial_buf_byte
    );
    require_case!(
        sink.calls.len() == expected_calls,
        "terminal error retried; calls={}",
        sink.calls.len()
    );
    Ok(format!(
        "errno={errno}; bytes={expected_bytes}; cursor={}:{}; \
         calls={expected_calls}",
        expected_cursor.0, expected_cursor.1
    ))
}

fn iov_limit_case(delta: isize) -> Result<String, String> {
    let limit = runtime_iov_max();
    require_case!(limit >= 2, "runtime_iov_max={limit} cannot exercise -1");
    let count = usize::try_from(limit as isize + delta).map_err(|_| {
        format!("invalid count for runtime_iov_max={limit}, delta={delta}")
    })?;
    let storage = vec![0x5Au8; count];
    let buffers: Vec<&[u8]> =
        storage.iter().map(std::slice::from_ref).collect();
    let mut sink = ScriptSink::new([]);
    let mut scratch = Vec::new();
    let stats = pwritev_all(
        &mut sink,
        0,
        &buffers,
        usize::MAX,
        isize::MAX as usize,
        &mut scratch,
    )
    .map_err(|failure| failure.to_string())?;
    let expected_calls = if delta > 0 { 2 } else { 1 };
    require_case!(
        stats.syscalls == expected_calls,
        "syscalls={}",
        stats.syscalls
    );
    require_case!(
        stats.max_iovecs == count.min(limit),
        "max_iovecs={}",
        stats.max_iovecs
    );
    require_case!(
        stats.bytes_written == count as u64,
        "bytes={}",
        stats.bytes_written
    );
    require_case!(
        stats.copied_bytes == 0,
        "copied_bytes={}",
        stats.copied_bytes
    );
    require_case!(
        sink.calls.iter().all(|call| call.lens.len() <= limit),
        "runtime iovec cap exceeded"
    );
    require_case!(
        sink.image.as_slice() == storage.as_slice(),
        "image mismatch"
    );
    Ok(format!(
        "runtime_iov_max={limit}; buffers={count}; syscalls={expected_calls}"
    ))
}

fn injected_runtime_iov_case(configured_cap: usize) -> Result<String, String> {
    const INJECTED_RUNTIME_CAP: usize = 7;
    let storage = [0x5Au8; INJECTED_RUNTIME_CAP + 1];
    let buffers: Vec<&[u8]> =
        storage.iter().map(std::slice::from_ref).collect();
    let mut sink = ScriptSink::new([]);
    let mut scratch = Vec::new();
    let stats = pwritev_all_with_runtime_cap(
        &mut sink,
        0,
        &buffers,
        configured_cap,
        isize::MAX as usize,
        INJECTED_RUNTIME_CAP,
        &mut scratch,
    )
    .map_err(|failure| failure.to_string())?;
    require_case!(stats.syscalls == 2, "syscalls={}", stats.syscalls);
    require_case!(
        stats.max_iovecs == INJECTED_RUNTIME_CAP,
        "max_iovecs={}",
        stats.max_iovecs
    );
    require_case!(
        sink.calls.iter().map(|call| call.lens.len()).collect::<Vec<_>>()
            == [INJECTED_RUNTIME_CAP, 1],
        "call_iovecs={:?}",
        sink.calls.iter().map(|call| call.lens.len()).collect::<Vec<_>>()
    );
    require_case!(
        sink.image.as_slice() == storage.as_slice(),
        "image mismatch"
    );
    Ok(format!(
        "configured_cap={configured_cap}; \
         injected_runtime_cap={INJECTED_RUNTIME_CAP}; calls=7,1"
    ))
}

fn byte_limit_case(cap: usize, total: usize) -> Result<String, String> {
    let bytes = vec![0xA5; total];
    let mut sink = ScriptSink::new([]);
    let mut scratch = Vec::new();
    let stats =
        pwritev_all(&mut sink, 0, &[bytes.as_slice()], 8, cap, &mut scratch)
            .map_err(|failure| failure.to_string())?;
    let expected_calls = if total > cap { 2 } else { 1 };
    require_case!(
        stats.syscalls == expected_calls,
        "syscalls={}",
        stats.syscalls
    );
    require_case!(
        stats.bytes_written == total as u64,
        "bytes={}",
        stats.bytes_written
    );
    require_case!(
        sink.calls.iter().all(|call| call.lens.iter().sum::<usize>() <= cap),
        "offered byte cap exceeded"
    );
    require_case!(sink.image == bytes, "image mismatch");
    Ok(format!("cap={cap}; bytes={total}; syscalls={expected_calls}"))
}

fn invalid_limit_case(
    iov_cap: usize,
    byte_cap: usize,
    offset: u64,
) -> Result<String, String> {
    let data = b"a".as_slice();
    let mut sink = ScriptSink::new([]);
    let mut scratch = Vec::new();
    let result = pwritev_all(
        &mut sink,
        offset,
        &[data],
        iov_cap,
        byte_cap,
        &mut scratch,
    );
    let failure = match result {
        Ok(stats) => {
            return Err(format!(
                "invalid limit unexpectedly accepted: {stats:?}"
            ))
        }
        Err(failure) => failure,
    };
    require_case!(
        failure.source.kind() == io::ErrorKind::InvalidInput,
        "error_kind={:?}",
        failure.source.kind()
    );
    require_case!(sink.calls.is_empty(), "syscall observed before rejection");
    require_case!(
        failure.stats == WriteStats::default(),
        "nonzero stats={:?}",
        failure.stats
    );
    Ok(format!(
        "iov_cap={iov_cap}; byte_cap={byte_cap}; offset={offset}; calls=0"
    ))
}

fn offset_overflow_case() -> Result<String, String> {
    let data = b"a".as_slice();
    let mut sink = ScriptSink::count_only([Action::Bytes(1), Action::Bytes(1)]);
    let mut scratch = Vec::new();
    let result =
        pwritev_all(&mut sink, i64::MAX as u64, &[data], 1, 1, &mut scratch);
    let failure = match result {
        Ok(stats) => {
            return Err(format!(
                "overflowing offset unexpectedly accepted: {stats:?}"
            ))
        }
        Err(failure) => failure,
    };
    require_case!(
        failure.source.kind() == io::ErrorKind::InvalidInput,
        "error_kind={:?}",
        failure.source.kind()
    );
    require_case!(
        failure.stats.bytes_written == 1,
        "bytes={}",
        failure.stats.bytes_written
    );
    require_case!(
        (failure.stats.completed_bufs, failure.stats.partial_buf_byte)
            == (1, 0),
        "cursor={}:{}",
        failure.stats.completed_bufs,
        failure.stats.partial_buf_byte
    );
    require_case!(
        sink.calls.len() == 1,
        "overflow retried; calls={}",
        sink.calls.len()
    );
    Ok("start=off_t_max; bytes=1; cursor=1:0; calls=1".to_owned())
}

fn correctness_file() -> Result<File, String> {
    let name = b"pwritev-correctness\0";
    let fd =
        unsafe { libc::memfd_create(name.as_ptr().cast(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(format!(
            "create correctness memfd: {}",
            io::Error::last_os_error()
        ));
    }
    // SAFETY: `memfd_create` returned a fresh owned descriptor above. Moving
    // it into `File` transfers the only ownership and closes it on drop.
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn read_exact_image(file: &File, len: usize) -> Result<Vec<u8>, String> {
    let mut image = vec![0; len];
    let mut read = 0usize;
    while read < len {
        let n = file
            .read_at(&mut image[read..], read as u64)
            .map_err(|error| format!("read correctness image: {error}"))?;
        require_case!(n != 0, "short correctness image at byte {read}");
        read += n;
    }
    Ok(image)
}

fn copy_partition_case() -> Result<String, String> {
    let file = correctness_file()?;
    let buffers = [b"abc".as_slice(), b"defg".as_slice(), b"hijkl".as_slice()];
    let expected = buffers.concat();
    let mut scratch = Vec::new();
    let stats = copy_contiguous_all(&file, 0, &buffers, 7, &mut scratch)
        .map_err(|failure| failure.to_string())?;
    require_case!(stats.syscalls == 2, "syscalls={}", stats.syscalls);
    require_case!(
        stats.copied_bytes == 12,
        "copied_bytes={}",
        stats.copied_bytes
    );
    require_case!(stats.bytes_written == 12, "bytes={}", stats.bytes_written);
    require_case!(
        stats.completed_bufs == 3,
        "completed_bufs={}",
        stats.completed_bufs
    );
    require_case!(scratch.len() <= 7, "scratch_len={}", scratch.len());
    require_case!(
        read_exact_image(&file, expected.len())? == expected,
        "image mismatch"
    );
    Ok("cap=7; batch_lengths=3,4,5; chunks=7,5; copied=12".to_owned())
}

fn copy_fallback_case() -> Result<String, String> {
    let file = correctness_file()?;
    let bytes = b"abcdefgh";
    let buffers = [bytes.as_slice()];
    let mut scratch = Vec::new();
    let stats = copy_contiguous_all(&file, 0, &buffers, 7, &mut scratch)
        .map_err(|failure| failure.to_string())?;
    require_case!(stats.syscalls == 1, "syscalls={}", stats.syscalls);
    require_case!(
        stats.copied_bytes == 0,
        "copied_bytes={}",
        stats.copied_bytes
    );
    require_case!(stats.bytes_written == 8, "bytes={}", stats.bytes_written);
    require_case!(scratch.is_empty(), "oversize fallback touched copy scratch");
    require_case!(
        read_exact_image(&file, bytes.len())? == bytes,
        "image mismatch"
    );
    Ok("cap=7; batch_len=8; copied=0; fallback_syscalls=1".to_owned())
}

fn copy_prior_progress_fault_case() -> Result<String, String> {
    let buffers = [b"abc".as_slice(), b"de".as_slice(), b"fgh".as_slice()];
    let mut sink = ScriptSink::new([
        Action::Bytes(usize::MAX),
        Action::Bytes(1),
        Action::Err(libc::EIO),
        Action::Bytes(usize::MAX),
    ]);
    let mut scratch = Vec::new();
    let result =
        copy_contiguous_all_with(&mut sink, 0, &buffers, 3, &mut scratch);
    let failure = match result {
        Ok(stats) => return Err(format!("expected EIO, got {stats:?}")),
        Err(failure) => failure,
    };
    require_case!(
        failure.source.raw_os_error() == Some(libc::EIO),
        "errno={:?}",
        failure.source.raw_os_error()
    );
    require_case!(
        failure.stats.syscalls == 3,
        "syscalls={}",
        failure.stats.syscalls
    );
    require_case!(
        failure.stats.bytes_written == 4,
        "bytes={}",
        failure.stats.bytes_written
    );
    require_case!(
        failure.stats.copied_bytes == 5,
        "copied={}",
        failure.stats.copied_bytes
    );
    require_case!(
        failure.stats.short_writes == 1,
        "short={}",
        failure.stats.short_writes
    );
    require_case!(
        (failure.stats.completed_bufs, failure.stats.partial_buf_byte)
            == (1, 1),
        "cursor={}:{}",
        failure.stats.completed_bufs,
        failure.stats.partial_buf_byte
    );
    require_case!(sink.calls.len() == 3, "terminal EIO retried");
    require_case!(sink.image.as_slice() == b"abcd", "visible prefix mismatch");
    Ok("prior_bytes=3; fault_chunk_progress=1; bytes=4; copied=5; cursor=1:1; \
        calls=3"
        .to_owned())
}

fn fdatasync_eio_case() -> Result<String, String> {
    struct EioSync(std::cell::Cell<usize>);
    impl DataSync for EioSync {
        fn sync_data_once(&self) -> io::Result<()> {
            self.0.set(self.0.get() + 1);
            Err(io::Error::from_raw_os_error(libc::EIO))
        }
    }

    let sink = EioSync(std::cell::Cell::new(0));
    let failure = fdatasync_once(&sink)
        .expect_err("scripted fdatasync EIO must propagate");
    require_case!(
        failure.raw_os_error() == Some(libc::EIO),
        "errno={:?}",
        failure.raw_os_error()
    );
    require_case!(sink.0.get() == 1, "fdatasync_calls={}", sink.0.get());
    Ok("errno=EIO; fdatasync_calls=1; retries=0".to_owned())
}

fn roll_exact_case() -> Result<String, String> {
    let actual = segment_partitions(&[30, 70], 100, 100)
        .map_err(|index| format!("unexpected rejection at {index}"))?;
    let expected = [SegmentPartition {
        start:       0,
        end:         2,
        roll_before: false,
    }];
    require_case!(actual == expected, "partitions={actual:?}");
    Ok("remaining=100; batches=30,70; old=0..2; rolls=0".to_owned())
}

fn roll_before_first_case() -> Result<String, String> {
    let actual = segment_partitions(&[60, 20], 50, 100)
        .map_err(|index| format!("unexpected rejection at {index}"))?;
    let expected = [SegmentPartition {
        start:       0,
        end:         2,
        roll_before: true,
    }];
    require_case!(actual == expected, "partitions={actual:?}");
    Ok("remaining=50; batches=60,20; new=0..2; roll_before=true".to_owned())
}

fn roll_old_new_case() -> Result<String, String> {
    let actual = segment_partitions(&[30, 40, 50, 20], 100, 100)
        .map_err(|index| format!("unexpected rejection at {index}"))?;
    let expected = [
        SegmentPartition { start: 0, end: 2, roll_before: false },
        SegmentPartition { start: 2, end: 4, roll_before: true },
    ];
    require_case!(actual == expected, "partitions={actual:?}");
    Ok("old=0..2; new=2..4; roll_before_new=true".to_owned())
}

fn roll_fault_prefix_case() -> Result<String, String> {
    let buffers = [b"abc".as_slice(), b"de".as_slice(), b"fghi".as_slice()];
    let partitions = segment_partitions(&[3, 2, 4], 5, 5)
        .map_err(|index| format!("unexpected rejection at {index}"))?;
    require_case!(
        partitions
            == [
                SegmentPartition {
                    start:       0,
                    end:         2,
                    roll_before: false,
                },
                SegmentPartition {
                    start:       2,
                    end:         3,
                    roll_before: true,
                },
            ],
        "partitions={partitions:?}"
    );
    let mut sink = ScriptSink::new([
        Action::Bytes(usize::MAX),
        Action::Err(libc::EIO),
        Action::Bytes(usize::MAX),
    ]);
    let mut scratch = Vec::new();
    pwritev_all_with_runtime_cap(
        &mut sink,
        0,
        &buffers[..2],
        8,
        99,
        8,
        &mut scratch,
    )
    .map_err(|failure| failure.to_string())?;
    let result = pwritev_all_with_runtime_cap(
        &mut sink,
        0,
        &buffers[2..],
        8,
        99,
        8,
        &mut scratch,
    );
    let failure = match result {
        Ok(stats) => {
            return Err(format!("expected new-file EIO, got {stats:?}"))
        }
        Err(failure) => failure,
    };
    require_case!(
        failure.source.raw_os_error() == Some(libc::EIO),
        "errno={:?}",
        failure.source.raw_os_error()
    );
    require_case!(sink.calls.len() == 2, "new-file EIO retried");
    require_case!(sink.image.as_slice() == b"abcde", "old-file prefix changed");
    Ok("old_partition=0..2 durable_candidate_prefix=5; \
        new_partition_eio_calls=1; retry=0"
        .to_owned())
}

fn roll_oversize_case() -> Result<String, String> {
    let actual = segment_partitions(&[20, 101, 30], 100, 100);
    require_case!(actual == Err(1), "result={actual:?}");
    Ok("empty_capacity=100; rejected_batch=1; batch_len=101".to_owned())
}

fn canonical_total_len(payload: &[u8]) -> Result<u64, EncodeError> {
    let frames = [Subframe::plain(1, 0, 0, payload)];
    BatchEncoder::total_len(&BatchInput {
        segment_epoch:        7,
        batch_id:             0,
        first_global_pos:     0,
        stream_id:            1,
        category_id:          1,
        first_stream_version: 0,
        crypto_chain:         None,
        subframes:            &frames,
    })
}

fn canonical_limit_legal_case() -> Result<String, String> {
    let framing = HEADER_LEN + SUBFRAME_HDR_LEN + MARKER_LEN;
    let legal_payload = MAX_BATCH_LEN as usize - framing;
    let payload = vec![0x5A; legal_payload];
    let total =
        canonical_total_len(&payload).map_err(|error| error.to_string())?;
    require_case!(total == MAX_BATCH_LEN, "total_len={total}");
    Ok(format!("payload={legal_payload}; total_len={MAX_BATCH_LEN}"))
}

fn canonical_limit_plus_one_case() -> Result<String, String> {
    let framing = HEADER_LEN + SUBFRAME_HDR_LEN + MARKER_LEN;
    let legal_payload = MAX_BATCH_LEN as usize - framing;
    let payload = vec![0x5A; legal_payload + 1];
    match canonical_total_len(&payload) {
        Err(EncodeError::BatchTooLarge { total_len, max }) => {
            require_case!(
                total_len == MAX_BATCH_LEN + 1,
                "total_len={total_len}"
            );
            require_case!(max == MAX_BATCH_LEN, "max={max}");
            Ok(format!(
                "payload={}; rejected_total={}; max={MAX_BATCH_LEN}",
                legal_payload + 1,
                MAX_BATCH_LEN + 1
            ))
        }
        other => Err(format!("expected BatchTooLarge, got {other:?}")),
    }
}

#[derive(Debug, Clone, Copy)]
enum Action {
    Bytes(usize),
    Err(i32),
    Zero,
}

#[derive(Debug, PartialEq, Eq)]
struct Call {
    offset: u64,
    lens:   Vec<usize>,
}

struct ScriptSink {
    actions:       VecDeque<Action>,
    calls:         Vec<Call>,
    image:         Vec<u8>,
    capture_image: bool,
}

impl ScriptSink {
    fn new(actions: impl IntoIterator<Item = Action>) -> Self {
        Self {
            actions:       actions.into_iter().collect(),
            calls:         Vec::new(),
            image:         Vec::new(),
            capture_image: true,
        }
    }

    fn count_only(actions: impl IntoIterator<Item = Action>) -> Self {
        Self { capture_image: false, ..Self::new(actions) }
    }
}

impl VectoredSink for ScriptSink {
    fn pwritev(
        &mut self,
        offset: u64,
        bufs: &[IoSlice<'_>],
    ) -> io::Result<usize> {
        self.calls.push(Call {
            offset,
            lens: bufs.iter().map(|b| b.len()).collect(),
        });
        match self.actions.pop_front().unwrap_or(Action::Bytes(usize::MAX)) {
            Action::Err(errno) => Err(io::Error::from_raw_os_error(errno)),
            Action::Zero => Ok(0),
            Action::Bytes(limit) => {
                let offered: usize = bufs.iter().map(|b| b.len()).sum();
                let n = limit.min(offered);
                if self.capture_image {
                    let start = usize::try_from(offset).map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "script image offset exceeds usize",
                        )
                    })?;
                    let end = start.checked_add(n).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "script image length overflow",
                        )
                    })?;
                    if self.image.len() < end {
                        self.image.resize(end, 0);
                    }
                    let mut written = 0usize;
                    for buf in bufs {
                        let take = buf.len().min(n - written);
                        self.image[start + written..start + written + take]
                            .copy_from_slice(&buf[..take]);
                        written += take;
                        if written == n {
                            break;
                        }
                    }
                }
                Ok(n)
            }
        }
    }
}

impl PositionedSink for ScriptSink {
    fn pwrite(&mut self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        self.pwritev(offset, &[IoSlice::new(buf)])
    }
}

fn scripted_run(
    actions: impl IntoIterator<Item = Action>,
    cap: usize,
    bytes: usize,
) -> (Result<WriteStats, WriteFailure>, ScriptSink) {
    let a = b"abc".as_slice();
    let b = b"defgh".as_slice();
    let c = b"ijklmno".as_slice();
    let buffers = [a, b, c];
    let mut sink = ScriptSink::new(actions);
    let mut scratch = Vec::new();
    let result = pwritev_all(&mut sink, 7, &buffers, cap, bytes, &mut scratch);
    (result, sink)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(
        actions: impl IntoIterator<Item = Action>,
        cap: usize,
        bytes: usize,
    ) -> (Result<WriteStats, WriteFailure>, ScriptSink) {
        scripted_run(actions, cap, bytes)
    }

    #[test]
    fn advances_within_and_across_iovec_boundaries() {
        for first in [1, 3, 4, 8, 14] {
            let (result, sink) =
                run([Action::Bytes(first), Action::Bytes(usize::MAX)], 8, 99);
            let stats = result.unwrap();
            assert_eq!(stats.bytes_written, 15);
            assert_eq!(stats.completed_bufs, 3);
            assert_eq!(stats.partial_buf_byte, 0);
            assert_eq!(&sink.image[7..], b"abcdefghijklmno");
            assert_eq!(sink.calls[1].offset, 7 + first as u64);
        }
    }

    #[test]
    fn repeated_partials_and_eintr_complete_exact_bytes() {
        let (result, sink) = run(
            [
                Action::Err(libc::EINTR),
                Action::Bytes(2),
                Action::Err(libc::EINTR),
                Action::Bytes(4),
                Action::Bytes(usize::MAX),
            ],
            2,
            6,
        );
        let stats = result.unwrap();
        assert_eq!(stats.interrupted, 2);
        assert_eq!(stats.short_writes, 2);
        assert_eq!(
            stats.syscalls, 6,
            "two EINTR calls + two short calls + two byte-cap chunks",
        );
        assert_eq!(stats.max_iovecs, 2);
        assert_eq!(&sink.image[7..], b"abcdefghijklmno");
    }

    #[test]
    fn iovec_and_byte_caps_are_never_exceeded() {
        let (result, sink) = run([], 2, 4);
        result.unwrap();
        assert!(sink.calls.iter().all(|c| c.lens.len() <= 2));
        assert!(sink.calls.iter().all(|c| c.lens.iter().sum::<usize>() <= 4));
        assert_eq!(sink.calls[0].lens, vec![3, 1]);
    }

    #[test]
    fn runtime_iov_max_plus_one_is_chunked_without_copying() {
        let cap = runtime_iov_max();
        let storage = vec![7u8; cap + 1];
        let buffers: Vec<&[u8]> =
            storage.iter().map(std::slice::from_ref).collect();
        let mut sink = ScriptSink::new([]);
        let mut scratch = Vec::new();
        let stats = pwritev_all(
            &mut sink,
            0,
            &buffers,
            usize::MAX,
            isize::MAX as usize,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(stats.bytes_written, (cap + 1) as u64);
        assert_eq!(stats.syscalls, 2);
        assert_eq!(stats.max_iovecs, cap);
        assert_eq!(sink.calls[0].lens.len(), cap);
        assert_eq!(sink.calls[1].lens.len(), 1);
    }

    #[test]
    fn byte_cap_minus_equal_plus_one_partitions_exactly() {
        let cap = 1024usize;
        for total in [cap - 1, cap, cap + 1] {
            let bytes = vec![0xA5; total];
            let mut sink = ScriptSink::new([]);
            let mut scratch = Vec::new();
            let stats = pwritev_all(
                &mut sink,
                0,
                &[bytes.as_slice()],
                8,
                cap,
                &mut scratch,
            )
            .unwrap();
            assert_eq!(stats.bytes_written, total as u64);
            assert_eq!(stats.syscalls, if total > cap { 2 } else { 1 });
            assert_eq!(sink.image, bytes);
        }
    }

    #[test]
    fn zero_progress_reports_exact_cursor() {
        let (result, _) = run([Action::Bytes(4), Action::Zero], 8, 99);
        let failure = result.unwrap_err();
        assert_eq!(failure.source.kind(), io::ErrorKind::WriteZero);
        assert_eq!(failure.stats.bytes_written, 4);
        assert_eq!(failure.stats.completed_bufs, 1);
        assert_eq!(failure.stats.partial_buf_byte, 1);
    }

    #[test]
    fn terminal_eio_and_enospc_are_not_retried() {
        for errno in [libc::EIO, libc::ENOSPC] {
            let (result, sink) = run(
                [Action::Bytes(8), Action::Err(errno), Action::Bytes(99)],
                8,
                99,
            );
            let failure = result.unwrap_err();
            assert_eq!(failure.source.raw_os_error(), Some(errno));
            assert_eq!(failure.stats.completed_bufs, 2);
            assert_eq!(failure.stats.partial_buf_byte, 0);
            assert_eq!(sink.calls.len(), 2, "terminal error must not retry");
        }
    }

    #[test]
    fn terminal_error_inside_batch_reports_partial_batch() {
        let (result, _) =
            run([Action::Bytes(6), Action::Err(libc::EIO)], 8, 99);
        let failure = result.unwrap_err();
        assert_eq!(failure.stats.completed_bufs, 1);
        assert_eq!(failure.stats.partial_buf_byte, 3);
    }

    #[test]
    fn invalid_limits_and_offset_fail_before_syscall() {
        let a = b"a".as_slice();
        let mut sink = ScriptSink::new([]);
        let mut scratch = Vec::new();
        assert!(pwritev_all(&mut sink, 0, &[a], 0, 1, &mut scratch).is_err());
        assert!(pwritev_all(&mut sink, 0, &[a], 1, 0, &mut scratch).is_err());
        assert!(
            pwritev_all(&mut sink, u64::MAX, &[a], 1, 1, &mut scratch).is_err()
        );
        assert!(sink.calls.is_empty());
    }

    #[test]
    fn empty_buffers_are_rejected_before_the_syscall() {
        let empty = b"".as_slice();
        let data = b"abc".as_slice();
        let mut sink = ScriptSink::new([]);
        let mut scratch = Vec::new();
        let result = pwritev_all(
            &mut sink,
            0,
            &[empty, data, empty],
            1,
            99,
            &mut scratch,
        );
        assert!(result.is_err());
        assert!(sink.calls.is_empty());
    }

    #[test]
    fn roll_partitions_never_cross_files_or_split_batches() {
        assert_eq!(
            segment_partitions(&[30, 40, 50, 20], 100, 100).unwrap(),
            vec![
                SegmentPartition {
                    start:       0,
                    end:         2,
                    roll_before: false,
                },
                SegmentPartition {
                    start:       2,
                    end:         4,
                    roll_before: true,
                },
            ]
        );
        assert_eq!(
            segment_partitions(&[30, 70], 100, 100).unwrap(),
            vec![SegmentPartition {
                start:       0,
                end:         2,
                roll_before: false,
            }],
            "exact capacity does not roll",
        );
        assert_eq!(
            segment_partitions(&[60, 50], 100, 100).unwrap(),
            vec![
                SegmentPartition {
                    start:       0,
                    end:         1,
                    roll_before: false,
                },
                SegmentPartition {
                    start:       1,
                    end:         2,
                    roll_before: true,
                },
            ]
        );
    }

    #[test]
    fn oversize_or_empty_physical_batch_is_rejected_before_roll() {
        assert_eq!(segment_partitions(&[101], 20, 100), Err(0));
        assert_eq!(segment_partitions(&[20, 0, 30], 100, 100), Err(1));
    }

    #[test]
    fn canonical_batch_limit_accepts_64_mib_and_rejects_plus_one() {
        let framing = HEADER_LEN + SUBFRAME_HDR_LEN + MARKER_LEN;
        let legal_payload = MAX_BATCH_LEN as usize - framing;
        let payload = vec![0x5A; legal_payload + 1];
        let input = |bytes: &[u8]| {
            let frames = [Subframe::plain(1, 0, 0, bytes)];
            BatchEncoder::total_len(&BatchInput {
                segment_epoch:        7,
                batch_id:             0,
                first_global_pos:     0,
                stream_id:            1,
                category_id:          1,
                first_stream_version: 0,
                crypto_chain:         None,
                subframes:            &frames,
            })
        };
        assert_eq!(input(&payload[..legal_payload]), Ok(MAX_BATCH_LEN));
        assert!(matches!(
            input(&payload),
            Err(EncodeError::BatchTooLarge {
                total_len,
                max: MAX_BATCH_LEN,
            }) if total_len == MAX_BATCH_LEN + 1
        ));
    }
}
