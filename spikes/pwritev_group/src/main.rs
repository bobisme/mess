//! Production-layout driver for the bn-1zv6 decision experiment.
//!
//! Commands are intentionally explicit: `verify` is the byte/recovery oracle;
//! `point` emits one raw CSV row. An external ABBA runner chooses order and
//! preserves rejected quiet-guard rows, so the binary never silently tunes a
//! policy or discards a repetition.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, IoSlice, Write as _};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mess_log::encode::{BatchInput, PreparedBatch, Subframe};
use mess_log::fold_chain::ChainHead;
use mess_log::format::{
    CHAIN_LEN, HEADER_LEN, MARKER_LEN, MAX_BATCH_LEN, SEGMENT_HEADER_LEN,
    SUBFRAME_HDR_LEN,
};
use mess_log::runtime::{
    EnospcSite, Fault, FileHandle, Fs, OpenOpts, RealRuntime, Runtime, SimFs,
};
use mess_log::scanner::{AcceptedBatch, scan_image};
use mess_log::writer::{
    PreparedBatchSpec, Receipt, SegmentParams, SegmentWriter, WriteError,
};
use pwritev_group::{
    RealSink, WriteFailure, WriteStats, copy_contiguous_all, fdatasync_once,
    k_pwrite_all, pwritev_all, runtime_iov_max, same_binary_correctness_report,
};

const BYTE_CAP: usize = 8 * 1024 * 1024;
const COPY_CAP: usize = 8 * 1024 * 1024;
const MAX_CORPUS_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const EXPLORE_MIN_BYTES: u64 = 32 * 1024 * 1024;
const EXPLORE_MIN_TIME: Duration = Duration::from_millis(250);
const BOUNDARY_MIN_BYTES: u64 = 64 * 1024 * 1024;
const BOUNDARY_MIN_TIME: Duration = Duration::from_millis(500);
const CONFIRM_MIN_BYTES: u64 = 128 * 1024 * 1024;
const CONFIRM_MIN_TIME: Duration = Duration::from_millis(750);
const VERIFY_ARGS: usize = 2;
const CONTRACT_ARGS: usize = 2;
const CORRECTNESS_ARGS: usize = 4;
const POINT_AUTO_MIN_ARGS: usize = 9;
const POINT_AUTO_MAX_ARGS: usize = 11;
const PREP_AUTO_MIN_ARGS: usize = 7;
const PREP_AUTO_MAX_ARGS: usize = 8;
const BOUNDARY_AUTO_ARGS: usize = 7;
const TRACE_MIN_ARGS: usize = 8;
const TRACE_MAX_ARGS: usize = 9;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Process,
    Os,
    Group,
}

impl Mode {
    fn parse(s: &str) -> Self {
        match s {
            "process" => Self::Process,
            "os" => Self::Os,
            "group" => Self::Group,
            _ => panic!("mode must be process|os|group"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Process => "process",
            Self::Os => "os",
            Self::Group => "group",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IovCap {
    Full,
    Fixed(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Variant {
    KPwrite,
    Pwritev { iov: IovCap, bytes: usize },
    Copy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnitShape {
    Domain,
    NewName,
}

impl UnitShape {
    fn parse(s: &str) -> Self {
        match s {
            "domain" => Self::Domain,
            "new_name" => Self::NewName,
            _ => panic!("unit shape must be domain|new_name"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Domain => "domain",
            Self::NewName => "new_name",
        }
    }

    fn physical_batches_per_unit(self) -> usize {
        match self {
            Self::Domain => 1,
            Self::NewName => 2,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RunLimit {
    Fixed(usize),
    FirstThreshold,
}

impl Variant {
    fn parse(s: &str) -> Self {
        if s == "k_pwrite" {
            return Self::KPwrite;
        }
        if s == "copy_contiguous" {
            return Self::Copy;
        }
        let Some(rest) = s.strip_prefix("pwritev_") else {
            panic!("unknown variant: {s}")
        };
        let (iov, byte_name) = rest.rsplit_once('_').unwrap_or((rest, "8m"));
        let iov = match iov {
            "full" => IovCap::Full,
            "64" => IovCap::Fixed(64),
            "256" => IovCap::Fixed(256),
            _ => panic!("pwritev iov cap must be full|64|256"),
        };
        let bytes = match byte_name {
            "1m" => 1024 * 1024,
            "4m" => 4 * 1024 * 1024,
            "8m" => 8 * 1024 * 1024,
            _ => panic!("pwritev byte cap must be 1m|4m|8m"),
        };
        Self::Pwritev { iov, bytes }
    }

    fn correctness_variants() -> Vec<Self> {
        let mut variants = vec![Self::KPwrite, Self::Copy];
        for iov in [IovCap::Full, IovCap::Fixed(64), IovCap::Fixed(256)] {
            for bytes in [1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024] {
                variants.push(Self::Pwritev { iov, bytes });
            }
        }
        variants
    }

    fn name(self) -> String {
        match self {
            Self::KPwrite => "k_pwrite".into(),
            Self::Copy => "copy_contiguous".into(),
            Self::Pwritev { iov, bytes } => {
                let iov = match iov {
                    IovCap::Full => "full".to_owned(),
                    IovCap::Fixed(cap) => cap.to_string(),
                };
                let bytes = match bytes {
                    1_048_576 => "1m",
                    4_194_304 => "4m",
                    8_388_608 => "8m",
                    _ => unreachable!("predeclared byte cap"),
                };
                format!("pwritev_{iov}_{bytes}")
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum TracePolicyKind {
    KPwrite,
    Fixed(Variant),
    Adaptive { min_width: usize, gathered: Variant, copy_ceiling: usize },
}

#[derive(Debug, Clone)]
struct TracePolicy {
    name: String,
    kind: TracePolicyKind,
}

impl TracePolicy {
    fn parse(value: &str) -> Self {
        if value == "k_pwrite" {
            return Self {
                name: value.to_owned(),
                kind: TracePolicyKind::KPwrite,
            };
        }
        if let Some(variant) = value.strip_prefix("fixed:") {
            let variant = Variant::parse(variant);
            return Self {
                name: format!("fixed:{}", variant.name()),
                kind: TracePolicyKind::Fixed(variant),
            };
        }
        let Some(config) = value.strip_prefix("adaptive:") else {
            panic!(
                "policy must be \
                 k_pwrite|fixed:<variant>|adaptive:<min>:<iov>:<byte_name>:\
                 <copy_ceiling_bytes>"
            )
        };
        let fields: Vec<&str> = config.split(':').collect();
        assert_eq!(fields.len(), 4, "adaptive policy needs four fields");
        let min_width = fields[0].parse::<usize>().expect("adaptive min width");
        assert!(min_width > 0, "adaptive min width must be positive");
        let iov = match fields[1] {
            "full" => IovCap::Full,
            "64" => IovCap::Fixed(64),
            "256" => IovCap::Fixed(256),
            _ => panic!("adaptive iov must be full|64|256"),
        };
        let bytes = match fields[2] {
            "1m" => 1024 * 1024,
            "4m" => 4 * 1024 * 1024,
            "8m" => 8 * 1024 * 1024,
            _ => panic!("adaptive byte cap must be 1m|4m|8m"),
        };
        let copy_ceiling =
            fields[3].parse::<usize>().expect("adaptive copy ceiling bytes");
        assert!(
            copy_ceiling <= COPY_CAP,
            "adaptive copy ceiling exceeds fixed 8 MiB safety cap"
        );
        let gathered = Variant::Pwritev { iov, bytes };
        Self {
            name: format!(
                "adaptive:{min_width}:{}:{}:{copy_ceiling}",
                fields[1], fields[2]
            ),
            kind: TracePolicyKind::Adaptive {
                min_width,
                gathered,
                copy_ceiling,
            },
        }
    }

    fn variant_for(&self, width: usize, group_bytes: usize) -> Variant {
        match self.kind {
            TracePolicyKind::KPwrite => Variant::KPwrite,
            TracePolicyKind::Fixed(_) if width < 2 => Variant::KPwrite,
            TracePolicyKind::Fixed(Variant::Copy) if group_bytes > COPY_CAP => {
                Variant::KPwrite
            }
            TracePolicyKind::Fixed(variant) => variant,
            TracePolicyKind::Adaptive { min_width, gathered, copy_ceiling } => {
                if width < min_width {
                    Variant::KPwrite
                } else if copy_ceiling != 0 && group_bytes <= copy_ceiling {
                    Variant::Copy
                } else {
                    gathered
                }
            }
        }
    }
}

#[derive(Debug)]
struct TraceHistogram {
    canonical:     String,
    widths:        Vec<usize>,
    owner_intents: usize,
    max_width:     usize,
}

impl TraceHistogram {
    fn parse(value: &str) -> Self {
        let mut compressed = BTreeMap::<usize, usize>::new();
        for entry in value.split(';') {
            let (width, count) = entry.split_once(':').unwrap_or_else(|| {
                panic!("histogram entry must be width:count: {entry}")
            });
            let width = width.parse::<usize>().expect("histogram width");
            let count = count.parse::<usize>().expect("histogram count");
            assert!(
                width > 0 && count > 0,
                "histogram values must be positive"
            );
            assert!(
                compressed.insert(width, count).is_none(),
                "duplicate histogram width {width}"
            );
        }
        assert!(!compressed.is_empty(), "histogram must be nonempty");
        let trace_groups = compressed
            .values()
            .try_fold(0usize, |sum, count| {
                sum.checked_add(*count).ok_or("trace group count overflow")
            })
            .expect("trace group count overflow");
        assert!(trace_groups <= 1_000_000, "trace has too many groups");
        let owner_intents = compressed
            .iter()
            .try_fold(0usize, |sum, (width, count)| {
                sum.checked_add(
                    width.checked_mul(*count).ok_or("trace width overflow")?,
                )
                .ok_or("trace owner-intent overflow")
            })
            .expect("trace owner-intent overflow");
        let mut widths = Vec::with_capacity(trace_groups);
        for (&width, &count) in &compressed {
            widths.extend(std::iter::repeat(width).take(count));
        }
        let max_width = *widths.last().expect("nonempty trace");
        let canonical = compressed
            .iter()
            .map(|(width, count)| format!("{width}:{count}"))
            .collect::<Vec<_>>()
            .join(";");
        Self { canonical, widths, owner_intents, max_width }
    }
}

#[derive(Debug, Clone, Copy)]
struct Shape {
    payload:          usize,
    events_per_batch: usize,
    owner_units:      usize,
    writers:          usize,
    chain:            bool,
    unit_shape:       UnitShape,
}

impl Shape {
    fn domain_batch_len(self) -> u64 {
        (HEADER_LEN
            + usize::from(self.chain) * CHAIN_LEN
            + self.events_per_batch * (SUBFRAME_HDR_LEN + self.payload)
            + MARKER_LEN) as u64
    }

    fn registry_batch_len(self) -> u64 {
        (HEADER_LEN
            + usize::from(self.chain) * CHAIN_LEN
            + SUBFRAME_HDR_LEN
            + 24
            + MARKER_LEN) as u64
    }

    fn physical_batches(self) -> usize {
        self.owner_units * self.unit_shape.physical_batches_per_unit()
    }

    fn corpus_len(self) -> u64 {
        let unit_len = self.domain_batch_len()
            + if self.unit_shape == UnitShape::NewName {
                self.registry_batch_len()
            } else {
                0
            };
        unit_len
            .checked_mul(self.owner_units as u64)
            .expect("corpus size overflow")
    }

    fn validate(self) {
        assert!(self.payload > 0);
        assert!(self.events_per_batch > 0);
        assert!(self.owner_units > 0);
        assert!(self.writers > 0);
        assert!(
            self.domain_batch_len() <= MAX_BATCH_LEN,
            "batch exceeds v3 cap"
        );
        let corpus = self.corpus_len();
        assert!(corpus <= MAX_CORPUS_BYTES, "corpus exceeds 2 GiB safety cap");
    }

    fn events(self) -> u64 {
        self.domain_events()
            + if self.unit_shape == UnitShape::NewName {
                self.owner_units as u64
            } else {
                0
            }
    }

    fn domain_events(self) -> u64 {
        self.owner_units as u64 * self.events_per_batch as u64
    }
}

struct Corpus {
    image:      Vec<u8>,
    batches:    Vec<Vec<u8>>,
    receipts:   Vec<Receipt>,
    exit_heads: BTreeMap<u64, [u8; CHAIN_LEN]>,
}

/// A write-counting sink for the prepared-write-pipeline CPU accounting row.
/// It runs the production `PreparedBatch` + `SegmentWriter` stamping/CRC path
/// while deliberately performing no kernel I/O or payload retention.
#[derive(Debug, Clone, Default)]
struct CountingFs;

#[derive(Debug, Clone, Default)]
struct CountingFile {
    len: Arc<AtomicU64>,
}

impl Fs for CountingFs {
    type File = CountingFile;

    fn open(&self, _path: &Path, _opts: OpenOpts) -> io::Result<Self::File> {
        Ok(CountingFile::default())
    }

    fn rename(&self, _from: &Path, _to: &Path) -> io::Result<()> { Ok(()) }

    fn remove(&self, _path: &Path) -> io::Result<()> { Ok(()) }
}

impl FileHandle for CountingFile {
    fn pwrite(&self, off: u64, buf: &[u8]) -> io::Result<usize> {
        let end = off
            .checked_add(buf.len() as u64)
            .ok_or_else(|| io::Error::other("counting write overflow"))?;
        self.len.fetch_max(end, Ordering::Relaxed);
        Ok(buf.len())
    }

    fn pread(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.len.load(Ordering::Relaxed);
        let available = len.saturating_sub(off).min(buf.len() as u64) as usize;
        buf[..available].fill(0);
        Ok(available)
    }

    fn fdatasync(&self) -> io::Result<()> { Ok(()) }

    fn len(&self) -> io::Result<u64> { Ok(self.len.load(Ordering::Relaxed)) }
}

fn scratch(prefix: &str) -> mess_testkit::SweepingTempDir {
    let root = std::env::var_os("MESS_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var("HOME").expect("HOME"))
                .join(".cache/mess-bench")
        });
    std::fs::create_dir_all(&root).expect("create benchmark scratch");
    mess_testkit::temp_dir_in(&root, prefix)
}

fn payload_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|i| ((i as u64 * 131 + 17) & 0xFF) as u8).collect()
}

fn append_canonical_batch<F: Fs>(
    writer: &mut SegmentWriter<F>,
    stream_id: u64,
    category_id: u64,
    first_stream_version: u64,
    payload: &[u8],
    events: usize,
    chain: bool,
    heads: &mut BTreeMap<u64, ChainHead>,
) -> Receipt {
    let subframes: Vec<Subframe<'_>> =
        (0..events).map(|_| Subframe::plain(1, 0, 0, payload)).collect();
    let zero_chain = [0u8; CHAIN_LEN];
    let mut batch = PreparedBatch::encode(&BatchInput {
        segment_epoch:        0,
        batch_id:             0,
        first_global_pos:     0,
        stream_id:            0,
        category_id:          0,
        first_stream_version: 0,
        crypto_chain:         chain.then_some(&zero_chain),
        subframes:            &subframes,
    })
    .expect("prepare canonical batch");
    batch.set_event_type_ids(&vec![1; events]).expect("set canonical type ids");
    let entry = chain.then(|| {
        heads
            .entry(stream_id)
            .or_insert_with(|| ChainHead::genesis(stream_id))
            .entry()
    });
    let receipt = writer
        .append_prepared(
            &PreparedBatchSpec {
                stream_id,
                category_id,
                first_stream_version,
                crypto_chain: entry.as_ref(),
            },
            &mut batch,
        )
        .expect("golden append");
    if chain {
        heads
            .get_mut(&stream_id)
            .expect("chain head exists")
            .absorb_batch((0..events).map(|_| payload));
    }
    receipt
}

fn build_corpus(shape: Shape, path: &Path) -> Corpus {
    shape.validate();
    let rt = RealRuntime::new();
    let fs = rt.fs();
    let content = SEGMENT_HEADER_LEN as u64 + shape.corpus_len();
    let segment_size =
        content.saturating_add(4096).next_power_of_two().max(1 << 20);
    let mut params = SegmentParams::new(1, 0, 7, 0);
    params.segment_size = segment_size;
    let mut writer = SegmentWriter::create(&fs, path, params)
        .expect("create golden segment");
    let payload = payload_bytes(shape.payload);
    let registry_payload = payload_bytes(24);
    let mut versions = vec![0u64; shape.writers];
    let mut registry_version = 0u64;
    let mut heads: BTreeMap<u64, ChainHead> = BTreeMap::new();
    let mut receipts = Vec::with_capacity(shape.physical_batches());
    for unit in 0..shape.owner_units {
        if shape.unit_shape == UnitShape::NewName {
            receipts.push(append_canonical_batch(
                &mut writer,
                0,
                0,
                registry_version,
                &registry_payload,
                1,
                shape.chain,
                &mut heads,
            ));
            registry_version += 1;
        }
        let writer_index = unit % shape.writers;
        let stream_id = writer_index as u64 + 1;
        let first_stream_version = versions[writer_index];
        receipts.push(append_canonical_batch(
            &mut writer,
            stream_id,
            1,
            first_stream_version,
            &payload,
            shape.events_per_batch,
            shape.chain,
            &mut heads,
        ));
        versions[writer_index] += shape.events_per_batch as u64;
    }
    writer.close().expect("close golden writer");
    let image = std::fs::read(path).expect("read golden image");
    assert_eq!(image.len() as u64, content);
    let batches = receipts
        .iter()
        .map(|receipt| {
            let start = receipt.offset as usize;
            let end = start + receipt.total_len as usize;
            image[start..end].to_vec()
        })
        .collect();
    let exit_heads =
        heads.into_iter().map(|(id, head)| (id, head.entry())).collect();
    Corpus { image, batches, receipts, exit_heads }
}

fn boundary_payloads(
    target_bytes: usize,
    physical_batches: usize,
) -> Vec<usize> {
    let fixed = HEADER_LEN + SUBFRAME_HDR_LEN + MARKER_LEN;
    let payload_budget = target_bytes
        .checked_sub(physical_batches * fixed)
        .expect("boundary target smaller than framing");
    assert!(payload_budget >= physical_batches, "payloads must be nonempty");
    let quotient = payload_budget / physical_batches;
    let remainder = payload_budget % physical_batches;
    (0..physical_batches)
        .map(|index| quotient + usize::from(index < remainder))
        .collect()
}

fn build_boundary_corpus(
    payload_lengths: &[usize],
    writers: usize,
    path: &Path,
) -> Corpus {
    assert!(!payload_lengths.is_empty());
    assert!(writers > 0);
    let corpus_len: u64 = payload_lengths
        .iter()
        .map(|payload| {
            (HEADER_LEN + SUBFRAME_HDR_LEN + payload + MARKER_LEN) as u64
        })
        .sum();
    assert!(corpus_len <= MAX_CORPUS_BYTES);
    assert!(payload_lengths.iter().all(|payload| {
        HEADER_LEN + SUBFRAME_HDR_LEN + payload + MARKER_LEN
            <= MAX_BATCH_LEN as usize
    }));
    let rt = RealRuntime::new();
    let fs = rt.fs();
    let content = SEGMENT_HEADER_LEN as u64 + corpus_len;
    let mut params = SegmentParams::new(1, 0, 7, 0);
    params.segment_size =
        content.saturating_add(4096).next_power_of_two().max(1 << 20);
    let mut writer = SegmentWriter::create(&fs, path, params)
        .expect("create boundary segment");
    let mut versions = vec![0u64; writers];
    let mut heads = BTreeMap::new();
    let mut receipts = Vec::with_capacity(payload_lengths.len());
    for (index, &payload_len) in payload_lengths.iter().enumerate() {
        let writer_index = index % writers;
        let payload = payload_bytes(payload_len);
        receipts.push(append_canonical_batch(
            &mut writer,
            writer_index as u64 + 1,
            1,
            versions[writer_index],
            &payload,
            1,
            false,
            &mut heads,
        ));
        versions[writer_index] += 1;
    }
    writer.close().expect("close boundary segment");
    let image = std::fs::read(path).expect("read boundary image");
    assert_eq!(image.len() as u64, content);
    let batches = receipts
        .iter()
        .map(|receipt| {
            let start = receipt.offset as usize;
            let end = start + receipt.total_len as usize;
            image[start..end].to_vec()
        })
        .collect();
    Corpus { image, batches, receipts, exit_heads: BTreeMap::new() }
}

fn allocate_keep_size(file: &File, len: u64) -> io::Result<()> {
    let len = i64::try_from(len).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "file too large")
    })?;
    let rc = unsafe {
        libc::fallocate(file.as_raw_fd(), libc::FALLOC_FL_KEEP_SIZE, 0, len)
    };
    if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

fn write_all_at(
    file: &File,
    mut offset: u64,
    mut bytes: &[u8],
) -> io::Result<()> {
    while !bytes.is_empty() {
        match file.write_at(bytes, offset) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => {
                offset += n as u64;
                bytes = &bytes[n..];
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn create_candidate(path: &Path, corpus: &Corpus) -> RealSink {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .expect("create candidate file");
    allocate_keep_size(&file, corpus.image.len() as u64)
        .expect("preallocate candidate");
    write_all_at(&file, 0, &corpus.image[..SEGMENT_HEADER_LEN])
        .expect("write canonical header");
    file.sync_data().expect("sync canonical header");
    RealSink::new(file)
}

fn add_stats(total: &mut WriteStats, one: WriteStats) {
    total.syscalls += one.syscalls;
    total.bytes_written += one.bytes_written;
    total.copied_bytes += one.copied_bytes;
    total.short_writes += one.short_writes;
    total.interrupted += one.interrupted;
    total.max_iovecs = total.max_iovecs.max(one.max_iovecs);
}

fn failure_with_prior(
    prior: WriteStats,
    mut failure: WriteFailure,
    completed_base: usize,
) -> WriteFailure {
    failure.stats.syscalls += prior.syscalls;
    failure.stats.bytes_written += prior.bytes_written;
    failure.stats.copied_bytes += prior.copied_bytes;
    failure.stats.short_writes += prior.short_writes;
    failure.stats.interrupted += prior.interrupted;
    failure.stats.max_iovecs = failure.stats.max_iovecs.max(prior.max_iovecs);
    failure.stats.completed_bufs += completed_base;
    failure
}

fn do_variant<'a>(
    variant: Variant,
    sink: &mut RealSink,
    offset: u64,
    buffers: &[&'a [u8]],
    iov: &mut Vec<IoSlice<'a>>,
    copy: &mut Vec<u8>,
) -> Result<WriteStats, WriteFailure> {
    match variant {
        Variant::KPwrite => k_pwrite_all(sink.file(), offset, buffers),
        Variant::Pwritev { iov: cap, bytes } => pwritev_all(
            sink,
            offset,
            buffers,
            match cap {
                IovCap::Full => runtime_iov_max(),
                IovCap::Fixed(cap) => cap,
            },
            bytes,
            iov,
        ),
        Variant::Copy => {
            copy_contiguous_all(sink.file(), offset, buffers, COPY_CAP, copy)
        }
    }
}

trait OsGroupSink {
    fn write_physical(
        &mut self,
        offset: u64,
        buffer: &[u8],
    ) -> Result<WriteStats, WriteFailure>;

    fn sync_physical(&mut self) -> io::Result<()>;
}

impl OsGroupSink for RealSink {
    fn write_physical(
        &mut self,
        offset: u64,
        buffer: &[u8],
    ) -> Result<WriteStats, WriteFailure> {
        k_pwrite_all(self.file(), offset, &[buffer])
    }

    fn sync_physical(&mut self) -> io::Result<()> {
        fdatasync_once(self.file())
    }
}

fn write_os_fallback<S: OsGroupSink>(
    sink: &mut S,
    buffers: &[&[u8]],
) -> Result<(WriteStats, u64), WriteFailure> {
    let mut stats = WriteStats::default();
    let mut barriers = 0u64;
    let mut offset = SEGMENT_HEADER_LEN as u64;
    for (index, buffer) in buffers.iter().enumerate() {
        let one = match sink.write_physical(offset, buffer) {
            Ok(one) => one,
            Err(failure) => {
                return Err(failure_with_prior(stats, failure, index))
            }
        };
        add_stats(&mut stats, one);
        stats.completed_bufs = index + 1;
        stats.partial_buf_byte = 0;
        offset = offset.checked_add(buffer.len() as u64).ok_or_else(|| {
            WriteFailure {
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "positioned offset overflow",
                ),
                stats,
            }
        })?;
        sink.sync_physical()
            .map_err(|source| WriteFailure { source, stats })?;
        barriers += 1;
    }
    Ok((stats, barriers))
}

fn write_group<'a>(
    requested: Variant,
    mode: Mode,
    sink: &mut RealSink,
    buffers: &[&'a [u8]],
    iov: &mut Vec<IoSlice<'a>>,
    copy: &mut Vec<u8>,
) -> Result<(WriteStats, u64), WriteFailure> {
    let mut stats = WriteStats::default();
    let mut barriers = 0u64;
    if mode == Mode::Os {
        // Mandatory semantic fallback: one physical batch, one write, one
        // barrier. The requested candidate cannot gather across this cut.
        return write_os_fallback(sink, buffers)
    } else {
        let one = do_variant(
            requested,
            sink,
            SEGMENT_HEADER_LEN as u64,
            buffers,
            iov,
            copy,
        )?;
        add_stats(&mut stats, one);
        stats.completed_bufs = one.completed_bufs;
        stats.partial_buf_byte = one.partial_buf_byte;
        if mode == Mode::Group {
            fdatasync_once(sink.file())
                .map_err(|source| WriteFailure { source, stats })?;
            barriers = 1;
        }
    }
    Ok((stats, barriers))
}

fn verify_receipts(corpus: &Corpus, candidate: &[u8], chain: bool) {
    assert_eq!(candidate, corpus.image, "candidate bytes differ from golden");
    let recovery = scan_image(candidate, None);
    assert_eq!(recovery.accepted.len(), corpus.receipts.len());
    assert_eq!(recovery.safe_offset as usize, candidate.len());
    for (accepted, receipt) in recovery.accepted.iter().zip(&corpus.receipts) {
        assert_receipt(accepted, receipt);
    }
    if chain {
        let mut heads: BTreeMap<u64, ChainHead> = BTreeMap::new();
        for batch in &recovery.accepted {
            let start = batch.offset as usize;
            let entry: [u8; CHAIN_LEN] = candidate
                [start + HEADER_LEN..start + HEADER_LEN + CHAIN_LEN]
                .try_into()
                .unwrap();
            let head = heads
                .entry(batch.stream_id)
                .or_insert_with(|| ChainHead::genesis(batch.stream_id));
            assert_eq!(head.entry(), entry, "stored chain entry diverged");
            head.absorb_batch(
                batch
                    .frames(candidate)
                    .expect("accepted batch matches image")
                    .map(|frame| frame.payload),
            );
        }
        let actual: BTreeMap<_, _> =
            heads.into_iter().map(|(id, head)| (id, head.entry())).collect();
        assert_eq!(actual, corpus.exit_heads, "exit chain heads diverged");
    }
}

fn assert_receipt(accepted: &AcceptedBatch, receipt: &Receipt) {
    assert_eq!(accepted.batch_id, receipt.batch_id);
    assert_eq!(accepted.first_global_pos, receipt.first_global_pos);
    assert_eq!(accepted.frame_count, receipt.frame_count);
    assert_eq!(accepted.total_len, receipt.total_len);
    assert_eq!(accepted.offset, receipt.offset);
}

fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    let rc =
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    assert_eq!(rc, 0);
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn percentile(sorted: &[u64], q: f64) -> u64 {
    let index = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn load1() -> f64 {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| s.split_whitespace().next().map(str::to_owned))
        .and_then(|s| s.parse().ok())
        .unwrap_or(-1.0)
}

/// Measure only the prepared-write pipeline: producer encode, owner stamping,
/// CRC recomputation, and a counting positioned write. Admission, scheduling,
/// publication, cancellation, and reply delivery remain outside this isolated
/// spike.
fn run_preparation_point(shape: Shape, min_bytes: u64, min_time: Duration) {
    shape.validate();
    let fs = CountingFs;
    let mut params = SegmentParams::new(1, 0, 7, 0);
    params.segment_size = SEGMENT_HEADER_LEN as u64 + MAX_CORPUS_BYTES;
    let mut writer = SegmentWriter::create(&fs, Path::new("counting"), params)
        .expect("create counting writer");
    let payload = payload_bytes(shape.payload);
    let registry_payload = payload_bytes(24);
    let mut versions = vec![0u64; shape.writers];
    let mut registry_version = 0u64;
    let mut heads: BTreeMap<u64, ChainHead> = BTreeMap::new();
    let mut latencies = Vec::new();
    let mut iterations = 0u64;
    let mut prepared_bytes = 0u64;
    let stop_reason;
    let load_before = load1();
    let cpu_start = thread_cpu_ns();
    let wall_start = Instant::now();
    loop {
        let group_start = Instant::now();
        for unit in 0..shape.owner_units {
            if shape.unit_shape == UnitShape::NewName {
                let _ = append_canonical_batch(
                    &mut writer,
                    0,
                    0,
                    registry_version,
                    &registry_payload,
                    1,
                    shape.chain,
                    &mut heads,
                );
                registry_version += 1;
            }
            let writer_index = unit % shape.writers;
            let _ = append_canonical_batch(
                &mut writer,
                writer_index as u64 + 1,
                1,
                versions[writer_index],
                &payload,
                shape.events_per_batch,
                shape.chain,
                &mut heads,
            );
            versions[writer_index] += shape.events_per_batch as u64;
        }
        latencies.push(group_start.elapsed().as_nanos() as u64);
        iterations += 1;
        prepared_bytes += shape.corpus_len();
        if prepared_bytes >= min_bytes {
            stop_reason = "bytes";
            break;
        }
        if wall_start.elapsed() >= min_time {
            stop_reason = "time";
            break;
        }
        if prepared_bytes.saturating_add(shape.corpus_len()) > MAX_CORPUS_BYTES
        {
            stop_reason = "cap";
            break;
        }
    }
    let wall_ns = wall_start.elapsed().as_nanos() as u64;
    let cpu_ns = thread_cpu_ns() - cpu_start;
    let load_after = load1();
    latencies.sort_unstable();
    let events = shape.events() * iterations;
    let row = format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.3},{:.3}",
        shape.payload,
        shape.events_per_batch,
        shape.owner_units,
        shape.physical_batches(),
        shape.writers,
        u8::from(shape.chain),
        shape.unit_shape.name(),
        iterations,
        stop_reason,
        events,
        shape.domain_events() * iterations,
        prepared_bytes,
        cpu_ns,
        wall_ns,
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.95),
        percentile(&latencies, 0.99),
        shape.physical_batches() as u64 * iterations,
        load_before,
        load_after,
    );
    const HEADER: &str = "payload,events_per_batch,owner_units,\
                          physical_batches,writers,chain,unit_shape,\
                          iterations,stop_reason,events,domain_events,bytes,\
                          cpu_ns,wall_ns,p50_ns,p95_ns,p99_ns,\
                          prepared_batches,load1_before,load1_after";
    println!("{HEADER}\n{row}");
}

fn run_point(
    variant: Variant,
    mode: Mode,
    shape: Shape,
    limit: RunLimit,
    min_bytes: u64,
    min_time: Duration,
    output: Option<&Path>,
) {
    if let RunLimit::Fixed(iterations) = limit {
        assert!(iterations > 0);
    }
    let dir = scratch("pwritev-point");
    let corpus = build_corpus(shape, &dir.path().join("golden.log"));
    let path = dir.path().join(format!("{}.log", variant.name()));
    let mut sink = create_candidate(&path, &corpus);
    let buffers: Vec<&[u8]> =
        corpus.batches.iter().map(Vec::as_slice).collect();
    let mut iov = Vec::with_capacity(runtime_iov_max());
    let mut copy = Vec::with_capacity(COPY_CAP.min(corpus.image.len()));
    // Untimed warm-up writes the exact same byte ranges and policy.
    write_group(variant, mode, &mut sink, &buffers, &mut iov, &mut copy)
        .expect("warm-up group");
    let group_bytes = shape.corpus_len();
    let expected_auto_iterations =
        min_bytes.div_ceil(group_bytes).max(1) as usize;
    let mut latencies = Vec::with_capacity(match limit {
        RunLimit::Fixed(iterations) => iterations,
        RunLimit::FirstThreshold => expected_auto_iterations,
    });
    let mut total = WriteStats::default();
    let mut barriers = 0u64;
    let mut iterations = 0usize;
    let stop_reason;
    let load_before = load1();
    let cpu_start = thread_cpu_ns();
    let wall_start = Instant::now();
    loop {
        let group_start = Instant::now();
        let (stats, group_barriers) = write_group(
            variant, mode, &mut sink, &buffers, &mut iov, &mut copy,
        )
        .expect("measured group");
        latencies.push(group_start.elapsed().as_nanos() as u64);
        add_stats(&mut total, stats);
        barriers += group_barriers;
        iterations += 1;
        match limit {
            RunLimit::Fixed(wanted) if iterations >= wanted => {
                stop_reason = "fixed";
                break;
            }
            RunLimit::Fixed(_) => {}
            RunLimit::FirstThreshold => {
                if total.bytes_written >= min_bytes {
                    stop_reason = "bytes";
                    break;
                }
                if wall_start.elapsed() >= min_time {
                    stop_reason = "time";
                    break;
                }
                if total.bytes_written.saturating_add(group_bytes)
                    > MAX_CORPUS_BYTES
                {
                    stop_reason = "cap";
                    break;
                }
            }
        }
    }
    let wall_ns = wall_start.elapsed().as_nanos() as u64;
    let cpu_ns = thread_cpu_ns() - cpu_start;
    let load_after = load1();
    drop(sink);
    let candidate = std::fs::read(&path).expect("read candidate image");
    verify_receipts(&corpus, &candidate, shape.chain);
    latencies.sort_unstable();
    let events = shape.events() * iterations as u64;
    let row = format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},\
         {},{},{:.3},{:.3}",
        variant.name(),
        mode.name(),
        shape.payload,
        shape.events_per_batch,
        shape.owner_units,
        shape.physical_batches(),
        shape.writers,
        u8::from(shape.chain),
        shape.unit_shape.name(),
        iterations,
        stop_reason,
        events,
        shape.domain_events() * iterations as u64,
        total.bytes_written,
        total.syscalls,
        total.short_writes,
        total.interrupted,
        total.max_iovecs,
        total.copied_bytes,
        barriers,
        cpu_ns,
        wall_ns,
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.95),
        percentile(&latencies, 0.99),
        load_before,
        load_after,
    );
    const HEADER: &str = "variant,mode,payload,events_per_batch,owner_units,\
                          physical_batches,writers,chain,unit_shape,\
                          iterations,stop_reason,events,domain_events,bytes,\
                          write_syscalls,short_writes,interrupted,max_iovecs,\
                          copied_bytes,barriers,cpu_ns,wall_ns,p50_ns,p95_ns,\
                          p99_ns,load1_before,load1_after";
    println!("{HEADER}\n{row}");
    if let Some(path) = output {
        let fresh = !path.exists();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("open output CSV");
        if fresh {
            writeln!(file, "{HEADER}").unwrap();
        }
        writeln!(file, "{row}").unwrap();
    }
}

const TRACE_FIELDS: &str =
    "policy,mode,payload,events_per_batch,writers,unit_shape,trace_histogram,\
     trace_groups,trace_owner_intents,trace_physical_batches,cycles,\
     stop_reason,events,domain_events,bytes,write_syscalls,short_writes,\
     interrupted,max_iovecs,copied_bytes,barriers,cpu_ns,wall_ns,p50_ns,\
     p95_ns,p99_ns,load1_before,load1_after";

fn run_trace_point(
    policy: TracePolicy,
    mode: Mode,
    payload: usize,
    events_per_batch: usize,
    writers: usize,
    histogram: TraceHistogram,
    chain: bool,
) {
    let shape = Shape {
        payload,
        events_per_batch,
        owner_units: histogram.max_width,
        writers,
        chain,
        unit_shape: UnitShape::Domain,
    };
    shape.validate();
    let batch_bytes = shape.domain_batch_len();
    let cycle_bytes = batch_bytes
        .checked_mul(histogram.owner_intents as u64)
        .expect("trace cycle byte overflow");
    assert!(
        cycle_bytes <= MAX_CORPUS_BYTES,
        "one complete trace cycle exceeds 2 GiB safety cap"
    );
    let dir = scratch("pwritev-trace");
    let corpus = build_corpus(shape, &dir.path().join("golden.log"));
    let path = dir.path().join("candidate.log");
    let mut sink = create_candidate(&path, &corpus);
    let buffers: Vec<&[u8]> =
        corpus.batches.iter().map(Vec::as_slice).collect();
    let mut iov = Vec::with_capacity(runtime_iov_max());
    let mut copy = Vec::with_capacity(COPY_CAP.min(corpus.image.len()));

    // One complete untimed cycle warms every frozen policy branch. Sorted
    // expansion leaves a max-width group last, so both warm-up and every
    // observation cycle end with an exact full-corpus image.
    for &width in &histogram.widths {
        let group_bytes = usize::try_from(batch_bytes)
            .expect("batch bytes fit usize")
            .checked_mul(width)
            .expect("trace group byte overflow");
        let variant = policy.variant_for(width, group_bytes);
        write_group(
            variant,
            mode,
            &mut sink,
            &buffers[..width],
            &mut iov,
            &mut copy,
        )
        .expect("warm-up trace group");
    }

    let mut latencies = Vec::new();
    let mut total = WriteStats::default();
    let mut barriers = 0u64;
    let mut cycles = 0u64;
    let stop_reason;
    let load_before = load1();
    let cpu_start = thread_cpu_ns();
    let wall_start = Instant::now();
    loop {
        for &width in &histogram.widths {
            let group_bytes = usize::try_from(batch_bytes)
                .expect("batch bytes fit usize")
                .checked_mul(width)
                .expect("trace group byte overflow");
            let variant = policy.variant_for(width, group_bytes);
            let group_start = Instant::now();
            let (stats, group_barriers) = write_group(
                variant,
                mode,
                &mut sink,
                &buffers[..width],
                &mut iov,
                &mut copy,
            )
            .expect("measured trace group");
            latencies.push(group_start.elapsed().as_nanos() as u64);
            add_stats(&mut total, stats);
            barriers += group_barriers;
        }
        cycles += 1;

        // A trace observation is indivisible: stopping is evaluated only
        // after replaying the complete histogram, never mid-cycle.
        if total.bytes_written >= CONFIRM_MIN_BYTES {
            stop_reason = "bytes";
            break;
        }
        if wall_start.elapsed() >= CONFIRM_MIN_TIME {
            stop_reason = "time";
            break;
        }
        if total.bytes_written.saturating_add(cycle_bytes) > MAX_CORPUS_BYTES {
            stop_reason = "cap";
            break;
        }
    }
    let wall_ns = wall_start.elapsed().as_nanos() as u64;
    let cpu_ns = thread_cpu_ns() - cpu_start;
    let load_after = load1();
    drop(sink);
    let candidate = std::fs::read(&path).expect("read trace candidate");
    verify_receipts(&corpus, &candidate, chain);
    latencies.sort_unstable();
    let trace_groups = histogram.widths.len() as u64;
    let trace_owner_intents = histogram.owner_intents as u64;
    let domain_events = trace_owner_intents
        .checked_mul(events_per_batch as u64)
        .and_then(|events| events.checked_mul(cycles))
        .expect("trace event count overflow");
    let row = format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},\
         {},{},{},{:.3},{:.3}",
        policy.name,
        mode.name(),
        payload,
        events_per_batch,
        writers,
        UnitShape::Domain.name(),
        histogram.canonical,
        trace_groups,
        trace_owner_intents,
        trace_owner_intents,
        cycles,
        stop_reason,
        domain_events,
        domain_events,
        total.bytes_written,
        total.syscalls,
        total.short_writes,
        total.interrupted,
        total.max_iovecs,
        total.copied_bytes,
        barriers,
        cpu_ns,
        wall_ns,
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.95),
        percentile(&latencies, 0.99),
        load_before,
        load_after,
    );
    println!("{TRACE_FIELDS}\n{row}");
}

fn run_boundary_point(
    variant: Variant,
    mode: Mode,
    target_bytes: usize,
    physical_batches: usize,
    writers: usize,
) {
    let payloads = boundary_payloads(target_bytes, physical_batches);
    let dir = scratch("pwritev-boundary");
    let corpus = build_boundary_corpus(
        &payloads,
        writers,
        &dir.path().join("golden.log"),
    );
    assert_eq!(
        corpus.batches.iter().map(Vec::len).sum::<usize>(),
        target_bytes
    );
    let path = dir.path().join(format!("{}.log", variant.name()));
    let mut sink = create_candidate(&path, &corpus);
    let buffers: Vec<&[u8]> =
        corpus.batches.iter().map(Vec::as_slice).collect();
    let mut iov = Vec::with_capacity(runtime_iov_max());
    let mut copy = Vec::with_capacity(COPY_CAP.min(corpus.image.len()));
    write_group(variant, mode, &mut sink, &buffers, &mut iov, &mut copy)
        .expect("warm-up boundary group");
    let mut latencies = Vec::new();
    let mut total = WriteStats::default();
    let mut barriers = 0u64;
    let mut iterations = 0u64;
    let stop_reason;
    let load_before = load1();
    let cpu_start = thread_cpu_ns();
    let wall_start = Instant::now();
    loop {
        let group_start = Instant::now();
        let (stats, group_barriers) = write_group(
            variant, mode, &mut sink, &buffers, &mut iov, &mut copy,
        )
        .expect("measured boundary group");
        latencies.push(group_start.elapsed().as_nanos() as u64);
        add_stats(&mut total, stats);
        barriers += group_barriers;
        iterations += 1;
        if total.bytes_written >= BOUNDARY_MIN_BYTES {
            stop_reason = "bytes";
            break;
        }
        if wall_start.elapsed() >= BOUNDARY_MIN_TIME {
            stop_reason = "time";
            break;
        }
        if total.bytes_written.saturating_add(target_bytes as u64)
            > MAX_CORPUS_BYTES
        {
            stop_reason = "cap";
            break;
        }
    }
    let wall_ns = wall_start.elapsed().as_nanos() as u64;
    let cpu_ns = thread_cpu_ns() - cpu_start;
    let load_after = load1();
    drop(sink);
    let candidate = std::fs::read(&path).expect("read boundary candidate");
    verify_receipts(&corpus, &candidate, false);
    latencies.sort_unstable();
    let events = physical_batches as u64 * iterations;
    let row = format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.\
         3},{:.3}",
        variant.name(),
        mode.name(),
        target_bytes,
        physical_batches,
        writers,
        iterations,
        stop_reason,
        events,
        total.bytes_written,
        total.syscalls,
        total.short_writes,
        total.interrupted,
        total.max_iovecs,
        total.copied_bytes,
        barriers,
        cpu_ns,
        wall_ns,
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.95),
        percentile(&latencies, 0.99),
        payloads.iter().min().unwrap(),
        payloads.iter().max().unwrap(),
        load_before,
        load_after,
    );
    const HEADER: &str = "variant,mode,target_bytes,physical_batches,writers,\
                          iterations,stop_reason,events,bytes,write_syscalls,\
                          short_writes,interrupted,max_iovecs,copied_bytes,\
                          barriers,cpu_ns,wall_ns,p50_ns,p95_ns,p99_ns,\
                          min_payload,max_payload,load1_before,load1_after";
    println!("{HEADER}\n{row}");
}

fn verify() {
    for (chain, unit_shape) in [
        (false, UnitShape::Domain),
        (true, UnitShape::Domain),
        (false, UnitShape::NewName),
        (true, UnitShape::NewName),
    ] {
        let shape = Shape {
            payload: 250,
            events_per_batch: 10,
            owner_units: 17,
            writers: 4,
            chain,
            unit_shape,
        };
        let dir = scratch("pwritev-verify");
        let corpus = build_corpus(shape, &dir.path().join("golden.log"));
        let buffers: Vec<&[u8]> =
            corpus.batches.iter().map(Vec::as_slice).collect();
        for mode in [Mode::Process, Mode::Os, Mode::Group] {
            for variant in Variant::correctness_variants() {
                let path = dir.path().join(format!(
                    "{}-{}-{}.log",
                    variant.name(),
                    mode.name(),
                    u8::from(chain)
                ));
                let mut sink = create_candidate(&path, &corpus);
                let mut iov = Vec::with_capacity(runtime_iov_max());
                let mut copy =
                    Vec::with_capacity(COPY_CAP.min(corpus.image.len()));
                let (stats, barriers) = write_group(
                    variant, mode, &mut sink, &buffers, &mut iov, &mut copy,
                )
                .expect("verify write");
                let expected_barriers = match mode {
                    Mode::Process => 0,
                    Mode::Os => shape.physical_batches() as u64,
                    Mode::Group => 1,
                };
                assert_eq!(barriers, expected_barriers);
                let expected_syscalls = match (mode, variant) {
                    (Mode::Os, _) | (_, Variant::KPwrite) => {
                        shape.physical_batches() as u64
                    }
                    _ => 1,
                };
                assert_eq!(
                    stats.syscalls, expected_syscalls,
                    "unexpected syscall/fallback shape for {mode:?} \
                     {variant:?}",
                );
                drop(sink);
                let candidate = std::fs::read(path).expect("read verify image");
                verify_receipts(&corpus, &candidate, chain);
            }
        }
    }
    println!(
        "PASS: exact bytes, receipts, CRC/scanner positions, chain heads, and \
         Process/Os/Group barrier counts"
    );
}

fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => {
                use std::fmt::Write as _;
                write!(out, "\\u{:04x}", ch as u32).unwrap();
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn owner_barrier_case(mode: Mode, expected: u64) {
    let shape = Shape {
        payload:          24,
        events_per_batch: 1,
        owner_units:      4,
        writers:          4,
        chain:            false,
        unit_shape:       UnitShape::Domain,
    };
    let dir = scratch("owner-barrier-case");
    let corpus = build_corpus(shape, &dir.path().join("golden.log"));
    let path = dir.path().join("candidate.log");
    let mut sink = create_candidate(&path, &corpus);
    let buffers: Vec<&[u8]> =
        corpus.batches.iter().map(Vec::as_slice).collect();
    let mut iov = Vec::new();
    let mut copy = Vec::new();
    let (_, barriers) = write_group(
        Variant::Pwritev { iov: IovCap::Full, bytes: BYTE_CAP },
        mode,
        &mut sink,
        &buffers,
        &mut iov,
        &mut copy,
    )
    .expect("owner barrier write");
    assert_eq!(barriers, expected);
    drop(sink);
    verify_receipts(
        &corpus,
        &std::fs::read(path).expect("read barrier candidate"),
        false,
    );
}

fn owner_os_prior_progress_failure_case() {
    struct FaultingOsSink {
        write_calls: usize,
        sync_calls:  usize,
        offsets:     Vec<u64>,
    }

    impl OsGroupSink for FaultingOsSink {
        fn write_physical(
            &mut self,
            offset: u64,
            buffer: &[u8],
        ) -> Result<WriteStats, WriteFailure> {
            self.offsets.push(offset);
            self.write_calls += 1;
            if self.write_calls <= 2 {
                return Ok(WriteStats {
                    syscalls: 1,
                    bytes_written: buffer.len() as u64,
                    completed_bufs: 1,
                    ..WriteStats::default()
                })
            }
            Err(WriteFailure {
                source: io::Error::from_raw_os_error(libc::EIO),
                stats:  WriteStats {
                    syscalls: 2,
                    bytes_written: 3,
                    short_writes: 1,
                    completed_bufs: 0,
                    partial_buf_byte: 3,
                    ..WriteStats::default()
                },
            })
        }

        fn sync_physical(&mut self) -> io::Result<()> {
            self.sync_calls += 1;
            Ok(())
        }
    }

    let buffers = [b"abc".as_slice(), b"defg".as_slice(), b"hijkl".as_slice()];
    let mut sink = FaultingOsSink {
        write_calls: 0,
        sync_calls:  0,
        offsets:     Vec::new(),
    };
    let merged = write_os_fallback(&mut sink, &buffers)
        .expect_err("third physical batch must fail");
    assert_eq!(merged.source.raw_os_error(), Some(libc::EIO));
    assert_eq!(merged.stats.syscalls, 4);
    assert_eq!(merged.stats.bytes_written, 10);
    assert_eq!(merged.stats.short_writes, 1);
    assert_eq!(merged.stats.completed_bufs, 2);
    assert_eq!(merged.stats.partial_buf_byte, 3);
    assert_eq!(sink.write_calls, 3, "terminal write was retried");
    assert_eq!(sink.sync_calls, 2, "failed batch was synced");
    assert_eq!(
        sink.offsets,
        [
            SEGMENT_HEADER_LEN as u64,
            SEGMENT_HEADER_LEN as u64 + 3,
            SEGMENT_HEADER_LEN as u64 + 7,
        ]
    );
}

fn owner_roll_new_fault_old_prefix_case() {
    let fs = SimFs::new(Fault::Tail);
    let old_path = Path::new("/roll-old.log");
    let new_path = Path::new("/roll-new.log");
    let mut params = SegmentParams::new(1, 0, 7, 0);
    params.segment_size = 1 << 20;
    let mut writer = SegmentWriter::create(&fs, old_path, params)
        .expect("create old segment");
    let payload = payload_bytes(24);
    let mut heads = BTreeMap::new();
    let receipt = append_canonical_batch(
        &mut writer,
        1,
        1,
        0,
        &payload,
        1,
        false,
        &mut heads,
    );
    writer.sync().expect("sync old prefix");
    let before = writer.summary();
    fs.inject_enospc(new_path, EnospcSite::Allocate);
    let failure = match writer.open_next(new_path, 2, 8, 1) {
        Ok(_) => panic!("new segment allocation unexpectedly succeeded"),
        Err(failure) => failure,
    };
    assert!(matches!(failure, WriteError::StoreFull { .. }));
    assert_eq!(writer.summary(), before, "old writer changed on new fault");
    assert!(
        fs.open(new_path, OpenOpts::read_only()).is_err(),
        "failed new-segment husk survived"
    );
    let old = fs.open(old_path, OpenOpts::read_only()).expect("reopen old");
    let mut image = vec![0; old.len().expect("old length") as usize];
    let read = old.pread(0, &mut image).expect("read old prefix");
    image.truncate(read);
    let recovery = scan_image(&image, None);
    assert_eq!(recovery.accepted.len(), 1);
    assert_eq!(recovery.safe_offset, receipt.offset + receipt.total_len);
    assert_receipt(&recovery.accepted[0], &receipt);
}

fn owner_recovery_partial_prefix_case() {
    let shape = Shape {
        payload:          250,
        events_per_batch: 10,
        owner_units:      4,
        writers:          4,
        chain:            false,
        unit_shape:       UnitShape::Domain,
    };
    let dir = scratch("recovery-partial-prefix");
    let corpus = build_corpus(shape, &dir.path().join("golden.log"));
    let buffers: Vec<&[u8]> =
        corpus.batches.iter().map(Vec::as_slice).collect();
    let current_path = dir.path().join("current.log");
    let candidate_path = dir.path().join("candidate.log");
    let mut current = create_candidate(&current_path, &corpus);
    let mut candidate = create_candidate(&candidate_path, &corpus);
    let mut current_iov = Vec::new();
    let mut current_copy = Vec::new();
    let mut candidate_iov = Vec::new();
    let mut candidate_copy = Vec::new();
    write_group(
        Variant::KPwrite,
        Mode::Process,
        &mut current,
        &buffers,
        &mut current_iov,
        &mut current_copy,
    )
    .expect("current full write");
    write_group(
        Variant::Pwritev { iov: IovCap::Full, bytes: BYTE_CAP },
        Mode::Process,
        &mut candidate,
        &buffers,
        &mut candidate_iov,
        &mut candidate_copy,
    )
    .expect("candidate full write");
    let partial = &corpus.receipts[2];
    let cut = partial.offset + partial.total_len / 2;
    current.into_file().set_len(cut).expect("truncate current prefix");
    candidate.into_file().set_len(cut).expect("truncate candidate prefix");
    let current_image =
        std::fs::read(&current_path).expect("read current prefix");
    let candidate_image =
        std::fs::read(&candidate_path).expect("read candidate prefix");
    assert_eq!(current_image, candidate_image, "visible bytes diverged");
    let current_recovery = scan_image(&current_image, None);
    let candidate_recovery = scan_image(&candidate_image, None);
    assert_eq!(current_recovery.safe_offset, candidate_recovery.safe_offset);
    assert_eq!(current_recovery.accepted.len(), 2);
    assert_eq!(candidate_recovery.accepted.len(), 2);
    for ((current_batch, candidate_batch), receipt) in current_recovery
        .accepted
        .iter()
        .zip(&candidate_recovery.accepted)
        .zip(&corpus.receipts)
    {
        assert_receipt(current_batch, receipt);
        assert_receipt(candidate_batch, receipt);
    }
}

fn owner_enospc_poison_case() {
    let fs = SimFs::new(Fault::Tail);
    let path = Path::new("/owner-enospc.log");
    let mut params = SegmentParams::new(1, 0, 7, 0);
    params.segment_size = 1 << 20;
    let mut writer =
        SegmentWriter::create(&fs, path, params).expect("create sim writer");
    fs.inject_enospc(path, EnospcSite::Fdatasync);
    let first = writer.sync().expect_err("injected fdatasync ENOSPC");
    assert_eq!(first.raw_os_error(), Some(libc::ENOSPC));
    assert!(writer.is_poisoned());
    writer.sync().expect_err("poisoned writer must fail subsequent sync");
}

fn correctness_json(path: &Path, binary_sha256: &str) {
    let kernel = same_binary_correctness_report();
    let mut cases: Vec<(String, &'static str, &'static str, String)> = kernel
        .cases
        .into_iter()
        .map(|case| {
            (
                case.name.to_owned(),
                "kernel_harness",
                case.status.as_str(),
                case.details,
            )
        })
        .collect();
    let owner_checks: [(&str, fn()); 8] = [
        ("owner.production_layout_receipts_crc_chain", verify),
        ("owner.barrier.process_zero", || owner_barrier_case(Mode::Process, 0)),
        ("owner.barrier.group_one", || owner_barrier_case(Mode::Group, 1)),
        ("owner.barrier.os_per_batch", || owner_barrier_case(Mode::Os, 4)),
        (
            "owner.os_fallback.failure.prior_progress_exact_cursor_no_retry",
            owner_os_prior_progress_failure_case,
        ),
        (
            "owner.roll.new_allocate_fault_old_prefix_preserved",
            owner_roll_new_fault_old_prefix_case,
        ),
        (
            "owner.recovery.current_candidate_partial_prefix_equal",
            owner_recovery_partial_prefix_case,
        ),
        ("owner.fdatasync.enospc_sticky_poison", owner_enospc_poison_case),
    ];
    for (name, check) in owner_checks {
        let result = std::panic::catch_unwind(check);
        cases.push((
            name.to_owned(),
            "owner_seam_harness",
            if result.is_ok() { "pass" } else { "fail" },
            if result.is_ok() {
                "executable owner-seam invariant passed".to_owned()
            } else {
                "owner-seam invariant panicked".to_owned()
            },
        ));
    }
    let all_passed = cases.iter().all(|case| case.2 == "pass");
    let required = cases
        .iter()
        .map(|case| json_string(&case.0))
        .collect::<Vec<_>>()
        .join(",");
    let serialized = cases
        .iter()
        .map(|(name, scope, status, details)| {
            format!(
                "{{\"name\":{},\"scope\":{},\"status\":{},\"details\":{}}}",
                json_string(name),
                json_string(scope),
                json_string(status),
                json_string(details),
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let document = format!(
        "{{\"schema\":\"pwritev_group.correctness.v1\",\"binary_sha256\":{},\"\
         all_passed\":{},\"out_of_scope\":[\"production owner cancellation \
         before/after admission requires engine \
         integration\"],\"integration_prerequisites\":[\"execute cancellation \
         semantics through the production \
         owner\"],\"required_cases\":[{}],\"cases\":[{}]}}\n",
        json_string(binary_sha256),
        all_passed,
        required,
        serialized,
    );
    std::fs::write(path, document).expect("write correctness artifact");
    assert!(all_passed, "same-binary correctness case failed");
}

fn contract_json() {
    assert_eq!((VERIFY_ARGS, CONTRACT_ARGS, CORRECTNESS_ARGS), (2, 2, 4));
    assert_eq!((POINT_AUTO_MIN_ARGS, POINT_AUTO_MAX_ARGS), (9, 11));
    assert_eq!((PREP_AUTO_MIN_ARGS, PREP_AUTO_MAX_ARGS), (7, 8));
    assert_eq!(BOUNDARY_AUTO_ARGS, 7);
    assert_eq!((TRACE_MIN_ARGS, TRACE_MAX_ARGS), (8, 9));
    assert_eq!(EXPLORE_MIN_BYTES, 33_554_432);
    assert_eq!(EXPLORE_MIN_TIME, Duration::from_millis(250));
    assert_eq!(BOUNDARY_MIN_BYTES, 67_108_864);
    assert_eq!(BOUNDARY_MIN_TIME, Duration::from_millis(500));
    assert_eq!(CONFIRM_MIN_BYTES, 134_217_728);
    assert_eq!(CONFIRM_MIN_TIME, Duration::from_millis(750));
    println!(
        "{}",
        r#"{"schema":"pwritev_group.contract.v1","argv_lengths":{"contract-json":[2],"correctness-json":[4],"verify":[2],"point-auto":[9,10,11],"prep-auto":[7,8],"boundary-auto":[7],"point-trace":[8,9]},"thresholds":{"point-auto":{"bytes":33554432,"millis":250},"prep-auto":{"bytes":33554432,"millis":250},"boundary-auto":{"bytes":67108864,"millis":500},"point-trace":{"bytes":134217728,"millis":750}},"fields":{"point-auto":["variant","mode","payload","events_per_batch","owner_units","physical_batches","writers","chain","unit_shape","iterations","stop_reason","events","domain_events","bytes","write_syscalls","short_writes","interrupted","max_iovecs","copied_bytes","barriers","cpu_ns","wall_ns","p50_ns","p95_ns","p99_ns","load1_before","load1_after"],"prep-auto":["payload","events_per_batch","owner_units","physical_batches","writers","chain","unit_shape","iterations","stop_reason","events","domain_events","bytes","cpu_ns","wall_ns","p50_ns","p95_ns","p99_ns","prepared_batches","load1_before","load1_after"],"boundary-auto":["variant","mode","target_bytes","physical_batches","writers","iterations","stop_reason","events","bytes","write_syscalls","short_writes","interrupted","max_iovecs","copied_bytes","barriers","cpu_ns","wall_ns","p50_ns","p95_ns","p99_ns","min_payload","max_payload","load1_before","load1_after"],"point-trace":["policy","mode","payload","events_per_batch","writers","unit_shape","trace_histogram","trace_groups","trace_owner_intents","trace_physical_batches","cycles","stop_reason","events","domain_events","bytes","write_syscalls","short_writes","interrupted","max_iovecs","copied_bytes","barriers","cpu_ns","wall_ns","p50_ns","p95_ns","p99_ns","load1_before","load1_after"]},"correctness_schema":"pwritev_group.correctness.v1"}"#
    );
}

fn usage() -> ! {
    eprintln!(
        "usage:\n  pwritev_group verify\n  pwritev_group contract-json\n  \
         pwritev_group correctness-json <output.json> <binary_sha256>\n  \
         pwritev_group point <variant> <mode> <payload> <events_per_batch> \
         <physical_batches> <writers> <iterations> [chain=0|1] [output.csv]\n  \
         pwritev_group point-auto <variant> <mode> <payload> \
         <events_per_batch> <owner_units> <writers> \
         <unit_shape=domain|new_name> [chain=0|1] [output.csv]\n  \
         pwritev_group prep-auto <payload> <events_per_batch> <owner_units> \
         <writers> <unit_shape=domain|new_name> [chain=0|1]\n  pwritev_group \
         boundary-auto <variant> <mode> <target_bytes> <physical_batches> \
         <writers>\n  pwritev_group point-trace <policy> <mode> <payload> \
         <events_per_batch> <writers> <width:count[;width:count...]> \
         [chain=0|1]"
    );
    std::process::exit(2)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("verify") if args.len() == VERIFY_ARGS => verify(),
        Some("contract-json") if args.len() == CONTRACT_ARGS => contract_json(),
        Some("correctness-json") if args.len() == CORRECTNESS_ARGS => {
            correctness_json(Path::new(&args[2]), &args[3]);
        }
        Some("point") if (9..=11).contains(&args.len()) => {
            let variant = Variant::parse(&args[2]);
            let mode = Mode::parse(&args[3]);
            let parse = |i: usize| args[i].parse::<usize>().unwrap();
            let shape = Shape {
                payload:          parse(4),
                events_per_batch: parse(5),
                owner_units:      parse(6),
                writers:          parse(7),
                chain:            args.get(9).is_some_and(|s| s == "1"),
                unit_shape:       UnitShape::Domain,
            };
            let iterations = parse(8);
            let output = args.get(10).map(PathBuf::from);
            run_point(
                variant,
                mode,
                shape,
                RunLimit::Fixed(iterations),
                EXPLORE_MIN_BYTES,
                EXPLORE_MIN_TIME,
                output.as_deref(),
            );
        }
        Some("point-auto")
            if (POINT_AUTO_MIN_ARGS..=POINT_AUTO_MAX_ARGS)
                .contains(&args.len()) =>
        {
            let variant = Variant::parse(&args[2]);
            let mode = Mode::parse(&args[3]);
            let parse = |i: usize| args[i].parse::<usize>().unwrap();
            let shape = Shape {
                payload:          parse(4),
                events_per_batch: parse(5),
                owner_units:      parse(6),
                writers:          parse(7),
                chain:            args.get(9).is_some_and(|s| s == "1"),
                unit_shape:       UnitShape::parse(&args[8]),
            };
            let output = args.get(10).map(PathBuf::from);
            run_point(
                variant,
                mode,
                shape,
                RunLimit::FirstThreshold,
                EXPLORE_MIN_BYTES,
                EXPLORE_MIN_TIME,
                output.as_deref(),
            );
        }
        Some("prep-auto")
            if (PREP_AUTO_MIN_ARGS..=PREP_AUTO_MAX_ARGS)
                .contains(&args.len()) =>
        {
            let parse = |i: usize| args[i].parse::<usize>().unwrap();
            run_preparation_point(
                Shape {
                    payload:          parse(2),
                    events_per_batch: parse(3),
                    owner_units:      parse(4),
                    writers:          parse(5),
                    unit_shape:       UnitShape::parse(&args[6]),
                    chain:            args.get(7).is_some_and(|s| s == "1"),
                },
                EXPLORE_MIN_BYTES,
                EXPLORE_MIN_TIME,
            );
        }
        Some("boundary-auto") if args.len() == BOUNDARY_AUTO_ARGS => {
            run_boundary_point(
                Variant::parse(&args[2]),
                Mode::parse(&args[3]),
                args[4].parse().unwrap(),
                args[5].parse().unwrap(),
                args[6].parse().unwrap(),
            );
        }
        Some("point-trace")
            if (TRACE_MIN_ARGS..=TRACE_MAX_ARGS).contains(&args.len()) =>
        {
            run_trace_point(
                TracePolicy::parse(&args[2]),
                Mode::parse(&args[3]),
                args[4].parse().expect("trace payload"),
                args[5].parse().expect("trace events_per_batch"),
                args[6].parse().expect("trace writers"),
                TraceHistogram::parse(&args[7]),
                args.get(8).is_some_and(|value| value == "1"),
            );
        }
        _ => usage(),
    }
}
