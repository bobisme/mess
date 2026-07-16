//! Runner/child phase handshake over one inherited AF_UNIX stream.

use std::ffi::{c_int, c_long, c_ulong};
use std::fmt::Write as _;
use std::fs::File;
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::os::fd::{FromRawFd as _, RawFd};
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
use std::os::unix::net::UnixStream;

use crate::schema::{canonical_object, json_string, json_u64};

#[repr(C)]
struct Timespec {
    tv_sec:  i64,
    tv_nsec: i64,
}

unsafe extern "C" {
    fn clock_gettime(clock_id: i32, time: *mut Timespec) -> i32;
    fn getrusage(who: i32, usage: *mut RUsage) -> i32;
    fn prctl(
        option: c_int,
        argument2: c_ulong,
        argument3: c_ulong,
        argument4: c_ulong,
        argument5: c_ulong,
    ) -> c_int;
}

const CLOCK_MONOTONIC: i32 = 1;
const RUSAGE_SELF: i32 = 0;
const PR_SET_PTRACER: c_int = 0x5961_6D61;

/// Authorize only the runner's pre-spawned frozen tracer under Yama
/// `ptrace_scope=1`. With no env binding, no ptrace authorization is changed.
pub fn authorize_ptracer_from_env() {
    let Ok(value) = std::env::var("ASTERISM_REBASELINE_PTRACER_PID") else {
        return;
    };
    assert!(
        !value.is_empty()
            && value.bytes().all(|byte| byte.is_ascii_digit())
            && !value.starts_with('0'),
        "invalid ASTERISM_REBASELINE_PTRACER_PID"
    );
    let pid: u32 =
        value.parse().expect("ASTERISM_REBASELINE_PTRACER_PID exceeds u32");
    assert!(pid <= i32::MAX as u32, "tracer PID exceeds pid_t");
    // SAFETY: PR_SET_PTRACER accepts one positive pid_t widened to unsigned
    // long; all unused variadic-compatible argument registers are zeroed.
    let status = unsafe { prctl(PR_SET_PTRACER, pid.into(), 0, 0, 0) };
    assert_eq!(status, 0, "PR_SET_PTRACER failed");
}

#[repr(C)]
struct Timeval {
    tv_sec:  c_long,
    tv_usec: c_long,
}

#[repr(C)]
struct RUsage {
    ru_utime:    Timeval,
    ru_stime:    Timeval,
    ru_maxrss:   c_long,
    ru_ixrss:    c_long,
    ru_idrss:    c_long,
    ru_isrss:    c_long,
    ru_minflt:   c_long,
    ru_majflt:   c_long,
    ru_nswap:    c_long,
    ru_inblock:  c_long,
    ru_oublock:  c_long,
    ru_msgsnd:   c_long,
    ru_msgrcv:   c_long,
    ru_nsignals: c_long,
    ru_nvcsw:    c_long,
    ru_nivcsw:   c_long,
}

#[derive(Clone, Copy, Debug)]
pub struct CpuSnapshot {
    pub user_ns:   u64,
    pub system_ns: u64,
}

pub fn cpu_snapshot() -> CpuSnapshot {
    // SAFETY: RUsage is a plain C output structure and zero is a valid bit
    // pattern for every field before getrusage fills it.
    let mut usage: RUsage = unsafe { std::mem::zeroed() };
    // SAFETY: `usage` is valid writable storage for RUSAGE_SELF.
    let status = unsafe { getrusage(RUSAGE_SELF, &raw mut usage) };
    assert_eq!(status, 0, "getrusage(RUSAGE_SELF) failed");
    fn nanos(value: Timeval) -> u64 {
        assert!(value.tv_sec >= 0 && (0..1_000_000).contains(&value.tv_usec));
        (value.tv_sec as u64)
            .checked_mul(1_000_000_000)
            .and_then(|seconds| {
                seconds.checked_add(value.tv_usec as u64 * 1_000)
            })
            .expect("rusage clock overflow")
    }
    CpuSnapshot {
        user_ns:   nanos(usage.ru_utime),
        system_ns: nanos(usage.ru_stime),
    }
}

pub fn monotonic_ns() -> u64 {
    let mut time = Timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `time` is a valid writable timespec and Linux clock id 1 is
    // CLOCK_MONOTONIC on every supported measurement host.
    let status = unsafe { clock_gettime(CLOCK_MONOTONIC, &raw mut time) };
    assert_eq!(status, 0, "clock_gettime(CLOCK_MONOTONIC) failed");
    assert!(time.tv_sec >= 0 && (0..1_000_000_000).contains(&time.tv_nsec));
    (time.tv_sec as u64)
        .checked_mul(1_000_000_000)
        .and_then(|seconds| seconds.checked_add(time.tv_nsec as u64))
        .expect("monotonic clock overflow")
}

#[derive(Clone, Copy, Debug)]
pub struct MeasuredMarkers {
    pub allocation_calls_end:         u64,
    pub allocated_bytes_end:          u64,
    pub counter_end_monotonic_ns:     u64,
    pub last_completion_monotonic_ns: u64,
    pub release_monotonic_ns:         u64,
    pub process_system_cpu_end_ns:    u64,
    pub process_user_cpu_end_ns:      u64,
    pub t0_monotonic_ns:              u64,
    pub t1_monotonic_ns:              u64,
}

pub struct Control {
    stream:         UnixStream,
    reader:         BufReader<UnixStream>,
    read_buffer:    String,
    context_sha256: String,
    perf:           PerfControl,
}

struct PerfPipes {
    command: File,
    ack:     File,
    ledger:  File,
}

enum PerfControl {
    NonCpu,
    Unavailable {
        _permission_result: String,
    },
    Available {
        _permission_result: String,
        pipes:              PerfPipes,
        disable:            Option<PerfDisableEvent>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PerfDisableEvent {
    nonce:                     Nonce,
    sent_monotonic_ns:         u64,
    ack_received_monotonic_ns: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Nonce([u8; 64]);

impl Nonce {
    pub fn as_str(&self) -> &str {
        // SAFETY: construction accepts ASCII lowercase hex only.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

struct StackLine {
    bytes: [u8; 1024],
    len:   usize,
}

impl StackLine {
    fn new() -> Self { Self { bytes: [0; 1024], len: 0 } }

    fn as_str(&self) -> &str {
        // SAFETY: fmt::Write only accepts valid UTF-8 strings.
        unsafe { std::str::from_utf8_unchecked(&self.bytes[..self.len]) }
    }
}

impl std::fmt::Write for StackLine {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        let end = self.len.checked_add(value.len()).ok_or(std::fmt::Error)?;
        let target =
            self.bytes.get_mut(self.len..end).ok_or(std::fmt::Error)?;
        target.copy_from_slice(value.as_bytes());
        self.len = end;
        Ok(())
    }
}

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

const PERF_PERMISSION_RESULT: &str =
    "ASTERISM_REBASELINE_PERF_PERMISSION_RESULT";
const PERF_COMMAND_FD: &str = "ASTERISM_REBASELINE_PERF_COMMAND_FD";
const PERF_ACK_FD: &str = "ASTERISM_REBASELINE_PERF_ACK_FD";
const PERF_ACK_LEDGER_FD: &str = "ASTERISM_REBASELINE_PERF_ACK_LEDGER_FD";

fn decimal(value: &str, signed: bool) -> bool {
    let digits =
        if signed { value.strip_prefix('-').unwrap_or(value) } else { value };
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

fn perf_permission_status(value: &str) -> bool {
    const AVAILABLE_PREFIX: &str = "available;perf_event_paranoid=";
    const UNAVAILABLE_PREFIX: &str = "not_available;perf_event_paranoid=";
    const SCOPE: &str = ";scope=user-only";
    const EXIT: &str = ";scope=user-only;exit_status=";
    if let Some(paranoid) = value
        .strip_prefix(AVAILABLE_PREFIX)
        .and_then(|rest| rest.strip_suffix(SCOPE))
    {
        assert!(decimal(paranoid, true), "invalid available perf permission");
        return true;
    }
    let rest = value
        .strip_prefix(UNAVAILABLE_PREFIX)
        .expect("invalid perf permission result");
    let (paranoid, exit_status) =
        rest.split_once(EXIT).expect("invalid unavailable perf permission");
    assert!(decimal(paranoid, true), "invalid unavailable perf paranoid");
    assert!(decimal(exit_status, false), "invalid perf permission exit status");
    let exit_status: u32 =
        exit_status.parse().expect("perf permission exit status exceeds u32");
    assert_ne!(
        exit_status, 0,
        "unavailable perf permission has zero exit status"
    );
    false
}

fn inherited_fd(name: &str) -> RawFd {
    let value =
        std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"));
    let fd: RawFd = value.parse().unwrap_or_else(|_| panic!("invalid {name}"));
    assert!(fd >= 3 && fd.to_string() == value, "noncanonical {name}");
    fd
}

fn assert_env_absent(name: &str) {
    assert!(std::env::var_os(name).is_none(), "unexpected {name}");
}

fn is_cpu_profile(mode: &str) -> bool {
    mode == "cpu_profiles"
        || (mode == "smoke"
            && std::env::var("ASTERISM_REBASELINE_SMOKE_TARGET")
                .expect("missing smoke target for perf environment")
                == "cpu_profiles")
}

pub fn validate_perf_environment_mode(mode: &str) {
    if !is_cpu_profile(mode) {
        for name in [
            PERF_PERMISSION_RESULT,
            PERF_COMMAND_FD,
            PERF_ACK_FD,
            PERF_ACK_LEDGER_FD,
        ] {
            assert_env_absent(name);
        }
    }
}

impl PerfControl {
    fn connect(mode: &str, control_fd: RawFd) -> Self {
        if !is_cpu_profile(mode) {
            validate_perf_environment_mode(mode);
            return Self::NonCpu;
        }
        let permission_result = std::env::var(PERF_PERMISSION_RESULT)
            .expect("missing CPU perf permission result");
        if !perf_permission_status(&permission_result) {
            for name in [PERF_COMMAND_FD, PERF_ACK_FD, PERF_ACK_LEDGER_FD] {
                assert_env_absent(name);
            }
            return Self::Unavailable { _permission_result: permission_result };
        }
        let command_fd = inherited_fd(PERF_COMMAND_FD);
        let ack_fd = inherited_fd(PERF_ACK_FD);
        let ledger_fd = inherited_fd(PERF_ACK_LEDGER_FD);
        let mut descriptors = [control_fd, command_fd, ack_fd, ledger_fd];
        descriptors.sort_unstable();
        assert!(
            descriptors.windows(2).all(|pair| pair[0] != pair[1]),
            "inherited control descriptors alias"
        );
        // SAFETY: the runner passes sole child ownership of each inherited
        // descriptor. The ledger descriptor is a duplicate of the runner's
        // one open file description, so both ACK writes share one offset.
        let pipes = unsafe {
            PerfPipes {
                command: File::from_raw_fd(command_fd),
                ack:     File::from_raw_fd(ack_fd),
                ledger:  File::from_raw_fd(ledger_fd),
            }
        };
        let command_metadata =
            pipes.command.metadata().expect("inspect perf command pipe");
        let ack_metadata = pipes.ack.metadata().expect("inspect perf ACK pipe");
        let ledger_metadata =
            pipes.ledger.metadata().expect("inspect perf ACK ledger");
        assert!(
            command_metadata.file_type().is_fifo()
                && ack_metadata.file_type().is_fifo()
                && ledger_metadata.file_type().is_file(),
            "perf inherited descriptor types differ"
        );
        let identities = [
            (command_metadata.dev(), command_metadata.ino()),
            (ack_metadata.dev(), ack_metadata.ino()),
            (ledger_metadata.dev(), ledger_metadata.ino()),
        ];
        assert!(
            identities[0] != identities[1]
                && identities[0] != identities[2]
                && identities[1] != identities[2],
            "perf inherited descriptors alias one kernel object"
        );
        Self::Available {
            _permission_result: permission_result,
            pipes,
            disable: None,
        }
    }

    fn disable_after_t1(&mut self, nonce: &Nonce, t1_monotonic_ns: u64) {
        let Self::Available { pipes, disable, .. } = self else {
            return;
        };
        assert!(disable.is_none(), "perf disable repeated");
        let sent_monotonic_ns = monotonic_ns();
        assert!(
            sent_monotonic_ns >= t1_monotonic_ns,
            "perf disable precedes t1"
        );
        pipes.command.write_all(b"disable\n").expect("write perf disable");
        pipes.command.flush().expect("flush perf disable");
        let mut ack = [0_u8; 4];
        pipes.ack.read_exact(&mut ack).expect("read exact perf disable ACK");
        assert_eq!(&ack, b"ack\n", "perf disable ACK differs");
        let ack_received_monotonic_ns = monotonic_ns();
        assert!(
            ack_received_monotonic_ns > sent_monotonic_ns,
            "perf disable ACK timestamp is not later than send"
        );
        pipes.ledger.write_all(&ack).expect("append perf disable ACK ledger");
        pipes.ledger.flush().expect("flush perf disable ACK ledger");
        *disable = Some(PerfDisableEvent {
            nonce: *nonce,
            sent_monotonic_ns,
            ack_received_monotonic_ns,
        });
    }

    fn measured_value(&self, nonce: &Nonce) -> Option<String> {
        match self {
            Self::NonCpu => None,
            Self::Unavailable { .. } => Some("null".to_owned()),
            Self::Available { disable, .. } => {
                let event =
                    disable.expect("available perf was not disabled at t1");
                assert_eq!(event.nonce, *nonce, "perf disable nonce mismatch");
                Some(canonical_object(&[
                    ("ack", json_string("ack")),
                    (
                        "ack_received_monotonic_ns",
                        json_u64(event.ack_received_monotonic_ns),
                    ),
                    ("command", json_string("disable")),
                    ("nonce", json_string(event.nonce.as_str())),
                    ("sent_monotonic_ns", json_u64(event.sent_monotonic_ns)),
                ]))
            }
        }
    }
}

fn command(line: &str, wanted: &str) -> Nonce {
    let prefix = match wanted {
        "start" => "{\"command\":\"start\",\"nonce\":\"",
        "release" => "{\"command\":\"release\",\"nonce\":\"",
        _ => panic!("unsupported simple control command"),
    };
    let suffix = "\"}";
    assert!(
        line.starts_with(&prefix) && line.ends_with(suffix),
        "invalid {wanted} command"
    );
    let nonce = &line[prefix.len()..line.len() - suffix.len()];
    assert!(lower_hex(nonce, 64), "invalid control nonce");
    let mut bytes = [0; 64];
    bytes.copy_from_slice(nonce.as_bytes());
    Nonce(bytes)
}

fn continue_command(line: &str, phase: &str) -> Nonce {
    let prefix = "{\"command\":\"continue\",\"nonce\":\"";
    let infix = format!("\",\"phase\":\"{phase}\"}}");
    assert!(
        line.starts_with(prefix) && line.ends_with(&infix),
        "invalid {phase} continue command"
    );
    let nonce = &line[prefix.len()..line.len() - infix.len()];
    assert!(lower_hex(nonce, 64), "invalid continue nonce");
    let mut bytes = [0; 64];
    bytes.copy_from_slice(nonce.as_bytes());
    Nonce(bytes)
}

impl Control {
    pub fn connect() -> Self {
        let fd: RawFd = std::env::var("ASTERISM_REBASELINE_CONTROL_FD")
            .expect("missing ASTERISM_REBASELINE_CONTROL_FD")
            .parse()
            .expect("invalid ASTERISM_REBASELINE_CONTROL_FD");
        assert!(fd >= 3, "control descriptor may not alias stdio");
        let mode = std::env::var("ASTERISM_REBASELINE_MODE")
            .expect("missing ASTERISM_REBASELINE_MODE");
        let perf = PerfControl::connect(&mode, fd);
        // SAFETY: the runner passes sole ownership of this inherited
        // descriptor.
        let stream = unsafe { UnixStream::from_raw_fd(fd) };
        let reader =
            BufReader::new(stream.try_clone().expect("clone control stream"));
        let context_sha256 =
            std::env::var("ASTERISM_REBASELINE_CONTEXT_SHA256")
                .expect("missing ASTERISM_REBASELINE_CONTEXT_SHA256");
        assert!(lower_hex(&context_sha256, 64), "invalid context SHA-256");
        Self {
            stream,
            reader,
            read_buffer: String::with_capacity(1024),
            context_sha256,
            perf,
        }
    }

    pub fn disable_perf_after_t1(
        &mut self,
        nonce: &Nonce,
        t1_monotonic_ns: u64,
    ) {
        self.perf.disable_after_t1(nonce, t1_monotonic_ns);
    }

    fn send(&mut self, value: &str) {
        self.stream.write_all(value.as_bytes()).expect("write control message");
        self.stream.write_all(b"\n").expect("write control newline");
        self.stream.flush().expect("flush control message");
    }

    fn receive(&mut self) -> &str {
        self.read_buffer.clear();
        let bytes = self
            .reader
            .read_line(&mut self.read_buffer)
            .expect("read control message");
        assert!(
            bytes > 0 && self.read_buffer.ends_with('\n'),
            "truncated control message"
        );
        self.read_buffer.pop();
        assert!(
            !self.read_buffer.contains('\n')
                && !self.read_buffer.contains('\r'),
            "invalid control line"
        );
        &self.read_buffer
    }

    pub fn boot(&mut self) -> Nonce {
        self.send(&canonical_object(&[
            ("context_sha256", json_string(&self.context_sha256)),
            ("phase", json_string("boot")),
            ("protocol_sha256", json_string(crate::contract::PROTOCOL_SHA256)),
            ("variant", json_string(crate::contract::VARIANT)),
        ]));
        continue_command(&self.receive(), "boot")
    }

    pub fn runtime(&mut self, nonce: &Nonce) -> Nonce {
        self.send(&canonical_object(&[
            ("nonce", json_string(nonce.as_str())),
            ("phase", json_string("runtime")),
        ]));
        continue_command(&self.receive(), "runtime")
    }

    pub fn opened(&mut self, nonce: &Nonce) -> Nonce {
        self.send(&canonical_object(&[
            ("nonce", json_string(nonce.as_str())),
            ("phase", json_string("opened")),
        ]));
        continue_command(&self.receive(), "opened")
    }

    pub fn opened_after_start(
        &mut self,
        nonce: &Nonce,
        open_start_monotonic_ns: u64,
        opened_monotonic_ns: u64,
    ) -> Nonce {
        self.send(&canonical_object(&[
            ("nonce", json_string(nonce.as_str())),
            ("open_start_monotonic_ns", json_u64(open_start_monotonic_ns)),
            ("opened_monotonic_ns", json_u64(opened_monotonic_ns)),
            ("phase", json_string("opened")),
        ]));
        continue_command(&self.receive(), "opened")
    }

    pub fn ready_and_wait_start(
        &mut self,
        nonce: &Nonce,
        allocation_calls_start: u64,
        allocated_bytes_start: u64,
        process_user_cpu_start_ns: u64,
        process_system_cpu_start_ns: u64,
        ready_monotonic_ns: u64,
        counter_start_monotonic_ns: u64,
    ) -> Nonce {
        // Counter snapshots already happened. Keep both this emission and the
        // start-command parse allocation-free so control traffic cannot enter
        // the measured allocation delta.
        let mut line = StackLine::new();
        write!(
            &mut line,
            concat!(
                "{{\"allocated_bytes_start\":{},\"allocation_calls_start\":{},",
                "\"context_sha256\":\"{}\",\"counter_start_monotonic_ns\":{},",
                "\"nonce\":\"{}\",\"phase\":\"ready\",",
                "\"process_system_cpu_start_ns\":{},\"\
                 process_user_cpu_start_ns\":{},",
                "\"protocol_sha256\":\"{}\",\"ready_monotonic_ns\":{},",
                "\"variant\":\"{}\"}}"
            ),
            allocated_bytes_start,
            allocation_calls_start,
            self.context_sha256,
            counter_start_monotonic_ns,
            nonce.as_str(),
            process_system_cpu_start_ns,
            process_user_cpu_start_ns,
            crate::contract::PROTOCOL_SHA256,
            ready_monotonic_ns,
            crate::contract::VARIANT,
        )
        .expect("ready control message exceeds stack buffer");
        self.send(line.as_str());
        command(&self.receive(), "start")
    }

    pub fn measured_and_wait_release(
        &mut self,
        nonce: &Nonce,
        markers: MeasuredMarkers,
    ) {
        let mut fields = vec![
            ("allocated_bytes_end", json_u64(markers.allocated_bytes_end)),
            ("allocation_calls_end", json_u64(markers.allocation_calls_end)),
            (
                "counter_end_monotonic_ns",
                json_u64(markers.counter_end_monotonic_ns),
            ),
            (
                "last_completion_monotonic_ns",
                json_u64(markers.last_completion_monotonic_ns),
            ),
            ("nonce", json_string(nonce.as_str())),
            ("phase", json_string("measured")),
            (
                "process_system_cpu_end_ns",
                json_u64(markers.process_system_cpu_end_ns),
            ),
            (
                "process_user_cpu_end_ns",
                json_u64(markers.process_user_cpu_end_ns),
            ),
            ("release_monotonic_ns", json_u64(markers.release_monotonic_ns)),
            ("t0_monotonic_ns", json_u64(markers.t0_monotonic_ns)),
            ("t1_monotonic_ns", json_u64(markers.t1_monotonic_ns)),
        ];
        if let Some(perf_disable) = self.perf.measured_value(nonce) {
            fields.push(("perf_disable", perf_disable));
        }
        self.send(&canonical_object(&fields));
        let released = command(&self.receive(), "release");
        assert_eq!(released, *nonce, "release nonce mismatch");
    }
}
