#!/usr/bin/env python3
"""Exact-PID strace launcher for the bn-2l3n measurement runner.

The launcher is spawned before the benchmark so its exact PID can be granted
with ``PR_SET_PTRACER`` under Yama ``ptrace_scope=1``.  It consumes one
canonical identity command, verifies start ticks, and execs the frozen strace
binary in place.  The PID and start ticks therefore remain stable across the
waiting-launcher and tracer identities.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import os
from pathlib import Path


def set_comm(name: str) -> None:
    if not name or len(name.encode()) > 15:
        raise SystemExit("invalid launcher comm")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(15, name.encode(), 0, 0, 0) != 0:
        raise SystemExit(f"PR_SET_NAME failed: errno={ctypes.get_errno()}")


def start_ticks(pid: int) -> int:
    payload = (Path("/proc") / str(pid) / "stat").read_text()
    closed = payload.rfind(")")
    fields = payload[closed + 2 :].split()
    if closed < 0 or len(fields) <= 19:
        raise SystemExit("malformed target /proc stat")
    return int(fields[19])


def canonical(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("ascii")


def main() -> int:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--command-fd", required=True, type=int)
    parser.add_argument("--strace", required=True, type=Path)
    parser.add_argument("--trace-output", required=True, type=Path)
    parser.add_argument("--trace-set", required=True)
    parser.add_argument("--waiting-comm", required=True)
    args = parser.parse_args()
    set_comm(args.waiting_comm)
    with os.fdopen(args.command_fd, "rb", closefd=True) as command:
        payload = command.readline(4097)
        if not payload.endswith(b"\n") or len(payload) > 4096 or command.read(1):
            raise SystemExit("launcher command framing differs")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SystemExit(f"launcher command is not JSON: {error}") from error
    if canonical(value) != payload or not isinstance(value, dict) or set(value) != {
        "pid",
        "starttime_ticks",
    }:
        raise SystemExit("launcher command is not the exact canonical identity")
    pid = value["pid"]
    ticks = value["starttime_ticks"]
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid <= 0
        or isinstance(ticks, bool)
        or not isinstance(ticks, int)
        or ticks <= 0
        or start_ticks(pid) != ticks
    ):
        raise SystemExit("launcher target identity differs")
    strace = args.strace.resolve(strict=True)
    if args.trace_output.exists() or args.trace_output.is_symlink():
        raise SystemExit("trace output identity is not fresh")
    argv = (
        str(strace),
        "-f",
        "-yy",
        "-s",
        "4096",
        "-qq",
        "-e",
        f"trace={args.trace_set}",
        "-o",
        str(args.trace_output),
        "-p",
        str(pid),
    )
    os.execve(strace, argv, {"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC"})
    raise AssertionError("execve returned")


if __name__ == "__main__":
    raise SystemExit(main())
