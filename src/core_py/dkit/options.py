"""Lock and election option helpers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

DEFAULT_LOCK_TTL = 30.0
DEFAULT_LOCK_SPIN_INTERVAL = 0.5
DEFAULT_ELECTION_UNHEALTHY_TIME = 10.0
DEFAULT_ELECTION_TIMEOUT = 3.0


@dataclass(slots=True)
class LockOption:
    ttl: float = DEFAULT_LOCK_TTL
    reentrant_identity: str = ""
    spin_interval: float = DEFAULT_LOCK_SPIN_INTERVAL
    max_wait_time: float = 0.0


LockOptionOp = Callable[[LockOption], None]


def new_lock_option(
    default_ttl: float = DEFAULT_LOCK_TTL, ops: list[LockOptionOp] | None = None
) -> LockOption:
    ttl = default_ttl if default_ttl > 0 else DEFAULT_LOCK_TTL
    opt = LockOption(ttl=ttl)
    for op in ops or []:
        op(opt)
    if opt.ttl <= 0:
        opt.ttl = ttl
    if opt.spin_interval <= 0:
        opt.spin_interval = DEFAULT_LOCK_SPIN_INTERVAL
    return opt


def lock_ttl(seconds: float) -> LockOptionOp:
    def op(option: LockOption) -> None:
        if seconds > 0:
            option.ttl = seconds

    return op


def lock_with_max_wait(seconds: float) -> LockOptionOp:
    def op(option: LockOption) -> None:
        if seconds > 0:
            option.max_wait_time = seconds

    return op


def lock_with_spin_interval(seconds: float) -> LockOptionOp:
    def op(option: LockOption) -> None:
        if seconds > 0:
            option.spin_interval = seconds

    return op


def reentrant(identity: str) -> LockOptionOp:
    def op(option: LockOption) -> None:
        option.reentrant_identity = identity

    return op


LeaderChangedHandler = Callable[["LeaderChangedEvent"], None]


@dataclass(slots=True)
class ElectionOption:
    node_key: str = ""
    keep_heartbeat: bool = False
    unhealthy_time: float = DEFAULT_ELECTION_UNHEALTHY_TIME
    timeout: float = DEFAULT_ELECTION_TIMEOUT
    isolation_key: str = ""
    can_elect: Callable[[], bool] | None = None
    on_leader_changed: LeaderChangedHandler | None = None


ElectionOptionOp = Callable[[ElectionOption], None]


@dataclass(slots=True)
class LeaderChangedEvent:
    node_key: str
    is_leader: bool
    leader_key: str = ""
