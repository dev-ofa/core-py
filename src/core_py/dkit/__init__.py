"""Distributed primitive protocols and backend implementations."""

from __future__ import annotations

from core_py.dkit.errors import (
    ERR_ALREADY_UNLOCKED,
    ERR_BACKEND_UNAVAILABLE,
    ERR_ELECTION_NOT_ENABLED,
    ERR_INVALID_OPTION,
    ERR_LOCK_NOT_ACQUIRED,
    ERR_NO_AVAILABLE_NUMBER,
    AlreadyUnlockedError,
    BackendUnavailableError,
    DKitError,
    ElectionNotEnabledError,
    InvalidOptionError,
    LockNotAcquiredError,
    NoAvailableNumberError,
)
from core_py.dkit.kit import Action, DefaultKit, new_default_kit
from core_py.dkit.memory import InMemoryAtomic, InMemoryMutex
from core_py.dkit.mongo import MongoAtomic, MongoMutex
from core_py.dkit.options import (
    DEFAULT_ELECTION_TIMEOUT,
    DEFAULT_ELECTION_UNHEALTHY_TIME,
    DEFAULT_LOCK_SPIN_INTERVAL,
    DEFAULT_LOCK_TTL,
    ElectionOption,
    ElectionOptionOp,
    LeaderChangedEvent,
    LeaderChangedHandler,
    LockOption,
    LockOptionOp,
    lock_ttl,
    lock_with_max_wait,
    lock_with_spin_interval,
    new_lock_option,
    reentrant,
)
from core_py.dkit.protocols import Atomic, DistributedMutex, ElectionController
from core_py.dkit.redis import RedisAtomic, RedisMutex

__all__ = [
    "Action",
    "AlreadyUnlockedError",
    "Atomic",
    "BackendUnavailableError",
    "DKitError",
    "DEFAULT_ELECTION_TIMEOUT",
    "DEFAULT_ELECTION_UNHEALTHY_TIME",
    "DEFAULT_LOCK_SPIN_INTERVAL",
    "DEFAULT_LOCK_TTL",
    "DefaultKit",
    "DistributedMutex",
    "ERR_ALREADY_UNLOCKED",
    "ERR_BACKEND_UNAVAILABLE",
    "ERR_ELECTION_NOT_ENABLED",
    "ERR_INVALID_OPTION",
    "ERR_LOCK_NOT_ACQUIRED",
    "ERR_NO_AVAILABLE_NUMBER",
    "ElectionController",
    "ElectionNotEnabledError",
    "ElectionOption",
    "ElectionOptionOp",
    "InMemoryAtomic",
    "InMemoryMutex",
    "InvalidOptionError",
    "LeaderChangedEvent",
    "LeaderChangedHandler",
    "LockNotAcquiredError",
    "LockOption",
    "LockOptionOp",
    "MongoAtomic",
    "MongoMutex",
    "NoAvailableNumberError",
    "RedisAtomic",
    "RedisMutex",
    "lock_ttl",
    "lock_with_max_wait",
    "lock_with_spin_interval",
    "new_default_kit",
    "new_lock_option",
    "reentrant",
]
