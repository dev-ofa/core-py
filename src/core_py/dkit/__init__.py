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
from core_py.dkit.kit import Action, DefaultKit, new_default_kit, new_default_kit_with_context
from core_py.dkit.memory import InMemoryAtomic, InMemoryMutex
from core_py.dkit.mongo import MongoAtomic, MongoMutex, new_mongo_atomic
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
from core_py.dkit.redis import RedisAtomic, RedisMutex, new_redis_atomic

# Go-style aliases.
DefaultLockTTL = DEFAULT_LOCK_TTL
DefaultLockSpinInterval = DEFAULT_LOCK_SPIN_INTERVAL
DefaultElectionUnhealthyTime = DEFAULT_ELECTION_UNHEALTHY_TIME
DefaultElectionTimeout = DEFAULT_ELECTION_TIMEOUT
ErrInvalidOption = ERR_INVALID_OPTION
ErrLockNotAcquired = ERR_LOCK_NOT_ACQUIRED
ErrAlreadyUnlocked = ERR_ALREADY_UNLOCKED
ErrElectionNotEnabled = ERR_ELECTION_NOT_ENABLED
ErrBackendUnavailable = ERR_BACKEND_UNAVAILABLE
ErrNoAvailableNumber = ERR_NO_AVAILABLE_NUMBER
NewLockOption = new_lock_option
LockTTL = lock_ttl
LockWithMaxWait = lock_with_max_wait
LockWithSpinInterval = lock_with_spin_interval
Reentrant = reentrant
NewDefaultKit = new_default_kit
NewDefaultKitWithContext = new_default_kit_with_context
NewRedisAtomic = new_redis_atomic
NewMongoAtomic = new_mongo_atomic
EventLeaderChanged = LeaderChangedEvent

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
    "DefaultElectionTimeout",
    "DefaultElectionUnhealthyTime",
    "DefaultKit",
    "DefaultLockSpinInterval",
    "DefaultLockTTL",
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
    "ErrAlreadyUnlocked",
    "ErrBackendUnavailable",
    "ErrElectionNotEnabled",
    "ErrInvalidOption",
    "ErrLockNotAcquired",
    "ErrNoAvailableNumber",
    "EventLeaderChanged",
    "InMemoryAtomic",
    "InMemoryMutex",
    "InvalidOptionError",
    "LeaderChangedEvent",
    "LeaderChangedHandler",
    "LockNotAcquiredError",
    "LockOption",
    "LockOptionOp",
    "LockTTL",
    "LockWithMaxWait",
    "LockWithSpinInterval",
    "MongoAtomic",
    "MongoMutex",
    "NewDefaultKit",
    "NewDefaultKitWithContext",
    "NewLockOption",
    "NewMongoAtomic",
    "NewRedisAtomic",
    "NoAvailableNumberError",
    "RedisAtomic",
    "RedisMutex",
    "Reentrant",
    "lock_ttl",
    "lock_with_max_wait",
    "lock_with_spin_interval",
    "new_default_kit",
    "new_default_kit_with_context",
    "new_lock_option",
    "new_mongo_atomic",
    "new_redis_atomic",
    "reentrant",
]
