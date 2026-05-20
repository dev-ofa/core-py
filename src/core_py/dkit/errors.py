"""Shared dkit errors."""

from __future__ import annotations


class DKitError(Exception):
    pass


class InvalidOptionError(DKitError):
    pass


class LockNotAcquiredError(DKitError):
    pass


class AlreadyUnlockedError(DKitError):
    pass


class ElectionNotEnabledError(DKitError):
    pass


class BackendUnavailableError(DKitError):
    pass


class NoAvailableNumberError(DKitError):
    pass


ERR_INVALID_OPTION = InvalidOptionError("dkit: invalid option")
ERR_LOCK_NOT_ACQUIRED = LockNotAcquiredError("dkit: lock not acquired")
ERR_ALREADY_UNLOCKED = AlreadyUnlockedError("dkit: already unlocked")
ERR_ELECTION_NOT_ENABLED = ElectionNotEnabledError("dkit: election not enabled")
ERR_BACKEND_UNAVAILABLE = BackendUnavailableError("dkit: backend unavailable")
ERR_NO_AVAILABLE_NUMBER = NoAvailableNumberError("dkit: no available number")
