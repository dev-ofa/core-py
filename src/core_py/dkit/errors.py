"""Shared dkit errors."""

from __future__ import annotations

from core_py import data

ERR_CODE_DKIT_INVALID_OPTION = 20100
ERR_CODE_DKIT_LOCK_NOT_ACQUIRED = 20101
ERR_CODE_DKIT_ALREADY_UNLOCKED = 20102
ERR_CODE_DKIT_ELECTION_NOT_ENABLED = 20103
ERR_CODE_DKIT_NO_AVAILABLE_NUMBER = 20104
ERR_CODE_DKIT_BACKEND_UNAVAILABLE = 10100
ERR_CODE_DKIT_DEFAULT_KIT_NOT_CONFIGURED = 10101


class DKitError(data.Error):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(code, message)


class InvalidOptionError(DKitError):
    def __init__(self, message: str = "dkit: invalid option") -> None:
        super().__init__(ERR_CODE_DKIT_INVALID_OPTION, message)


class LockNotAcquiredError(DKitError):
    def __init__(self, message: str = "dkit: lock not acquired") -> None:
        super().__init__(ERR_CODE_DKIT_LOCK_NOT_ACQUIRED, message)


class AlreadyUnlockedError(DKitError):
    def __init__(self, message: str = "dkit: already unlocked") -> None:
        super().__init__(ERR_CODE_DKIT_ALREADY_UNLOCKED, message)


class ElectionNotEnabledError(DKitError):
    def __init__(self, message: str = "dkit: election not enabled") -> None:
        super().__init__(ERR_CODE_DKIT_ELECTION_NOT_ENABLED, message)


class BackendUnavailableError(DKitError):
    def __init__(self, message: str = "dkit: backend unavailable") -> None:
        super().__init__(ERR_CODE_DKIT_BACKEND_UNAVAILABLE, message)


class NoAvailableNumberError(DKitError):
    def __init__(self, message: str = "dkit: no available number") -> None:
        super().__init__(ERR_CODE_DKIT_NO_AVAILABLE_NUMBER, message)


class DefaultKitNotConfiguredError(DKitError):
    def __init__(self, message: str = "dkit: default kit not configured") -> None:
        super().__init__(ERR_CODE_DKIT_DEFAULT_KIT_NOT_CONFIGURED, message)


ERR_INVALID_OPTION = InvalidOptionError("dkit: invalid option")
ERR_LOCK_NOT_ACQUIRED = LockNotAcquiredError("dkit: lock not acquired")
ERR_ALREADY_UNLOCKED = AlreadyUnlockedError("dkit: already unlocked")
ERR_ELECTION_NOT_ENABLED = ElectionNotEnabledError("dkit: election not enabled")
ERR_BACKEND_UNAVAILABLE = BackendUnavailableError("dkit: backend unavailable")
ERR_NO_AVAILABLE_NUMBER = NoAvailableNumberError("dkit: no available number")
ERR_DEFAULT_KIT_NOT_CONFIGURED = DefaultKitNotConfiguredError("dkit: default kit not configured")
