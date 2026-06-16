"""Audit models and helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from core_py import data


def _now() -> datetime:
    return datetime.now(UTC)


def _now_ms() -> int:
    return int(_now().timestamp() * 1000)


def _missing_context_error(message: str) -> data.ValidationError:
    return data.new_validate_error(message)


class TenantCarrier(Protocol):
    def get_tenant_id(self) -> str: ...
    def set_tenant_id(self, id_: str) -> None: ...
    def get_app_id(self) -> str: ...
    def set_app_id(self, id_: str) -> None: ...


@dataclass
class CreateAudit:
    created_at: datetime | None = None
    created_by: str = ""

    def get_create_time_raw(self) -> datetime | None:
        return self.created_at

    def get_creator_info(self) -> tuple[str, datetime | None]:
        return self.created_by, self.created_at

    def set_creator(self, user: str) -> None:
        self.created_by = user
        self.created_at = _now()


@dataclass
class CreateAuditMs:
    created_at: int = 0
    created_by: str = ""

    def get_create_time_raw(self) -> int:
        return self.created_at

    def get_creator_info(self) -> tuple[str, datetime | None]:
        return self.created_by, datetime.fromtimestamp(
            self.created_at / 1000, UTC
        ) if self.created_at else None

    def set_creator(self, user: str) -> None:
        self.created_by = user
        self.created_at = _now_ms()


@dataclass
class UpdateAudit:
    updated_at: datetime | None = None
    updated_by: str = ""

    def get_update_time_raw(self) -> datetime | None:
        return self.updated_at

    def get_update_info(self) -> tuple[str, datetime | None]:
        return self.updated_by, self.updated_at

    def set_updater(self, user: str) -> None:
        self.updated_by = user
        self.updated_at = _now()


@dataclass
class UpdateAuditMs:
    updated_at: int = 0
    updated_by: str = ""

    def get_update_time_raw(self) -> int:
        return self.updated_at

    def get_update_info(self) -> tuple[str, datetime | None]:
        return self.updated_by, datetime.fromtimestamp(
            self.updated_at / 1000, UTC
        ) if self.updated_at else None

    def set_updater(self, user: str) -> None:
        self.updated_by = user
        self.updated_at = _now_ms()


@dataclass
class DeleteAudit:
    deleted_at: datetime | None = None
    deleted_by: str = ""

    def get_delete_time_raw(self) -> datetime | None:
        return self.deleted_at

    def get_delete_info(self) -> tuple[str, datetime | None]:
        return self.deleted_by, self.deleted_at

    def set_deleter(self, user: str) -> None:
        self.deleted_by = user
        self.deleted_at = _now()


@dataclass
class DeleteAuditMs:
    deleted_at: int = 0
    deleted_by: str = ""

    def get_delete_time_raw(self) -> int:
        return self.deleted_at

    def get_delete_info(self) -> tuple[str, datetime | None]:
        return self.deleted_by, datetime.fromtimestamp(
            self.deleted_at / 1000, UTC
        ) if self.deleted_at else None

    def set_deleter(self, user: str) -> None:
        self.deleted_by = user
        self.deleted_at = _now_ms()


@dataclass
class TenantAudit:
    tenant_id: str = ""
    app_id: str = ""

    def get_tenant_id(self) -> str:
        return self.tenant_id

    def set_tenant_id(self, id_: str) -> None:
        self.tenant_id = id_

    def get_app_id(self) -> str:
        return self.app_id

    def set_app_id(self, id_: str) -> None:
        self.app_id = id_
