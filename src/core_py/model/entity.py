"""Entity helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from core_py.model.audit import CreateAudit
from core_py.model.snowflake_id import SnowflakeID

P = TypeVar("P", str, int, SnowflakeID)


@dataclass
class Entity(CreateAudit, Generic[P]):
    id: P | None = None

    def get_id(self) -> P | None:
        return self.id

    def set_id(self, id_: P) -> None:
        self.id = id_
