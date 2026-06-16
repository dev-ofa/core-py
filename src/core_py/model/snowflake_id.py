"""Snowflake ID value type."""

from __future__ import annotations

from typing import Any

from core_py import data


class SnowflakeID(str):
    """Snowflake ID that is JSON-safe as a string and DB-safe as an integer.

    Use this type when callers need API-safe strings but still want numeric database
    storage. It has a higher integration cost than plain fixed-width string IDs
    because every persistence layer must honor the custom codec. When a project can
    choose its ID format freely, prefer fixed-width lexicographic IDs so application
    code does not need numeric/string conversion at every boundary.
    """

    def __new__(cls, value: int | str | SnowflakeID) -> SnowflakeID:
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                raise data.new_validation_error("snowflake id should not be empty")
            try:
                numeric = int(raw)
            except ValueError as exc:
                raise data.new_validation_error("parse snowflake id failed", cause=exc) from exc
        else:
            try:
                numeric = int(value)
            except (TypeError, ValueError) as exc:
                raise data.new_validation_error("parse snowflake id failed", cause=exc) from exc
        if numeric < 0:
            raise data.new_validation_error("snowflake id should not be negative")
        return str.__new__(cls, str(numeric))

    @property
    def int_value(self) -> int:
        """Return the numeric value used by database storage and ordering."""

        return int(self)

    def __int__(self) -> int:
        return int(str(self))

    def __index__(self) -> int:
        return int(self)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, int):
            return int(self) == other
        return str.__eq__(self, other)

    def __hash__(self) -> int:
        return str.__hash__(self)

    @classmethod
    def parse(cls, value: Any) -> SnowflakeID:
        """Parse an arbitrary numeric/string value into SnowflakeID."""

        return cls(value)
