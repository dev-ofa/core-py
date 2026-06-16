"""Pagination helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass
class Pager:
    page_size: int = 0
    page_num: int = 0
    page_token: str = ""

    def get_page_info(self) -> tuple[int, int, str]:
        return self.page_size, self.page_num, self.page_token

    def set_page_number(self, page_number: int) -> None:
        self.page_num = page_number

    def initial_default_val(self) -> None:
        if self.page_size == 0:
            self.page_size = 20


@dataclass
class PagedResult(Generic[T]):
    rows: list[T]
    total_count: int


@dataclass
class FeedResult(Generic[T]):
    rows: list[T]
    next_page_token: str = ""
