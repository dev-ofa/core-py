import asyncio
import fnmatch
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any


class FakeRedis:
    def __init__(self) -> None:
        self.data: dict[str, tuple[str, float | None]] = {}
        self.closed = False

    def set(
        self,
        key: str,
        value: str,
        nx: bool = False,
        ex: int | None = None,
        px: int | None = None,
    ) -> bool:
        self._purge(key)
        if nx and key in self.data:
            return False
        expires_at = None
        if px is not None:
            expires_at = time.time() + px / 1000
        elif ex is not None:
            expires_at = time.time() + ex
        self.data[key] = (value, expires_at)
        return True

    def get(self, key: str) -> str | None:
        self._purge(key)
        item = self.data.get(key)
        return item[0] if item else None

    def exists(self, key: str) -> int:
        self._purge(key)
        return int(key in self.data)

    def eval(self, script: str, keys_count: int, key: str, owner: str, *args: str) -> int:
        del script, keys_count
        self._purge(key)
        item = self.data.get(key)
        if item and item[0] == owner:
            if args:
                self.data[key] = (item[0], time.time() + float(args[0]) / 1000)
                return 1
            del self.data[key]
            return 1
        return 0

    def delete(self, key: str) -> int:
        self._purge(key)
        if key not in self.data:
            return 0
        del self.data[key]
        return 1

    def scan_iter(self, match: str) -> list[str]:
        for key in list(self.data):
            self._purge(key)
        return [key for key in self.data if fnmatch.fnmatch(key, match)]

    def close(self) -> None:
        self.closed = True

    def _purge(self, key: str) -> None:
        item = self.data.get(key)
        if item and item[1] is not None and item[1] <= time.time():
            del self.data[key]


@dataclass
class FakeDeleteResult:
    deleted_count: int


@dataclass
class FakeUpdateResult:
    matched_count: int


class FakeIndexConflictError(Exception):
    def __init__(self, message: str = "index options conflict", code: int = 85) -> None:
        super().__init__(message)
        self.code = code


class FakeCollection:
    def __init__(self) -> None:
        self.docs: dict[str, dict[str, Any]] = {}
        self.indexes: dict[str, dict[str, Any]] = {}
        self.before_update: Callable[[dict[str, Any], dict[str, Any], bool], None] | None = None

    def create_index(self, keys: Any, **kwargs: Any) -> str:
        field = _index_field(keys)
        spec = {"expireAfterSeconds": kwargs.get("expireAfterSeconds")}
        existing = self.indexes.get(field)
        if existing is not None and existing != spec:
            raise FakeIndexConflictError()
        self.indexes[field] = spec
        return f"{field}_1"

    def drop_indexes(self) -> None:
        self.indexes.clear()

    def find_one(self, query: dict[str, Any]) -> dict[str, Any] | None:
        self._purge_ttl_docs()
        for doc in self.docs.values():
            if matches(doc, query):
                return dict(doc)
        return None

    def update_one(
        self, query: dict[str, Any], update: dict[str, Any], upsert: bool = False
    ) -> FakeUpdateResult:
        self._purge_ttl_docs()
        if self.before_update is not None:
            hook = self.before_update
            self.before_update = None
            hook(query, update, upsert)
        doc = self.find_one(query)
        if doc is None:
            if not upsert:
                return FakeUpdateResult(matched_count=0)
            doc = {"_id": query["_id"]}
        doc.update(update.get("$set", {}))
        self.docs[str(doc["_id"])] = doc
        return FakeUpdateResult(matched_count=1)

    def insert_one(self, doc: dict[str, Any]) -> None:
        self._purge_ttl_docs()
        key = str(doc["_id"])
        if key in self.docs:
            raise ValueError("duplicate key")
        self.docs[key] = dict(doc)

    def delete_one(self, query: dict[str, Any]) -> FakeDeleteResult:
        self._purge_ttl_docs()
        for key, doc in list(self.docs.items()):
            if matches(doc, query):
                del self.docs[key]
                return FakeDeleteResult(deleted_count=1)
        return FakeDeleteResult(deleted_count=0)

    def delete_many(self, query: dict[str, Any]) -> None:
        self._purge_ttl_docs()
        for key, doc in list(self.docs.items()):
            if matches(doc, query):
                del self.docs[key]

    def find(self, query: dict[str, Any]) -> Iterable[dict[str, Any]]:
        self._purge_ttl_docs()
        return [dict(doc) for doc in self.docs.values() if matches(doc, query)]

    def _purge_ttl_docs(self) -> None:
        now = datetime.now(UTC)
        for key, doc in list(self.docs.items()):
            for field, spec in self.indexes.items():
                value = doc.get(field)
                expire_after = int(spec.get("expireAfterSeconds") or 0)
                if isinstance(value, datetime) and value + timedelta(seconds=expire_after) <= now:
                    del self.docs[key]
                    break


class FakeMongoDatabase:
    def __init__(self) -> None:
        self.collections: dict[str, FakeCollection] = {}
        self.client = self
        self.closed = False

    def __getitem__(self, name: str) -> FakeCollection:
        if name not in self.collections:
            self.collections[name] = FakeCollection()
        return self.collections[name]

    def close(self) -> None:
        self.closed = True


def matches(doc: dict[str, Any], query: dict[str, Any]) -> bool:
    for key, expected in query.items():
        if key == "$or":
            if not any(matches(doc, item) for item in expected):
                return False
            continue
        actual = doc.get(key)
        if isinstance(expected, dict):
            if "$lte" in expected and not (actual <= expected["$lte"]):
                return False
            if "$gt" in expected and not (actual > expected["$gt"]):
                return False
        elif actual != expected:
            return False
    return True


def wait_until(predicate: Any, timeout: float = 2.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return bool(predicate())


async def wait_until_async(predicate: Any, timeout: float = 2.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(0.02)
    return bool(await predicate())


def _index_field(keys: Any) -> str:
    if isinstance(keys, str):
        return keys
    if isinstance(keys, list) and keys:
        first = keys[0]
        if isinstance(first, tuple):
            return str(first[0])
        if isinstance(first, dict):
            return str(first.get("key", ""))
    raise ValueError(f"unsupported index keys: {keys!r}")
