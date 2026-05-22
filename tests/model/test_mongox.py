from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from core_py import context, data, model
from core_py.model import mongox


@dataclass
class Item(model.CreateAudit, model.UpdateAudit, model.DeleteAudit, model.TenantAudit):
    id: str = ""
    name: str = ""


@dataclass
class PatchItem:
    id: str = ""
    enabled: bool = True
    count: int = 1
    name: str = "default"
    tags: list[str] | None = None


@dataclass(slots=True)
class _Result:
    matched_count: int = 0
    modified_count: int = 0
    deleted_count: int = 0
    upserted_count: int = 0


class _DuplicateKeyError(Exception):
    code = 11000


class _FakeCollection:
    def __init__(self) -> None:
        self.docs: list[dict[str, Any]] = []

    def find(self, filter_: dict[str, Any], **opts: Any) -> list[dict[str, Any]]:
        rows = [dict(doc) for doc in self.docs if _matches(doc, filter_)]
        for field, order in reversed(opts.get("sort", [])):
            rows.sort(key=lambda doc: str(doc.get(field, "")), reverse=order < 0)
        skip = int(opts.get("skip", 0) or 0)
        limit = int(opts.get("limit", 0) or 0)
        if skip:
            rows = rows[skip:]
        if limit:
            rows = rows[:limit]
        return rows

    def find_one(self, filter_: dict[str, Any]) -> dict[str, Any] | None:
        for doc in self.docs:
            if _matches(doc, filter_):
                return dict(doc)
        return None

    def count_documents(self, filter_: dict[str, Any]) -> int:
        return len([doc for doc in self.docs if _matches(doc, filter_)])

    def insert_one(self, doc: dict[str, Any]) -> None:
        if self.find_one({"_id": doc["_id"]}) is not None:
            raise _DuplicateKeyError()
        self.docs.append(dict(doc))

    def insert_many(self, docs: list[dict[str, Any]]) -> None:
        for doc in docs:
            self.insert_one(doc)

    def replace_one(
        self, filter_: dict[str, Any], doc: dict[str, Any], upsert: bool = False
    ) -> _Result:
        for idx, old in enumerate(self.docs):
            if _matches(old, filter_):
                self.docs[idx] = dict(doc)
                return _Result(matched_count=1, modified_count=1)
        if upsert:
            self.insert_one(doc)
            return _Result(upserted_count=1)
        return _Result()

    def update_one(self, filter_: dict[str, Any], update: dict[str, Any]) -> _Result:
        for doc in self.docs:
            if _matches(doc, filter_):
                doc.update(update.get("$set", {}))
                return _Result(matched_count=1, modified_count=1)
        return _Result()

    def update_many(self, filter_: dict[str, Any], update: dict[str, Any]) -> _Result:
        matched = 0
        for doc in self.docs:
            if _matches(doc, filter_):
                doc.update(update.get("$set", {}))
                matched += 1
        return _Result(matched_count=matched, modified_count=matched)

    def delete_one(self, filter_: dict[str, Any]) -> _Result:
        for idx, doc in enumerate(self.docs):
            if _matches(doc, filter_):
                self.docs.pop(idx)
                return _Result(deleted_count=1)
        return _Result()

    def delete_many(self, filter_: dict[str, Any]) -> _Result:
        keep = [doc for doc in self.docs if not _matches(doc, filter_)]
        deleted = len(self.docs) - len(keep)
        self.docs = keep
        return _Result(deleted_count=deleted)


@pytest.mark.asyncio
async def test_collection_lib_create_and_find_with_repo_rules() -> None:
    with context.use_context():
        _seed_context()
        collection = _FakeCollection()
        lib = mongox.CollectionRepository(collection, Item).with_repo_opt(
            model.RepoOpt(data_isolation=model.DATA_ISOLATION_TENANT)
        )
        repo: model.Repo[str, Item] = lib

        created = await repo.create(Item(id="i-1", name="first"))
        collection.insert_one({"_id": "i-2", "name": "other", "tenant_id": "other-tenant"})

        rows = await lib.find({})

        assert created.created_by == "user-1"
        assert created.tenant_id == "tenant-1"
        assert [row.id for row in rows] == ["i-1"]
        assert collection.docs[0]["_id"] == "i-1"
        assert collection.docs[0]["updated_by"] == "user-1"


@pytest.mark.asyncio
async def test_collection_lib_update_uses_optimistic_lock() -> None:
    with context.use_context():
        _seed_context()
        collection = _FakeCollection()
        lib = mongox.CollectionRepository(collection, Item)
        item = await lib.create(Item(id="i-1", name="before"))
        original_updated_at = item.updated_at

        item.name = "after"
        updated = await lib.update(item)

        assert updated.name == "after"
        assert updated.updated_at != original_updated_at
        assert collection.find_one({"_id": "i-1"})["name"] == "after"  # type: ignore[index]


@pytest.mark.asyncio
async def test_collection_lib_update_reports_stale_conflict() -> None:
    with context.use_context():
        _seed_context()
        collection = _FakeCollection()
        lib = mongox.CollectionRepository(collection, Item)
        item = await lib.create(Item(id="i-1", name="before"))
        stale_updated_at = item.updated_at
        item.name = "after"
        await lib.update(item)

        stale = Item(id="i-1", name="stale")
        stale.updated_at = stale_updated_at

        with pytest.raises(data.BaseError) as exc_info:
            await lib.update(stale)
        assert exc_info.value.code == data.ERR_CODE_CONFLICT


@pytest.mark.asyncio
async def test_collection_lib_patch_and_soft_delete() -> None:
    with context.use_context():
        _seed_context()
        collection = _FakeCollection()
        lib = mongox.CollectionRepository(collection, Item)
        item = await lib.create(Item(id="i-1", name="before"))

        item.name = "patched"
        await lib.patch(item)
        patched = await lib.get("i-1")
        await lib.delete(patched)

        assert collection.find_one({"_id": "i-1"})["name"] == "patched"  # type: ignore[index]
        assert await lib.find({}) == []
        assert collection.find_one({"_id": "i-1"})["deleted_by"] == "user-1"  # type: ignore[index]


def test_build_patch_payload_preserves_explicit_python_zero_values() -> None:
    payload = mongox.build_patch_payload(
        PatchItem(id="i-1", enabled=False, count=0, name="", tags=[])
    )

    assert payload == {
        "_id": "i-1",
        "enabled": False,
        "count": 0,
        "name": "",
        "tags": [],
    }


@pytest.mark.asyncio
async def test_collection_lib_reports_structured_error_when_isolation_context_is_missing() -> None:
    context.clear_current_context()
    collection = _FakeCollection()
    lib = mongox.CollectionRepository(collection, Item).with_repo_opt(
        model.RepoOpt(data_isolation=model.DATA_ISOLATION_TENANT)
    )

    with pytest.raises(data.BaseError) as exc_info:
        await lib.find({})

    assert exc_info.value.code == data.ERR_CODE_VALIDATE
    assert exc_info.value.message == "there is no tenant id in context"


def _seed_context() -> None:
    context.set_operator("user-1")
    context.set_tenant_id("tenant-1")
    context.set_app_id("app-1")


def _matches(doc: dict[str, Any], filter_: dict[str, Any]) -> bool:
    for key, expected in filter_.items():
        actual = doc.get(key)
        if isinstance(expected, dict):
            if "$exists" in expected and (key in doc) is not bool(expected["$exists"]):
                return False
            if "$in" in expected and actual not in expected["$in"]:
                return False
            continue
        if actual != expected:
            return False
    return True
