from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from core_py import context, data, model
from core_py.model import mongox


@dataclass
class Item(model.CreateAudit, model.UpdateAudit, model.DeleteAudit, model.TenantAudit):
    id: str = ""
    name: str = ""


@dataclass
class IntIDItem:
    id: int = 0
    name: str = ""


@dataclass
class SnowflakeIDItem:
    id: model.SnowflakeID = model.SnowflakeID(0)
    name: str = ""


@dataclass
class BoolCursorItem:
    id: str = ""
    enabled: bool = False


@dataclass
class PatchItem:
    id: str = ""
    enabled: bool = True
    count: int = 1
    name: str = "default"
    tags: list[str] | None = None


@dataclass
class InlineMeta:
    note: str = field(default="", metadata={"bson": "note_text"})


@dataclass
class NestedPatchValue:
    title: str = ""
    inline_meta: InlineMeta = field(default_factory=InlineMeta)


@dataclass
class TaggedItem(model.Entity[str], model.UpdateAudit, model.TenantAudit):
    name: str = field(default="", metadata={"bson": "str_field"})
    inline_meta: InlineMeta = field(default_factory=InlineMeta, metadata={"bson": "inline"})
    ignored: str = field(default="", metadata={"bson": "-"})


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
        for field_name, order in reversed(opts.get("sort", [])):
            rows.sort(key=lambda doc: doc.get(field_name, ""), reverse=order < 0)
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

        with pytest.raises(data.ResourceError) as exc_info:
            await lib.update(stale)
        assert data.is_err_code(data.ERR_CODE_CONFLICT, exc_info.value)
        assert exc_info.value.resource == "_FakeCollection/i-1"
        assert str(exc_info.value) == "data is modified by other: _FakeCollection/i-1"


@pytest.mark.asyncio
async def test_collection_lib_not_found_error_includes_resource() -> None:
    collection = _FakeCollection()
    lib = mongox.CollectionRepository[str, Item](collection, Item)

    with pytest.raises(data.ResourceError) as exc_info:
        await lib.get("missing")

    assert exc_info.value.resource == "_FakeCollection/missing"
    assert str(exc_info.value) == "resource not found: _FakeCollection/missing"


@pytest.mark.asyncio
async def test_collection_lib_patch_and_soft_delete() -> None:
    with context.use_context():
        _seed_context()
        collection = _FakeCollection()
        lib = mongox.CollectionRepository[str, Item](collection, Item)
        item = await lib.create(Item(id="i-1", name="before"))

        item.name = "patched"
        await lib.patch(item)
        patched = await lib.get("i-1")
        await lib.delete(patched)

        assert collection.find_one({"_id": "i-1"})["name"] == "patched"  # type: ignore[index]
        assert await lib.find({}) == []
        assert collection.find_one({"_id": "i-1"})["deleted_by"] == "user-1"  # type: ignore[index]


@pytest.mark.asyncio
async def test_collection_lib_upsert_create_and_update() -> None:
    with context.use_context():
        _seed_context()
        collection = _FakeCollection()
        lib = mongox.CollectionRepository(collection, Item)

        created = await lib.upsert(Item(id="i-1", name="created"))
        created_at = created.created_at

        created.name = "updated"
        updated = await lib.upsert(created)

        assert collection.find_one({"_id": "i-1"})["name"] == "updated"  # type: ignore[index]
        assert updated.created_at == created_at
        assert updated.updated_by == "user-1"


@pytest.mark.asyncio
async def test_collection_lib_page_query_supports_default_sort_and_explicit_sort() -> None:
    collection = _FakeCollection()
    base = datetime(2026, 1, 1, tzinfo=UTC)
    collection.insert_one({"_id": "i-1", "name": "beta", "created_at": base + timedelta(seconds=1)})
    collection.insert_one(
        {"_id": "i-2", "name": "alpha", "created_at": base + timedelta(seconds=2)}
    )
    collection.insert_one(
        {"_id": "i-3", "name": "gamma", "created_at": base + timedelta(seconds=3)}
    )
    lib = mongox.CollectionRepository(collection, Item)

    default_page = await lib.page_query(
        mongox.PageQueryInput(pager=model.Pager(page_size=2, page_num=2))
    )
    custom_page = await lib.page_query(
        mongox.PageQueryInput(
            sort=data.Sortable(order_by="name desc"),
            pager=model.Pager(page_size=2, page_num=1),
        )
    )

    assert [row.id for row in default_page.rows] == ["i-3"]
    assert default_page.total_count == 3
    assert [row.id for row in custom_page.rows] == ["i-3", "i-1"]


@pytest.mark.asyncio
async def test_collection_lib_feed_query_uses_cursor_and_over_fetch() -> None:
    collection = _FakeCollection()
    base = datetime(2026, 1, 1, tzinfo=UTC)
    lib = mongox.CollectionRepository(collection, Item)
    collection.insert_one(
        {"_id": "i-1", "name": "alpha", "created_at": base + timedelta(seconds=1)}
    )
    collection.insert_one(
        {"_id": "i-2", "name": "beta", "created_at": base + timedelta(seconds=2)}
    )
    collection.insert_one(
        {"_id": "i-3", "name": "gamma", "created_at": base + timedelta(seconds=3)}
    )
    collection.insert_one(
        {"_id": "i-4", "name": "delta", "created_at": base + timedelta(seconds=4)}
    )

    first_page = await lib.feed_query(
        mongox.FeedQueryInput(
            pager=model.Pager(page_size=2),
            is_descending=True,
            cursor_field="created_at",
        )
    )
    second_page = await lib.feed_query(
        mongox.FeedQueryInput(
            pager=model.Pager(page_size=2, page_token=first_page.next_page_token),
            is_descending=True,
            cursor_field="created_at",
        )
    )

    assert [row.id for row in first_page.rows] == ["i-4", "i-3"]
    assert first_page.next_page_token != ""
    assert [row.id for row in second_page.rows] == ["i-2", "i-1"]
    assert second_page.next_page_token == ""


@pytest.mark.asyncio
async def test_collection_lib_feed_query_parses_int_id_token() -> None:
    collection = _FakeCollection()
    collection.insert_one({"_id": 1, "name": "alpha"})
    collection.insert_one({"_id": 2, "name": "beta"})
    collection.insert_one({"_id": 3, "name": "gamma"})
    lib = mongox.CollectionRepository(collection, IntIDItem)

    first_page = await lib.feed_query(
        mongox.FeedQueryInput(
            pager=model.Pager(page_size=2),
            is_descending=True,
        )
    )
    second_page = await lib.feed_query(
        mongox.FeedQueryInput(
            pager=model.Pager(page_size=2, page_token=first_page.next_page_token),
            is_descending=True,
        )
    )

    assert [row.id for row in first_page.rows] == [3, 2]
    assert first_page.next_page_token == "2"
    assert [row.id for row in second_page.rows] == [1]
    assert second_page.next_page_token == ""


@pytest.mark.asyncio
async def test_collection_lib_feed_query_reports_invalid_cursor_token_as_validation_error() -> None:
    collection = _FakeCollection()
    collection.insert_one({"_id": 1, "name": "alpha"})
    lib = mongox.CollectionRepository(collection, IntIDItem)

    with pytest.raises(data.ValidationError) as exc_info:
        await lib.feed_query(
            mongox.FeedQueryInput(
                pager=model.Pager(page_size=2, page_token="bad"),
                is_descending=True,
            )
        )

    assert data.code_of(exc_info.value) == data.ERR_CODE_VALIDATE


@pytest.mark.asyncio
async def test_collection_lib_feed_query_reports_invalid_bool_cursor_token() -> None:
    collection = _FakeCollection()
    collection.insert_one({"_id": "i-1", "enabled": False})
    lib = mongox.CollectionRepository(collection, BoolCursorItem)

    with pytest.raises(data.ValidationError) as exc_info:
        await lib.feed_query(
            mongox.FeedQueryInput(
                pager=model.Pager(page_size=2, page_token="maybe"),
                cursor_field="enabled",
            )
        )

    assert data.code_of(exc_info.value) == data.ERR_CODE_VALIDATE


@pytest.mark.asyncio
async def test_collection_lib_feed_query_reports_missing_cursor_field_as_validation_error() -> None:
    collection = _FakeCollection()
    collection.insert_one({"_id": "i-1", "name": "alpha"})
    lib = mongox.CollectionRepository(collection, Item)

    with pytest.raises(data.ValidationError) as exc_info:
        await lib.feed_query(
            mongox.FeedQueryInput(
                pager=model.Pager(page_size=2, page_token="n-1"),
                cursor_field="missing_field",
            )
        )

    assert data.code_of(exc_info.value) == data.ERR_CODE_VALIDATE


@pytest.mark.asyncio
async def test_collection_lib_feed_query_parses_snowflake_id_token() -> None:
    collection = _FakeCollection()
    collection.insert_one({"_id": 623949464310157349, "name": "alpha"})
    collection.insert_one({"_id": 623949464310157350, "name": "beta"})
    collection.insert_one({"_id": 623949464310157351, "name": "gamma"})
    lib = mongox.CollectionRepository(collection, SnowflakeIDItem)

    first_page = await lib.feed_query(
        mongox.FeedQueryInput(
            pager=model.Pager(page_size=2),
            is_descending=True,
        )
    )
    second_page = await lib.feed_query(
        mongox.FeedQueryInput(
            pager=model.Pager(page_size=2, page_token=first_page.next_page_token),
            is_descending=True,
        )
    )

    assert [row.id for row in first_page.rows] == [
        model.SnowflakeID(623949464310157351),
        model.SnowflakeID(623949464310157350),
    ]
    assert first_page.next_page_token == "623949464310157350"
    assert [row.id for row in second_page.rows] == [model.SnowflakeID(623949464310157349)]


@pytest.mark.asyncio
async def test_collection_lib_codec_supports_bson_metadata() -> None:
    with context.use_context():
        _seed_context()
        collection = _FakeCollection()
        lib = mongox.CollectionRepository[str, TaggedItem](collection, TaggedItem)

        item = TaggedItem(
            id="i-1", name="hello", inline_meta=InlineMeta(note="memo"), ignored="skip"
        )
        await lib.create(item)
        loaded = await lib.get("i-1")

        raw = collection.find_one({"_id": "i-1"})
        assert raw is not None
        assert raw == {
            "_id": "i-1",
            "created_at": item.created_at,
            "created_by": "user-1",
            "updated_at": item.updated_at,
            "updated_by": "user-1",
            "tenant_id": "tenant-1",
            "app_id": "app-1",
            "str_field": "hello",
            "note_text": "memo",
        }
        assert loaded.name == "hello"
        assert loaded.inline_meta.note == "memo"
        assert loaded.ignored == ""


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
async def test_collection_lib_patch_raw_encodes_dataclass_values() -> None:
    with context.use_context():
        _seed_context()
        collection = _FakeCollection()
        lib = mongox.CollectionRepository(collection, Item)
        await lib.create(Item(id="i-1", name="before"))

        await lib.patch_raw(
            mongox.PatchRawInput(
                filter={"_id": "i-1"},
                patch_payload={
                    "nested": NestedPatchValue(
                        title="topic",
                        inline_meta=InlineMeta(note="memo"),
                    ),
                    "items": [InlineMeta(note="one")],
                },
                is_many=False,
            )
        )

    raw = collection.find_one({"_id": "i-1"})
    assert raw is not None
    assert raw["nested"] == {
        "title": "topic",
        "inline_meta": {"note_text": "memo"},
    }
    assert raw["items"] == [{"note_text": "one"}]


@pytest.mark.asyncio
async def test_collection_lib_reports_structured_error_when_isolation_context_is_missing() -> None:
    context.clear_current_context()
    collection = _FakeCollection()
    lib = mongox.CollectionRepository(collection, Item).with_repo_opt(
        model.RepoOpt(data_isolation=model.DATA_ISOLATION_TENANT)
    )

    with pytest.raises(data.ValidationError) as exc_info:
        await lib.find({})

    assert exc_info.value.message == "there is no tenant id in context"


@pytest.mark.asyncio
async def test_collection_lib_applies_user_tenant_and_app_isolation() -> None:
    collection = _FakeCollection()
    collection.insert_one(
        {
            "_id": "i-1",
            "name": "match",
            "created_by": "user-1",
            "tenant_id": "tenant-1",
            "app_id": "app-1",
        }
    )
    collection.insert_one(
        {
            "_id": "i-2",
            "name": "other",
            "created_by": "user-2",
            "tenant_id": "tenant-2",
            "app_id": "app-2",
        }
    )

    with context.use_context():
        _seed_context()
        user_rows = (
            await mongox.CollectionRepository(collection, Item)
            .with_repo_opt(model.RepoOpt(data_isolation=model.DATA_ISOLATION_USER))
            .find({})
        )
        tenant_rows = (
            await mongox.CollectionRepository(collection, Item)
            .with_repo_opt(model.RepoOpt(data_isolation=model.DATA_ISOLATION_TENANT))
            .find({})
        )
        app_rows = (
            await mongox.CollectionRepository(collection, Item)
            .with_repo_opt(model.RepoOpt(data_isolation=model.DATA_ISOLATION_APP))
            .find({})
        )

    assert [row.id for row in user_rows] == ["i-1"]
    assert [row.id for row in tenant_rows] == ["i-1"]
    assert [row.id for row in app_rows] == ["i-1"]


@pytest.mark.asyncio
async def test_collection_lib_without_entity_type_keeps_raw_collection_behavior() -> None:
    context.clear_current_context()
    collection = _FakeCollection()
    collection.insert_one({"_id": "i-1", "tenant_id": "tenant-1"})
    lib: mongox.CollectionRepository[Any, Any] = mongox.CollectionRepository(
        collection
    ).with_repo_opt(model.RepoOpt(data_isolation=model.DATA_ISOLATION_TENANT))

    rows = await lib.find({})

    assert rows == [{"_id": "i-1", "tenant_id": "tenant-1"}]


def _seed_context() -> None:
    context.set_operator("user-1")
    context.set_tenant_id("tenant-1")
    context.set_app_id("app-1")

def _matches(doc: dict[str, Any], filter_: dict[str, Any]) -> bool:
    if "$and" in filter_:
        return all(_matches(doc, item) for item in filter_["$and"])
    if "$or" in filter_:
        return any(_matches(doc, item) for item in filter_["$or"])
    for key, expected in filter_.items():
        if key == "$and":
            continue
        if key == "$or":
            continue
        actual = doc.get(key)
        if isinstance(expected, dict):
            if "$exists" in expected and (key in doc) is not bool(expected["$exists"]):
                return False
            if "$in" in expected and actual not in expected["$in"]:
                return False
            if "$lt" in expected and not (actual < expected["$lt"]):
                return False
            if "$gt" in expected and not (actual > expected["$gt"]):
                return False
            continue
        if actual != expected:
            return False
    return True
