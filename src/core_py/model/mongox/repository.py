from __future__ import annotations

import asyncio
import dataclasses
from typing import Any, Generic, TypeVar, cast, get_type_hints

from core_py import context, data
from core_py import model as model_mod
from core_py._async import collect_async_iterable, maybe_await
from core_py.model.mongox.codec import (
    decode_document,
    field_bson_meta,
    get_creator_info,
    get_id,
    is_duplicate_error,
    parse_bson_meta,
    supports,
    to_bson_value,
    to_document,
    type_supports,
)
from core_py.model.mongox.patch import build_patch_payload
from core_py.model.mongox.query import (
    FeedQueryInput,
    PageQueryInput,
    PatchRawInput,
    feed_cursor_filter,
    merge_feed_filter,
    normalize_feed_cursor_field,
    page_limit_skip,
    parse_feed_page_token,
    parse_feed_token,
    sort_conf,
)
from core_py.model.snowflake_id import SnowflakeID

P = TypeVar("P", str, int, SnowflakeID)
T = TypeVar("T")


class CollectionRepository(Generic[P, T]):
    def __init__(self, collection: Any, entity_type: type[T] | None = None) -> None:
        if collection is None:
            raise data.new_validation_error("collection should not be empty")
        self._collection = collection
        self._entity_type = entity_type
        self._id_key = "_id"
        self._opt = model_mod.RepoOpt()

    def with_repo_opt(self, opt: model_mod.RepoOpt | None) -> CollectionRepository[P, T]:
        self._opt = opt or model_mod.RepoOpt()
        return self

    def get_merged_repo_opt(self) -> model_mod.RepoOpt:
        return model_mod.merge_repo_opt(self._opt)

    def inject_cond(self, filter_: dict[str, Any] | None) -> dict[str, Any]:
        ret = cast(dict[str, Any], to_bson_value(dict(filter_ or {})))
        self._inject_isolation_cond(ret)
        self._inject_soft_delete_cond(ret)
        return ret

    async def find(self, filter_: dict[str, Any] | None = None, **find_opts: Any) -> list[T]:
        injected = self.inject_cond(filter_)
        cursor = await maybe_await(self._collection.find(injected, **find_opts))
        rows = await collect_async_iterable(cursor)
        return [cast(T, decode_document(self._entity_type, doc, self._id_key)) for doc in rows]

    async def page_query(self, input_: PageQueryInput) -> model_mod.PagedResult[T]:
        filter_ = self.inject_cond(input_.filter)
        total_count = int(await maybe_await(self._collection.count_documents(filter_)))
        find_opts: dict[str, Any] = {}
        limit, skip = page_limit_skip(input_.pager)
        if limit > 0:
            find_opts["limit"] = limit
        if skip > 0:
            find_opts["skip"] = skip
        sort = sort_conf(input_.sort)
        if not sort and self._supports_create_audit():
            sort = [("created_at", 1)]
        if sort:
            find_opts["sort"] = sort
        cursor = await maybe_await(self._collection.find(filter_, **find_opts))
        rows = [
            cast(T, decode_document(self._entity_type, doc, self._id_key))
            for doc in await collect_async_iterable(cursor)
        ]
        return model_mod.PagedResult(rows=rows, total_count=total_count)

    async def feed_query(self, input_: FeedQueryInput) -> model_mod.FeedResult[T]:
        filter_ = self.inject_cond(input_.filter)
        page_size, _, page_token = 0, 0, ""
        if input_.pager is not None:
            if hasattr(input_.pager, "get_page_info"):
                page_size, _, page_token = input_.pager.get_page_info()
            else:
                page_size = int(getattr(input_.pager, "page_size", 0) or 0)
                page_token = str(getattr(input_.pager, "page_token", "") or "")
        if page_size <= 0:
            page_size = 20

        cursor_field = normalize_feed_cursor_field(input_.cursor_field)
        if page_token:
            cursor_field_type = self._feed_cursor_field_type(cursor_field)
            if cursor_field_type is None and cursor_field != self._id_key:
                raise data.new_validation_error(f"cursor field {cursor_field} not found")
            cursor_value = parse_feed_token(
                page_token,
                cursor_field_type,
            )
            filter_ = merge_feed_filter(
                filter_,
                feed_cursor_filter(cursor_field, cursor_value, input_.is_descending),
            )

        find_opts: dict[str, Any] = {"limit": page_size + 1}
        find_opts["sort"] = feed_sort_conf(cursor_field, input_.is_descending)

        cursor = await maybe_await(self._collection.find(filter_, **find_opts))
        rows = [
            cast(T, decode_document(self._entity_type, doc, self._id_key))
            for doc in await collect_async_iterable(cursor)
        ]
        next_page_token = ""
        if len(rows) > page_size:
            rows = rows[:page_size]
            next_page_token = self._feed_cursor_token(rows[-1], cursor_field)
        return model_mod.FeedResult(rows=rows, next_page_token=next_page_token)

    def _feed_cursor_token(self, row: T, cursor_field: str) -> str:
        if cursor_field == "_id":
            return str(get_id(row))
        if isinstance(row, dict):
            return str(row[cursor_field])
        return str(getattr(row, cursor_field))

    def _feed_cursor_field_type(self, cursor_field: str) -> Any | None:
        if self._entity_type is None:
            return None
        if cursor_field == self._id_key:
            return _dataclass_field_type(self._entity_type, "id")
        return _bson_field_type(self._entity_type, cursor_field)

    async def get(self, id_: P) -> T:
        return await self.get_by_filter({self._id_key: id_})

    async def get_by_filter(self, filter_: dict[str, Any]) -> T:
        injected = self.inject_cond(filter_)
        opt = self.get_merged_repo_opt()
        attempts = 3 if opt.try_fix_sync_delay == model_mod.FIXED_STRATEGY_BACKOFF else 1
        last_err: Exception | None = None
        for idx in range(attempts):
            doc = await maybe_await(self._collection.find_one(injected))
            if doc is not None:
                return cast(T, decode_document(self._entity_type, doc, self._id_key))
            last_err = data.new_resource_not_found_error(self._resource_by_filter(injected))
            if idx < attempts - 1:
                await asyncio.sleep(0.05)
        raise last_err or data.new_resource_not_found_error(self._resource_by_filter(injected))

    async def create(self, doc: T) -> T:
        self._check_id_existed(doc)
        model_mod.create_audit(doc)
        payload = to_document(doc, self._id_key)
        try:
            await maybe_await(self._collection.insert_one(payload))
        except Exception as exc:
            if is_duplicate_error(exc):
                raise data.new_resource_conflict_error(self._resource_by_doc(doc)) from exc
            raise
        return doc

    async def update(self, doc: T) -> T:
        return await self._common_replace(doc, is_upsert=False)

    async def upsert(self, doc: T) -> T:
        return await self._common_replace(doc, is_upsert=True)

    async def patch(self, doc: T) -> None:
        filter_ = self._base_update_filter(doc)
        patch_payload = build_patch_payload(doc)
        await self.patch_raw(
            PatchRawInput(
                filter=filter_,
                patch_payload=patch_payload,
                is_many=False,
                skip_inject_cond=True,
            )
        )

    async def patch_raw(self, input_: PatchRawInput) -> None:
        filter_ = (
            dict(input_.filter) if input_.skip_inject_cond else self.inject_cond(input_.filter)
        )
        payload = {
            key: to_bson_value(value, self._id_key) for key, value in input_.patch_payload.items()
        }
        if self._supports_update_audit():
            user, ok = context.get_operator()
            if not ok:
                raise data.new_validate_error("there is no user in context")
            payload["updated_by"] = user
            payload["updated_at"] = model_mod._now()

        update = {"$set": payload}
        if input_.is_many:
            result = await maybe_await(self._collection.update_many(filter_, update))
        else:
            result = await maybe_await(self._collection.update_one(filter_, update))
        if int(getattr(result, "matched_count", 0)) == 0:
            raise data.new_resource_not_found_error(self._resource_by_filter(filter_))
        if "updated_at" in filter_ and int(getattr(result, "modified_count", 0)) == 0:
            raise data.new_resource_error(
                data.ERR_CODE_CONFLICT,
                "optimistic locking failed",
                self._resource_by_filter(filter_),
            )

    async def delete(self, doc: T) -> None:
        has_delete_audit, _ = model_mod.delete_audit(doc)
        opt = self.get_merged_repo_opt()
        if has_delete_audit and opt.soft_delete != model_mod.SOFT_DELETE_DISABLE:
            await self.update(doc)
            return

        filter_ = self.inject_cond({self._id_key: get_id(doc)})
        result = await maybe_await(self._collection.delete_one(filter_))
        if int(getattr(result, "deleted_count", 0)) == 0:
            raise data.new_resource_not_found_error(self._resource_by_doc(doc))

    async def batch_create(self, docs: list[T]) -> None:
        payloads: list[dict[str, Any]] = []
        for doc in docs:
            self._check_id_existed(doc)
            model_mod.create_audit(doc)
            payloads.append(to_document(doc, self._id_key))
        try:
            if payloads:
                await maybe_await(self._collection.insert_many(payloads))
        except Exception as exc:
            if is_duplicate_error(exc):
                raise data.new_resource_conflict_error(self._resource_by_docs(docs)) from exc
            raise

    async def batch_update(self, docs: list[T]) -> None:
        for doc in docs:
            await self.update(doc)

    async def batch_delete(self, docs: list[T]) -> tuple[int, None]:
        return await self.batch_delete_by_ids([get_id(doc) for doc in docs])

    async def batch_delete_by_ids(self, ids: list[P]) -> tuple[int, None]:
        if not ids:
            return 0, None
        return await self.batch_delete_by_filter({self._id_key: {"$in": ids}})

    async def batch_delete_by_filter(self, filter_: dict[str, Any]) -> tuple[int, None]:
        injected = self.inject_cond(filter_)
        opt = self.get_merged_repo_opt()
        if self._supports_delete_audit() and opt.soft_delete != model_mod.SOFT_DELETE_DISABLE:
            user, ok = context.get_operator()
            if not ok:
                raise data.new_validate_error("there is no user")
            result = await maybe_await(
                self._collection.update_many(
                    injected,
                    {"$set": {"deleted_at": model_mod._now(), "deleted_by": user}},
                )
            )
            matched = int(getattr(result, "matched_count", 0))
            if matched == 0:
                raise data.new_resource_not_found_error(self._resource_by_filter(injected))
            return matched, None

        result = await maybe_await(self._collection.delete_many(injected))
        deleted = int(getattr(result, "deleted_count", 0))
        if deleted == 0:
            raise data.new_resource_not_found_error(self._resource_by_filter(injected))
        return deleted, None

    async def _common_replace(self, doc: T, is_upsert: bool) -> T:
        filter_ = self._audit_and_build_replace_filter(doc, is_upsert)
        payload = to_document(doc, self._id_key)
        try:
            result = await maybe_await(
                self._collection.replace_one(filter_, payload, upsert=is_upsert)
            )
        except Exception as exc:
            if is_duplicate_error(exc):
                raise data.new_resource_conflict_error(self._resource_by_doc(doc)) from exc
            raise

        if is_upsert and int(getattr(result, "upserted_count", 0)) > 0:
            return doc
        if int(getattr(result, "matched_count", 0)) == 0:
            check_filter = dict(filter_)
            check_filter.pop("updated_at", None)
            if int(await maybe_await(self._collection.count_documents(check_filter))) > 0:
                raise data.new_resource_error(
                    data.ERR_CODE_CONFLICT,
                    "data is modified by other",
                    self._resource_by_filter(filter_),
                )
            raise data.new_resource_not_found_error(self._resource_by_filter(filter_))
        if "updated_at" in filter_ and int(getattr(result, "modified_count", 0)) == 0:
            raise data.new_resource_error(
                data.ERR_CODE_CONFLICT,
                "optimistic locking failed",
                self._resource_by_filter(filter_),
            )
        return doc

    def _audit_and_build_replace_filter(self, doc: T, is_upsert: bool) -> dict[str, Any]:
        if is_upsert and supports(doc, "get_creator_info"):
            _, created_at = get_creator_info(doc)
            if not created_at:
                model_mod.create_audit(doc)
                return {self._id_key: get_id(doc)}
        return self._base_update_filter(doc)

    def _base_update_filter(self, doc: T) -> dict[str, Any]:
        ret = model_mod.update_lock_and_audit(doc, self.get_merged_repo_opt())
        filter_: dict[str, Any] = {self._id_key: get_id(doc)}
        if ret.has_original_update:
            filter_["updated_at"] = ret.original_updated_at
        return self.inject_cond(filter_)

    def _collection_name(self) -> str:
        name = getattr(self._collection, "name", "")
        if name:
            return str(name)
        return type(self._collection).__name__

    def _resource_by_id(self, id_: Any) -> str:
        return f"{self._collection_name()}/{id_}"

    def _resource_by_doc(self, doc: T) -> str:
        return self._resource_by_id(get_id(doc))

    def _resource_by_docs(self, docs: list[T]) -> str:
        ids = [get_id(doc) for doc in docs]
        return f"{self._collection_name()} ids={ids}"

    def _resource_by_filter(self, filter_: dict[str, Any]) -> str:
        if self._id_key in filter_:
            return self._resource_by_id(filter_[self._id_key])
        return f"{self._collection_name()} filter={filter_}"

    def _check_id_existed(self, doc: T) -> None:
        if not get_id(doc):
            raise data.new_validation_error("id can not be empty")

    def _inject_isolation_cond(self, filter_: dict[str, Any]) -> None:
        opt = self.get_merged_repo_opt()
        if opt.data_isolation == model_mod.DATA_ISOLATION_USER and self._supports_create_audit():
            user, ok = context.get_operator()
            if not ok:
                raise data.new_validate_error("there is no user id in context")
            filter_["created_by"] = user
        elif (
            opt.data_isolation == model_mod.DATA_ISOLATION_TENANT and self._supports_tenant_audit()
        ):
            tenant_id, ok = context.get_tenant_id()
            if not ok:
                raise data.new_validate_error("there is no tenant id in context")
            filter_["tenant_id"] = tenant_id
        elif opt.data_isolation == model_mod.DATA_ISOLATION_APP and self._supports_tenant_audit():
            app_id, ok = context.get_app_id()
            if not ok:
                raise data.new_validate_error("there is no app id in context")
            filter_["app_id"] = app_id

    def _inject_soft_delete_cond(self, filter_: dict[str, Any]) -> None:
        opt = self.get_merged_repo_opt()
        if opt.soft_delete == model_mod.SOFT_DELETE_DISABLE:
            return
        if self._supports_delete_audit():
            filter_["deleted_at"] = {"$exists": False}

    def _supports_create_audit(self) -> bool:
        return type_supports(self._entity_type, "set_creator")

    def _supports_update_audit(self) -> bool:
        return type_supports(self._entity_type, "set_updater")

    def _supports_delete_audit(self) -> bool:
        return type_supports(self._entity_type, "set_deleter")

    def _supports_tenant_audit(self) -> bool:
        return type_supports(self._entity_type, "set_tenant_id", "set_app_id")


def _dataclass_field_type(entity_type: type[Any], field_name: str) -> Any | None:
    if not dataclasses.is_dataclass(entity_type):
        return None
    type_hints = get_type_hints(entity_type)
    return type_hints.get(field_name)


def _bson_field_type(entity_type: type[Any], cursor_field: str) -> Any | None:
    if not dataclasses.is_dataclass(entity_type):
        return None
    type_hints = get_type_hints(entity_type)
    for field_ in dataclasses.fields(entity_type):
        mapped_name, inline, skip = parse_bson_meta(field_.name, field_bson_meta(field_))
        if skip:
            continue
        field_type = type_hints.get(field_.name, field_.type)
        if inline and isinstance(field_type, type):
            nested = _bson_field_type(field_type, cursor_field)
            if nested is not None:
                return nested
            continue
        if mapped_name == cursor_field:
            return field_type
    return None
