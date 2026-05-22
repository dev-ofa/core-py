"""Mongo repository helpers aligned with core-go model/mongox."""

from __future__ import annotations

import asyncio
import dataclasses
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar, cast

from core_py import context, data
from core_py import model as model_mod
from core_py._async import collect_async_iterable, maybe_await

P = TypeVar("P", str, int)
T = TypeVar("T")


@dataclass(slots=True)
class PageQueryInput:
    filter: dict[str, Any] = field(default_factory=dict)
    pager: Any = None
    sort: Any = None


@dataclass(slots=True)
class PatchRawInput:
    filter: dict[str, Any] = field(default_factory=dict)
    patch_payload: dict[str, Any] = field(default_factory=dict)
    is_many: bool = False
    skip_inject_cond: bool = False


class CollectionRepository(Generic[P, T]):
    def __init__(self, collection: Any, entity_type: type[T] | None = None) -> None:
        if collection is None:
            raise ValueError("collection should not be empty")
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
        ret = dict(filter_ or {})
        self._inject_isolation_cond(ret)
        self._inject_soft_delete_cond(ret)
        return ret

    async def find(self, filter_: dict[str, Any] | None = None, **find_opts: Any) -> list[T]:
        injected = self.inject_cond(filter_)
        cursor = await maybe_await(self._collection.find(injected, **find_opts))
        rows = await collect_async_iterable(cursor)
        return [self._decode(doc) for doc in rows]

    async def page_query(self, input_: PageQueryInput) -> model_mod.PagedResult[T]:
        filter_ = self.inject_cond(input_.filter)
        total_count = int(await maybe_await(self._collection.count_documents(filter_)))
        find_opts: dict[str, Any] = {}
        limit, skip = _page_limit_skip(input_.pager)
        if limit > 0:
            find_opts["limit"] = limit
        if skip > 0:
            find_opts["skip"] = skip
        sort = _sort_conf(input_.sort)
        if not sort and self._supports_create_audit():
            sort = [("created_at", 1)]
        if sort:
            find_opts["sort"] = sort
        cursor = await maybe_await(self._collection.find(filter_, **find_opts))
        rows = [self._decode(doc) for doc in await collect_async_iterable(cursor)]
        return model_mod.PagedResult(rows=rows, total_count=total_count)

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
                return self._decode(doc)
            last_err = data.ERR_NOT_FOUND
            if idx < attempts - 1:
                await asyncio.sleep(0.05)
        raise last_err or data.ERR_NOT_FOUND

    async def create(self, doc: T) -> T:
        self._check_id_existed(doc)
        model_mod.create_audit(doc)
        payload = _to_document(doc, self._id_key)
        try:
            await maybe_await(self._collection.insert_one(payload))
        except Exception as exc:
            if _is_duplicate_error(exc):
                raise data.ERR_CONFLICT from exc
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
        filter_ = dict(input_.filter) if input_.skip_inject_cond else self.inject_cond(input_.filter)
        payload = dict(input_.patch_payload)
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
            raise data.ERR_NOT_FOUND
        if "updated_at" in filter_ and int(getattr(result, "modified_count", 0)) == 0:
            raise data.new_conflict_error("optimistic locking failed")

    async def delete(self, doc: T) -> None:
        has_delete_audit, _ = model_mod.delete_audit(doc)
        opt = self.get_merged_repo_opt()
        if has_delete_audit and opt.soft_delete != model_mod.SOFT_DELETE_DISABLE:
            await self.update(doc)
            return

        filter_ = self.inject_cond({self._id_key: _get_id(doc)})
        result = await maybe_await(self._collection.delete_one(filter_))
        if int(getattr(result, "deleted_count", 0)) == 0:
            raise data.ERR_NOT_FOUND

    async def batch_create(self, docs: list[T]) -> None:
        payloads: list[dict[str, Any]] = []
        for doc in docs:
            self._check_id_existed(doc)
            model_mod.create_audit(doc)
            payloads.append(_to_document(doc, self._id_key))
        try:
            if payloads:
                await maybe_await(self._collection.insert_many(payloads))
        except Exception as exc:
            if _is_duplicate_error(exc):
                raise data.ERR_CONFLICT from exc
            raise

    async def batch_update(self, docs: list[T]) -> None:
        for doc in docs:
            await self.update(doc)

    async def batch_delete(self, docs: list[T]) -> tuple[int, None]:
        return await self.batch_delete_by_ids([_get_id(doc) for doc in docs])

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
                raise data.ERR_NOT_FOUND
            return matched, None

        result = await maybe_await(self._collection.delete_many(injected))
        deleted = int(getattr(result, "deleted_count", 0))
        if deleted == 0:
            raise data.ERR_NOT_FOUND
        return deleted, None

    async def _common_replace(self, doc: T, is_upsert: bool) -> T:
        filter_ = self._audit_and_build_replace_filter(doc, is_upsert)
        payload = _to_document(doc, self._id_key)
        try:
            result = await maybe_await(self._collection.replace_one(filter_, payload, upsert=is_upsert))
        except Exception as exc:
            if _is_duplicate_error(exc):
                raise data.ERR_CONFLICT from exc
            raise

        if is_upsert and int(getattr(result, "upserted_count", 0)) > 0:
            return doc
        if int(getattr(result, "matched_count", 0)) == 0:
            check_filter = dict(filter_)
            check_filter.pop("updated_at", None)
            if int(await maybe_await(self._collection.count_documents(check_filter))) > 0:
                raise data.new_conflict_error("data is modified by other")
            raise data.ERR_NOT_FOUND
        if "updated_at" in filter_ and int(getattr(result, "modified_count", 0)) == 0:
            raise data.new_conflict_error("optimistic locking failed")
        return doc

    def _audit_and_build_replace_filter(self, doc: T, is_upsert: bool) -> dict[str, Any]:
        if is_upsert and _supports(doc, "get_creator_info"):
            _, created_at = _get_creator_info(doc)
            if not created_at:
                model_mod.create_audit(doc)
                return {self._id_key: _get_id(doc)}
        return self._base_update_filter(doc)

    def _base_update_filter(self, doc: T) -> dict[str, Any]:
        ret = model_mod.update_lock_and_audit(doc, self.get_merged_repo_opt())
        filter_: dict[str, Any] = {self._id_key: _get_id(doc)}
        if ret.has_original_update:
            filter_["updated_at"] = ret.original_updated_at
        return self.inject_cond(filter_)

    def _check_id_existed(self, doc: T) -> None:
        if not _get_id(doc):
            raise ValueError("id can not be empty")

    def _inject_isolation_cond(self, filter_: dict[str, Any]) -> None:
        opt = self.get_merged_repo_opt()
        if opt.data_isolation == model_mod.DATA_ISOLATION_USER and self._supports_create_audit():
            user, ok = context.get_operator()
            if not ok:
                raise data.new_validate_error("there is no user id in context")
            filter_["created_by"] = user
        elif opt.data_isolation == model_mod.DATA_ISOLATION_TENANT and self._supports_tenant_audit():
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
        return _type_supports(self._entity_type, "set_creator")

    def _supports_update_audit(self) -> bool:
        return _type_supports(self._entity_type, "set_updater")

    def _supports_delete_audit(self) -> bool:
        return _type_supports(self._entity_type, "set_deleter")

    def _supports_tenant_audit(self) -> bool:
        return _type_supports(self._entity_type, "set_tenant_id", "set_app_id")

    def _decode(self, doc: Any) -> T:
        if self._entity_type is None or not isinstance(doc, dict):
            return cast(T, doc)
        payload = dict(doc)
        if self._id_key in payload:
            payload["id"] = payload.pop(self._id_key)
        try:
            return self._entity_type(**payload)
        except TypeError:
            return cast(T, doc)


def build_patch_payload(value: Any) -> dict[str, Any]:
    return build_patch_payload_with_parent(value, "")


def build_patch_payload_with_parent(value: Any, parent: str) -> dict[str, Any]:
    items: list[tuple[str, Any]]
    if dataclasses.is_dataclass(value):
        items = [(field_.name, getattr(value, field_.name)) for field_ in dataclasses.fields(value)]
    elif isinstance(value, dict):
        items = list(value.items())
    else:
        items = list(vars(value).items()) if hasattr(value, "__dict__") else []

    payload: dict[str, Any] = {}
    for raw_name, field_value in items:
        if raw_name.startswith("_") or field_value is None:
            continue
        name = _field_name(raw_name)
        key = _gen_db_key(parent, name)
        if dataclasses.is_dataclass(field_value):
            payload.update(build_patch_payload_with_parent(field_value, key))
        else:
            payload[key] = field_value
    return payload


def identity(val: T) -> T:
    return val


def _page_limit_skip(pager: Any) -> tuple[int, int]:
    if pager is None:
        return 0, 0
    if hasattr(pager, "get_page_info"):
        page_size, page_num, _ = pager.get_page_info()
    else:
        page_size = int(getattr(pager, "page_size", 0) or 0)
        page_num = int(getattr(pager, "page_num", 0) or 0)
    skip = page_size * (page_num - 1) if page_size > 0 and page_num > 0 else 0
    return page_size, skip


def _sort_conf(sort: Any) -> list[tuple[str, int]]:
    if sort is None:
        return []
    pairs = sort.get_sort_info() if hasattr(sort, "get_sort_info") else []
    return [(pair.field, -1 if pair.is_descending else 1) for pair in pairs]


def _to_document(doc: Any, id_key: str) -> dict[str, Any]:
    if isinstance(doc, dict):
        payload = dict(doc)
    elif dataclasses.is_dataclass(doc):
        payload = {}
        for field_ in dataclasses.fields(doc):
            name = field_.name
            value = getattr(doc, name)
            if value is None:
                continue
            if dataclasses.is_dataclass(value) and name.endswith("_audit"):
                payload.update(_to_document(value, id_key))
            else:
                payload[_field_name(name)] = value
    else:
        payload = {
            _field_name(k): v
            for k, v in vars(doc).items()
            if not k.startswith("_") and v is not None
        }
    if "id" in payload:
        payload[id_key] = payload.pop("id")
    return payload


def _get_id(doc: Any) -> Any:
    if hasattr(doc, "get_id"):
        return doc.get_id()
    if isinstance(doc, dict):
        return doc.get("_id", doc.get("id"))
    return getattr(doc, "id", getattr(doc, "_id", None))


def _get_creator_info(doc: Any) -> tuple[Any, Any]:
    method = getattr(doc, "get_creator_info", None)
    if callable(method):
        return cast(tuple[Any, Any], method())
    return None, None


def _type_supports(entity_type: type[Any] | None, *names: str) -> bool:
    if entity_type is None:
        return True
    return all(hasattr(entity_type, name) for name in names)


def _supports(value: Any, name: str) -> bool:
    return hasattr(value, name)


def _field_name(name: str) -> str:
    return "_id" if name == "id" else name


def _gen_db_key(parent: str, field_key: str) -> str:
    return field_key if not parent else f"{parent}.{field_key}"


def _is_duplicate_error(exc: Exception) -> bool:
    if exc.__class__.__name__ == "DuplicateKeyError":
        return True
    return int(getattr(exc, "code", 0) or 0) == 11000
