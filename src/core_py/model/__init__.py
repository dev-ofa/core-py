"""Domain model helpers: entities, audit fields and repository options."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Generic, Protocol, TypeVar

from core_py import context, data

P = TypeVar("P", str, int)
T = TypeVar("T")


def _now() -> datetime:
    return datetime.now(UTC)


def _now_ms() -> int:
    return int(_now().timestamp() * 1000)


def _missing_context_error(message: str) -> data.BaseError:
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


@dataclass
class Entity(Generic[P]):
    id: P | None = None
    create_audit: CreateAudit = field(default_factory=CreateAudit)

    def get_id(self) -> P | None:
        return self.id

    def set_id(self, id_: P) -> None:
        self.id = id_


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


class Repo(Protocol[P, T]):
    async def get(self, id_: P) -> T: ...
    async def create(self, doc: T) -> T: ...
    async def update(self, doc: T) -> T: ...
    async def upsert(self, doc: T) -> T: ...
    async def patch(self, doc: T) -> None: ...
    async def delete(self, doc: T) -> None: ...
    async def batch_create(self, docs: list[T]) -> None: ...
    async def batch_update(self, docs: list[T]) -> None: ...
    async def batch_delete(self, docs: list[T]) -> tuple[int, None]: ...
    async def batch_delete_by_ids(self, ids: list[P]) -> tuple[int, None]: ...


@dataclass
class RepoOpt:
    deploy_isolation: str = ""
    data_isolation: str = ""
    update_run_context: str = ""
    try_fix_sync_delay: str = ""
    soft_delete: str = ""


@dataclass(slots=True)
class UpdatesResult:
    has_original_update: bool = False
    original_updated_at: Any = None


CTX_KEY_REPO_OPT = "repo-opt"
FIXED_STRATEGY_NONE = "none"
FIXED_STRATEGY_BACKOFF = "backoff"
UPDATE_RUN_CONTEXT_AT_CREATING = "at-creating"
UPDATE_RUN_CONTEXT_ALWAYS = "always"
DEPLOY_ISOLATION_NONE = "none"
DEPLOY_ISOLATION_CLUSTER = "cluster"
DEPLOY_ISOLATION_ENV = "env"
DATA_ISOLATION_NONE = "none"
DATA_ISOLATION_TENANT = "tenant"
DATA_ISOLATION_USER = "user"
DATA_ISOLATION_APP = "app"
SOFT_DELETE_ENABLE = ""
SOFT_DELETE_DISABLE = "disable"


def repo_opt_or_new() -> RepoOpt:
    value = context.current_context().get(CTX_KEY_REPO_OPT)
    return value if isinstance(value, RepoOpt) else RepoOpt()


def _set_repo_opt(opt: RepoOpt) -> None:
    current = context.current_context()
    current[CTX_KEY_REPO_OPT] = opt
    context.set_current_context(current)


def set_repo_data_isolation(data_isolation: str) -> None:
    opt = repo_opt_or_new()
    opt.data_isolation = data_isolation
    _set_repo_opt(opt)


def set_repo_deploy_isolation(deploy_isolation: str) -> None:
    opt = repo_opt_or_new()
    opt.deploy_isolation = deploy_isolation
    _set_repo_opt(opt)


def set_repo_update_run_context(update_run_context: str) -> None:
    opt = repo_opt_or_new()
    opt.update_run_context = update_run_context
    _set_repo_opt(opt)


def set_soft_delete(soft_delete: str) -> None:
    opt = repo_opt_or_new()
    opt.soft_delete = soft_delete
    _set_repo_opt(opt)


def set_fixed_strategy(fixed_strategy: str) -> None:
    opt = repo_opt_or_new()
    opt.try_fix_sync_delay = fixed_strategy
    _set_repo_opt(opt)


def merge_repo_opt(merge: RepoOpt | None) -> RepoOpt:
    opt = repo_opt_or_new()
    if merge is None:
        return opt
    return RepoOpt(
        deploy_isolation=opt.deploy_isolation or merge.deploy_isolation,
        data_isolation=opt.data_isolation or merge.data_isolation,
        update_run_context=opt.update_run_context or merge.update_run_context,
        try_fix_sync_delay=opt.try_fix_sync_delay or merge.try_fix_sync_delay,
        soft_delete=opt.soft_delete or merge.soft_delete,
    )


def create_audit(entity: Any) -> None:
    if hasattr(entity, "set_creator"):
        user, ok = context.get_operator()
        if not ok:
            raise _missing_context_error("there is no user in context")
        entity.set_creator(user)
    if hasattr(entity, "set_tenant_id") and hasattr(entity, "set_app_id"):
        tenant_id, ok = context.get_tenant_id()
        if not ok:
            raise _missing_context_error("there is no tenantid in context")
        app_id, ok = context.get_app_id()
        if not ok:
            raise _missing_context_error("there is no appid in context")
        entity.set_tenant_id(tenant_id)
        entity.set_app_id(app_id)
    update_audit(entity)


def update_audit(entity: Any) -> None:
    if hasattr(entity, "set_updater"):
        user, ok = context.get_operator()
        if not ok:
            raise _missing_context_error("there is no user in context")
        entity.set_updater(user)


def update_audit_and_env(entity: Any) -> None:
    update_audit(entity)


def update_lock_and_audit(entity: Any, opt: RepoOpt | None = None) -> UpdatesResult:
    repo_opt = opt or RepoOpt()
    original_updated_at = None
    if hasattr(entity, "get_update_time_raw"):
        original_updated_at = entity.get_update_time_raw()

    if repo_opt.update_run_context == UPDATE_RUN_CONTEXT_ALWAYS:
        update_audit_and_env(entity)
    else:
        update_audit(entity)

    return UpdatesResult(
        has_original_update=bool(original_updated_at),
        original_updated_at=original_updated_at,
    )


def delete_audit(entity: Any) -> tuple[bool, None]:
    if hasattr(entity, "set_deleter"):
        user, ok = context.get_operator()
        if not ok:
            raise _missing_context_error("there is no user in context")
        entity.set_deleter(user)
        return True, None
    return False, None


def audit(auditors: list[Any]) -> None:
    for auditor in auditors:
        if hasattr(auditor, "set_creator"):
            user, ok = context.get_operator()
            if not ok:
                raise _missing_context_error("there is no user in context")
            auditor.set_creator(user)
        elif hasattr(auditor, "set_updater"):
            user, ok = context.get_operator()
            if not ok:
                raise _missing_context_error("there is no user in context")
            auditor.set_updater(user)
        elif hasattr(auditor, "set_deleter"):
            user, ok = context.get_operator()
            if not ok:
                raise _missing_context_error("there is no user in context")
            auditor.set_deleter(user)
        if hasattr(auditor, "set_tenant_id") and hasattr(auditor, "set_app_id"):
            tenant_id, ok = context.get_tenant_id()
            if not ok:
                raise _missing_context_error("there is no tenantid in context")
            app_id, ok = context.get_app_id()
            if not ok:
                raise _missing_context_error("there is no appid in context")
            auditor.set_tenant_id(tenant_id)
            auditor.set_app_id(app_id)
