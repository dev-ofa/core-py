"""Domain model helpers: entities, audit fields and repository options."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Generic, Protocol, TypeVar

from core_py import context

P = TypeVar("P", str, int)
T = TypeVar("T")


def _now() -> datetime:
    return datetime.now(UTC)


def _now_ms() -> int:
    return int(_now().timestamp() * 1000)


class OperatorCarrier(Protocol):
    def set_user(self, user: str) -> None: ...


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
        self.set_user(user)

    def set_user(self, user: str) -> None:
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
        self.set_user(user)

    def set_user(self, user: str) -> None:
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
        UpdateAudit.set_user(self, user)

    def set_user(self, user: str) -> None:
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
        UpdateAuditMs.set_user(self, user)

    def set_user(self, user: str) -> None:
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
        DeleteAudit.set_user(self, user)

    def set_user(self, user: str) -> None:
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
        DeleteAuditMs.set_user(self, user)

    def set_user(self, user: str) -> None:
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


@dataclass
class RepoOpt:
    deploy_isolation: str = ""
    data_isolation: str = ""
    update_run_context: str = ""
    try_fix_sync_delay: str = ""
    soft_delete: str = ""


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


def ctx_repo_opt_or_new(ctx: context.Context | None) -> RepoOpt:
    value = (ctx or {}).get(CTX_KEY_REPO_OPT)
    return value if isinstance(value, RepoOpt) else RepoOpt()


def _set_ctx_repo_opt(ctx: context.Context | None, opt: RepoOpt) -> dict[str, Any]:
    ret: dict[str, Any] = dict(ctx or {})
    ret[CTX_KEY_REPO_OPT] = opt
    return ret


def set_ctx_repo_data_isolation(ctx: context.Context | None, data_isolation: str) -> dict[str, Any]:
    opt = ctx_repo_opt_or_new(ctx)
    opt.data_isolation = data_isolation
    return _set_ctx_repo_opt(ctx, opt)


def set_ctx_repo_deploy_isolation(
    ctx: context.Context | None, deploy_isolation: str
) -> dict[str, Any]:
    opt = ctx_repo_opt_or_new(ctx)
    opt.deploy_isolation = deploy_isolation
    return _set_ctx_repo_opt(ctx, opt)


def set_ctx_repo_update_run_context(
    ctx: context.Context | None, update_run_context: str
) -> dict[str, Any]:
    opt = ctx_repo_opt_or_new(ctx)
    opt.update_run_context = update_run_context
    return _set_ctx_repo_opt(ctx, opt)


def set_ctx_soft_delete(ctx: context.Context | None, soft_delete: str) -> dict[str, Any]:
    opt = ctx_repo_opt_or_new(ctx)
    opt.soft_delete = soft_delete
    return _set_ctx_repo_opt(ctx, opt)


def set_ctx_fixed_strategy(ctx: context.Context | None, fixed_strategy: str) -> dict[str, Any]:
    opt = ctx_repo_opt_or_new(ctx)
    opt.try_fix_sync_delay = fixed_strategy
    return _set_ctx_repo_opt(ctx, opt)


def ctx_merge_repo_opt(ctx: context.Context | None, merge: RepoOpt | None) -> RepoOpt:
    opt = ctx_repo_opt_or_new(ctx)
    if merge is None:
        return opt
    return RepoOpt(
        deploy_isolation=opt.deploy_isolation or merge.deploy_isolation,
        data_isolation=opt.data_isolation or merge.data_isolation,
        update_run_context=opt.update_run_context or merge.update_run_context,
        try_fix_sync_delay=opt.try_fix_sync_delay or merge.try_fix_sync_delay,
        soft_delete=opt.soft_delete or merge.soft_delete,
    )


def ctx_create_audit(ctx: context.Context | None, entity: Any) -> None:
    if hasattr(entity, "set_creator"):
        user, ok = context.ctx_get_operator(ctx)
        if not ok:
            raise ValueError("there is no user in context")
        entity.set_creator(user)
    if hasattr(entity, "set_tenant_id") and hasattr(entity, "set_app_id"):
        tenant_id, ok = context.ctx_get_tenant_id(ctx)
        if not ok:
            raise ValueError("there is no tenantid in context")
        app_id, ok = context.ctx_get_app_id(ctx)
        if not ok:
            raise ValueError("there is no appid in context")
        entity.set_tenant_id(tenant_id)
        entity.set_app_id(app_id)
    ctx_update_audit(ctx, entity)


def ctx_update_audit(ctx: context.Context | None, entity: Any) -> None:
    if hasattr(entity, "set_updater"):
        user, ok = context.ctx_get_operator(ctx)
        if not ok:
            raise ValueError("there is no user in context")
        entity.set_updater(user)


def ctx_update_audit_and_env(ctx: context.Context | None, entity: Any) -> None:
    ctx_update_audit(ctx, entity)


def ctx_delete_audit(ctx: context.Context | None, entity: Any) -> tuple[bool, None]:
    if hasattr(entity, "set_deleter"):
        user, ok = context.ctx_get_operator(ctx)
        if not ok:
            raise ValueError("there is no user in context")
        entity.set_deleter(user)
        return True, None
    return False, None


def ctx_audit(ctx: context.Context | None, auditors: list[Any]) -> None:
    for auditor in auditors:
        if hasattr(auditor, "set_user"):
            user, ok = context.ctx_get_operator(ctx)
            if not ok:
                raise ValueError("there is no user in context")
            auditor.set_user(user)
        if hasattr(auditor, "set_tenant_id") and hasattr(auditor, "set_app_id"):
            tenant_id, ok = context.ctx_get_tenant_id(ctx)
            if not ok:
                raise ValueError("there is no tenantid in context")
            app_id, ok = context.ctx_get_app_id(ctx)
            if not ok:
                raise ValueError("there is no appid in context")
            auditor.set_tenant_id(tenant_id)
            auditor.set_app_id(app_id)


# Go-style aliases.
CtxCreateAudit = ctx_create_audit
CtxUpdateAudit = ctx_update_audit
CtxUpdateAuditAndEnv = ctx_update_audit_and_env
CtxDeleteAudit = ctx_delete_audit
CtxAudit = ctx_audit
CtxRepoOptOrNew = ctx_repo_opt_or_new
CtxMergeRepoOpt = ctx_merge_repo_opt
SetCtxRepoDataIsolation = set_ctx_repo_data_isolation
SetCtxRepoDeployIsolation = set_ctx_repo_deploy_isolation
SetCtxRepoUpdateRunContext = set_ctx_repo_update_run_context
SetCtxSoftDelete = set_ctx_soft_delete
SetCtxFixedStrategy = set_ctx_fixed_strategy
