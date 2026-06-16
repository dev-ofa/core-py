"""Repository options and audit helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core_py import context
from core_py.model.audit import _missing_context_error


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


def update_lock_and_audit(
    entity: Any,
    opt: RepoOpt | None = None,
) -> UpdatesResult:
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
