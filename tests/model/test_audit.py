from dataclasses import dataclass

import pytest

from core_py import context, data, model


@dataclass
class Audited(model.CreateAudit, model.UpdateAudit, model.DeleteAudit, model.TenantAudit):
    pass


def test_audit_helpers_fill_context_values() -> None:
    with context.use_context():
        context.set_operator("user-1")
        context.set_tenant_id("tenant-1")
        context.set_app_id("app-1")

        item = Audited()
        model.create_audit(item)
        deleted, _ = model.delete_audit(item)

        assert item.created_by == "user-1"
        assert item.updated_by == "user-1"
        assert item.deleted_by == "user-1"
        assert item.tenant_id == "tenant-1"
        assert item.app_id == "app-1"
        assert deleted is True


def test_update_lock_and_audit_returns_original_updated_time() -> None:
    with context.use_context():
        context.set_operator("user-1")

        item = Audited()
        item.updated_at = 123  # type: ignore[assignment]

        result = model.update_lock_and_audit(item, model.RepoOpt())

        assert result.has_original_update is True
        assert result.original_updated_at == 123
        assert item.updated_by == "user-1"


def test_audit_helpers_raise_structured_error_without_context() -> None:
    context.clear_current_context()

    with pytest.raises(data.BaseError) as exc_info:
        model.create_audit(Audited())

    assert exc_info.value.code == data.ERR_CODE_VALIDATE
    assert exc_info.value.message == "there is no user in context"
