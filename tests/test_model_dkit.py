from dataclasses import dataclass

from core_py import context, dkit, model


@dataclass
class Audited(model.CreateAudit, model.UpdateAudit, model.DeleteAudit, model.TenantAudit):
    pass


def test_audit_helpers_fill_context_values():
    ctx = context.empty_context()
    ctx = context.ctx_set_operator(ctx, "user-1")
    ctx = context.ctx_set_tenant_id(ctx, "tenant-1")
    ctx = context.ctx_set_app_id(ctx, "app-1")

    item = Audited()
    model.ctx_create_audit(ctx, item)
    deleted, _ = model.ctx_delete_audit(ctx, item)

    assert item.created_by == "user-1"
    assert item.updated_by == "user-1"
    assert item.deleted_by == "user-1"
    assert item.tenant_id == "tenant-1"
    assert item.app_id == "app-1"
    assert deleted is True


def test_dkit_id_and_mutex_helpers():
    atomic = dkit.InMemoryAtomic(default_ttl=1)
    kit = dkit.new_default_kit(atomic)
    called = []

    kit.mutex_do("job", lambda ctx: called.append(kit.next_id_string(ctx)))

    assert called and called[0].isdigit()
    assert atomic.new_mutex("job").exist_lock() is False
