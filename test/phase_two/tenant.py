from weaviate.classes.tenants import Tenant, TenantActivityStatus
from error_handlers import handle_collection_errors, handle_tenant_errors

STATE_MAP = {
    "active": TenantActivityStatus.ACTIVE,
    "inactive": TenantActivityStatus.INACTIVE,
    "offloaded": TenantActivityStatus.OFFLOADED,
}


@handle_collection_errors
def add_tenant(client, collection_name, tenant_name):
    collection = client.collections.get(collection_name)
    collection.tenants.create(tenant_name)


@handle_collection_errors
def get_tenants(client, collection_name, getNames=False):
    collection = client.collections.get(collection_name)
    return (
        collection.tenants.get() if not getNames else [*collection.tenants.get().keys()]
    )


@handle_tenant_errors
def get_tenant(client, collection_name, tenant_name):
    collection = client.collections.get(collection_name)
    return collection.tenants.get_by_name(tenant_name)


@handle_tenant_errors
def delete_tenant(client, collection_name, tenant_name):
    collection = client.collections.get(collection_name)
    return collection.tenants.remove(tenant_name)


@handle_collection_errors
def get_tenants_by_state(client, collection_name, state):
    if state.lower() not in STATE_MAP:
        raise ValueError("Invalid tenant state")

    tenants = get_tenants(client, collection_name, getNames=True)
    return [
        tenant
        for tenant in tenants
        if get_tenant_state(client, collection_name, tenant) == STATE_MAP[state.lower()]
    ]


@handle_collection_errors
def get_tenants_with_states(client, collection_name):
    tenants = get_tenants(client, collection_name, getNames=True)
    return [
        {
            "name": tenant,
            "state": get_tenant_state(client, collection_name, tenant),
        }
        for tenant in tenants
    ]


@handle_tenant_errors
def get_tenant_state(client, collection_name, tenant_name):
    if tenant_name not in get_tenants(client, collection_name, True):
        raise ValueError(
            f"Tenant {tenant_name} not found in collection {collection_name}"
        )

    collection = client.collections.get(collection_name)
    tenant = collection.tenants.get_by_name(tenant_name)
    return next(
        (
            state
            for state, status in STATE_MAP.items()
            if tenant.activity_status == status
        ),
    )


@handle_tenant_errors
def set_tenant_state(client, collection_name, tenant_name, state):
    if state.lower() not in STATE_MAP:
        raise ValueError("Invalid tenant state")

    collection = client.collections.get(collection_name)
    collection.tenants.update(
        tenants=[
            Tenant(
                name=tenant_name,
                activity_status=STATE_MAP[state.lower()],
            )
        ]
    )


def offload_inactive_tenants(client, collection_name):
    inactive_tenants = get_tenants_by_state(client, collection_name, "inactive")
    for tenant in inactive_tenants:
        set_tenant_state(client, collection_name, tenant.name, "offloaded")
