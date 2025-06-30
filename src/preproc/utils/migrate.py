import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dotenv import load_dotenv

try:
    # Try relative imports (when run from outside utils)
    from .client import get_client
    from .blob_utils import BlobStorageManager
    from .collection import get_collections, create_collection, get_collection_name
    from .tenant import get_tenants, add_tenant, set_tenant_state, get_tenant_state
    from .objects import get_objects, batch_upload_objects, generate_uuid
    from .schema import TEXT_SCHEMA, IMAGE_SCHEMA
    from .error_handlers import handle_collection_errors, handle_object_errors
except ImportError:
    # Fall back to absolute imports (when run from inside utils)
    from client import get_client
    from blob_utils import BlobStorageManager
    from collection import get_collections, create_collection, get_collection_name
    from tenant import get_tenants, add_tenant, set_tenant_state, get_tenant_state
    from objects import get_objects, batch_upload_objects, generate_uuid
    from schema import TEXT_SCHEMA, IMAGE_SCHEMA
    from error_handlers import handle_collection_errors, handle_object_errors

load_dotenv()


# Create backup of collection data and metadata to blob storage
@handle_collection_errors
def backup_collection(
    client,
    collection_name: str,
    tenant_name: Optional[str] = None,
    include_vectors: bool = True,
) -> str:
    timestamp = datetime.now().isoformat()
    backup_data = {
        "timestamp": timestamp,
        "collection_name": collection_name,
        "tenant_name": tenant_name,
        "include_vectors": include_vectors,
        "metadata": {},
        "objects": [],
    }

    # Get collection metadata
    collection = client.collections.get(collection_name)
    backup_data["metadata"]["collection_config"] = collection.config.get()

    # Get tenant information if multi-tenant
    if tenant_name:
        tenants_info = {
            tenant_name: get_tenant_state(client, collection_name, tenant_name)
        }
        backup_data["metadata"]["tenants"] = tenants_info

        # Get objects from specific tenant
        objects = get_objects(
            client,
            collection_name,
            tenant_name,
            get_properties=True,
            get_vectors=include_vectors,
        )
    else:
        # Get all tenants if multi-tenant collection
        try:
            all_tenants = get_tenants(client, collection_name, getNames=True)
            tenants_info = {
                tenant: get_tenant_state(client, collection_name, tenant)
                for tenant in all_tenants
            }
            backup_data["metadata"]["tenants"] = tenants_info
        except Exception:
            backup_data["metadata"]["tenants"] = {}

        # Get all objects (requires iteration over tenants for multi-tenant collections)
        objects = []
        if backup_data["metadata"]["tenants"]:
            for tenant in backup_data["metadata"]["tenants"].keys():
                tenant_objects = get_objects(
                    client,
                    collection_name,
                    tenant,
                    get_properties=True,
                    get_vectors=include_vectors,
                )
                # Add tenant field to each object before extending the list
                for obj in tenant_objects:
                    obj["tenant"] = tenant
                    objects.append(obj)
        else:
            # Single-tenant collection
            objects = get_objects(
                client,
                collection_name,
                None,
                get_properties=True,
                get_vectors=include_vectors,
            )

    backup_data["objects"] = objects
    backup_json = json.dumps(backup_data, indent=2, default=str)

    # Upload to blob storage
    blob_manager = BlobStorageManager(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))

    # Use tenant name in path, or 'global' for non-tenant specific backups
    tenant_path = tenant_name if tenant_name else "global"
    blob_name = f"backups/{tenant_path}/{collection_name}_{timestamp}.json"

    return blob_manager.upload_processed_file(
        blob_name,
        backup_json.encode("utf-8"),
        metadata={"type": "collection_backup", "collection": collection_name},
        content_type="application/json",
    )


# Restore collection data and metadata from blob storage backup
@handle_collection_errors
def restore_collection(
    client,
    backup_blob_url: str,
    target_collection_name: Optional[str] = None,
    overwrite: bool = False,
) -> Dict[str, Any]:
    blob_manager = BlobStorageManager(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))

    # Download backup data
    container, blob_path = blob_manager.extract_blob_path_from_url(backup_blob_url)
    backup_data_bytes = blob_manager.download_blob(container, blob_path)
    backup_data = json.loads(backup_data_bytes.decode("utf-8"))

    original_collection = backup_data["collection_name"]
    collection_name = target_collection_name or original_collection

    # Determine schema based on collection type
    if "text" in collection_name.lower():
        schema = TEXT_SCHEMA
    elif "image" in collection_name.lower():
        schema = IMAGE_SCHEMA
    else:
        schema = TEXT_SCHEMA  # Default fallback

    # Create collection if it doesn't exist or overwrite is True
    collections = get_collections(client)
    existing_collections = [
        col.name if hasattr(col, "name") else str(col) for col in collections
    ]

    if overwrite and collection_name in existing_collections:
        client.collections.delete(collection_name)

    if collection_name not in existing_collections or overwrite:
        has_tenants = bool(backup_data["metadata"].get("tenants"))
        create_collection(
            client,
            collection_name,
            schema,
            existing_collections,
            multi_tenancy=has_tenants,
        )

    # Restore tenants if multi-tenant
    if backup_data["metadata"].get("tenants"):
        for tenant_name, tenant_state in backup_data["metadata"]["tenants"].items():
            try:
                add_tenant(client, collection_name, tenant_name)
                set_tenant_state(client, collection_name, tenant_name, tenant_state)
            except Exception as e:
                print(f"Warning: Could not restore tenant {tenant_name}: {e}")

    # Restore objects
    objects_to_restore = []
    tenant_object_count = {}

    for obj_data in backup_data["objects"]:
        restore_obj = {"uuid": obj_data["uuid"], "properties": obj_data["properties"]}

        if backup_data["include_vectors"] and obj_data.get("vector"):
            restore_obj["vector"] = obj_data["vector"]

        # Track tenant for multi-tenant restoration
        tenant = obj_data.get("tenant")
        if tenant:
            tenant_object_count[tenant] = tenant_object_count.get(tenant, 0) + 1

        objects_to_restore.append(restore_obj)

        # Debug: Print backup data info
    print(f"Backup has {len(backup_data['objects'])} objects")
    print(f"Tenant metadata: {backup_data['metadata'].get('tenants', {})}")
    print(f"Has tenants: {bool(backup_data['metadata'].get('tenants'))}")

    # Debug: Check if objects have tenant field
    objects_with_tenant = [obj for obj in backup_data["objects"] if obj.get("tenant")]
    print(f"Objects with tenant field: {len(objects_with_tenant)}")
    if len(objects_with_tenant) > 0:
        print(f"Sample object tenant: {objects_with_tenant[0].get('tenant')}")

    # Batch upload objects (handles both single and multi-tenant)
    if backup_data["metadata"].get("tenants"):
        # Multi-tenant restoration
        results = {}

        # Check if objects have tenant field
        objects_with_tenant = [
            obj for obj in backup_data["objects"] if obj.get("tenant")
        ]

        if objects_with_tenant:
            # Objects have tenant field - use tenant-specific restoration
            for tenant_name in backup_data["metadata"]["tenants"].keys():
                tenant_objects = [
                    obj
                    for obj in backup_data["objects"]
                    if obj.get("tenant") == tenant_name
                ]
                tenant_restore_objects = []
                for obj_data in tenant_objects:
                    restore_obj = {
                        "uuid": obj_data["uuid"],
                        "properties": obj_data["properties"],
                    }
                    if backup_data["include_vectors"] and obj_data.get("vector"):
                        restore_obj["vector"] = obj_data["vector"]
                    tenant_restore_objects.append(restore_obj)

                print(
                    f"Restoring {len(tenant_restore_objects)} objects to tenant {tenant_name}"
                )
                if tenant_restore_objects:
                    uploaded, failed = batch_upload_objects(
                        client, collection_name, tenant_restore_objects, tenant_name
                    )
                    results[tenant_name] = {"uploaded": uploaded, "failed": len(failed)}
                    print(f"Uploaded: {uploaded}, Failed: {len(failed)}")
        else:
            # Objects don't have tenant field - restore all objects to first tenant
            print(
                "Objects don't have tenant field, restoring all to first available tenant"
            )
            tenant_names = list(backup_data["metadata"]["tenants"].keys())
            if tenant_names:
                first_tenant = tenant_names[0]
                tenant_restore_objects = []
                for obj_data in backup_data["objects"]:
                    restore_obj = {
                        "uuid": obj_data["uuid"],
                        "properties": obj_data["properties"],
                    }
                    if backup_data["include_vectors"] and obj_data.get("vector"):
                        restore_obj["vector"] = obj_data["vector"]
                    tenant_restore_objects.append(restore_obj)

                print(
                    f"Restoring {len(tenant_restore_objects)} objects to tenant {first_tenant}"
                )
                if tenant_restore_objects:
                    uploaded, failed = batch_upload_objects(
                        client, collection_name, tenant_restore_objects, first_tenant
                    )
                    results[first_tenant] = {
                        "uploaded": uploaded,
                        "failed": len(failed),
                    }
                    print(f"Uploaded: {uploaded}, Failed: {len(failed)}")
    else:
        # Single-tenant restoration - need to determine actual tenant name
        print(f"Single tenant restoration with {len(objects_to_restore)} objects")

        # For multi-tenant collections without tenant metadata, use first available tenant
        try:
            available_tenants = get_tenants(client, collection_name, getNames=True)
            if available_tenants:
                first_tenant = available_tenants[0]
                print(f"Using first available tenant: {first_tenant}")
                uploaded, failed = batch_upload_objects(
                    client, collection_name, objects_to_restore, first_tenant
                )
            else:
                print("No tenants found, collection might not be multi-tenant")
                uploaded, failed = batch_upload_objects(
                    client, collection_name, objects_to_restore, None
                )
        except Exception as e:
            print(f"Error handling tenants: {e}")
            uploaded, failed = batch_upload_objects(
                client, collection_name, objects_to_restore, None
            )

        results = {"uploaded": uploaded, "failed": len(failed)}
        print(f"Restoration results: {results}")

    return {
        "collection_name": collection_name,
        "original_collection": original_collection,
        "restoration_results": results,
        "total_objects": len(backup_data["objects"]),
        "timestamp": datetime.now().isoformat(),
    }


# Migrate data from one Weaviate cluster to another
@handle_collection_errors
def migrate_between_clusters(
    source_cluster_url: str,
    source_api_key: str,
    dest_cluster_url: str,
    dest_api_key: str,
    collection_names: List[str],
    include_vectors: bool = True,
) -> Dict[str, Any]:
    import weaviate
    import weaviate.classes as wvc

    # Connect to source cluster
    source_client = weaviate.connect_to_weaviate_cloud(
        cluster_url=source_cluster_url,
        auth_credentials=wvc.init.Auth.api_key(source_api_key),
        skip_init_checks=True,
    )

    # Connect to destination cluster
    dest_client = weaviate.connect_to_weaviate_cloud(
        cluster_url=dest_cluster_url,
        auth_credentials=wvc.init.Auth.api_key(dest_api_key),
        skip_init_checks=True,
    )

    migration_results = {}

    try:
        for collection_name in collection_names:
            # Get source collection configuration
            source_collection = source_client.collections.get(collection_name)
            source_config = source_collection.config.get()

            # Determine appropriate schema
            if "text" in collection_name.lower():
                schema = TEXT_SCHEMA
            elif "image" in collection_name.lower():
                schema = IMAGE_SCHEMA
            else:
                schema = TEXT_SCHEMA

            # Create collection on destination
            dest_collections = get_collections(dest_client)
            dest_existing = [
                col.name if hasattr(col, "name") else str(col)
                for col in dest_collections
            ]
            has_tenants = source_config.multi_tenancy_config.enabled

            create_collection(
                dest_client,
                collection_name,
                schema,
                dest_existing,
                multi_tenancy=has_tenants,
            )

            # Migrate tenants if multi-tenant
            tenant_results = {}
            if has_tenants:
                source_tenants = get_tenants(
                    source_client, collection_name, getNames=True
                )
                for tenant_name in source_tenants:
                    # Create tenant on destination
                    add_tenant(dest_client, collection_name, tenant_name)

                    # Get tenant state and replicate
                    tenant_state = get_tenant_state(
                        source_client, collection_name, tenant_name
                    )
                    set_tenant_state(
                        dest_client, collection_name, tenant_name, tenant_state
                    )

                    # Migrate tenant data
                    tenant_objects = get_objects(
                        source_client,
                        collection_name,
                        tenant_name,
                        get_properties=True,
                        get_vectors=include_vectors,
                    )

                    if tenant_objects:
                        uploaded, failed = batch_upload_objects(
                            dest_client, collection_name, tenant_objects, tenant_name
                        )
                        tenant_results[tenant_name] = {
                            "uploaded": uploaded,
                            "failed": len(failed),
                        }
            else:
                # Single-tenant collection migration
                collection_objects = get_objects(
                    source_client,
                    collection_name,
                    None,
                    get_properties=True,
                    get_vectors=include_vectors,
                )

                if collection_objects:
                    uploaded, failed = batch_upload_objects(
                        dest_client, collection_name, collection_objects, None
                    )
                    tenant_results["single_tenant"] = {
                        "uploaded": uploaded,
                        "failed": len(failed),
                    }

            migration_results[collection_name] = {
                "tenant_results": tenant_results,
                "total_tenants": len(tenant_results) if tenant_results else 0,
            }

    finally:
        source_client.close()
        dest_client.close()

    return {
        "migration_results": migration_results,
        "collections_migrated": len(collection_names),
        "timestamp": datetime.now().isoformat(),
    }


# Migrate data from one collection to another within the same cluster
@handle_object_errors
def migrate_between_collections(
    client,
    source_collection: str,
    dest_collection: str,
    property_mapping: Optional[Dict[str, str]] = None,
    tenant_mapping: Optional[Dict[str, str]] = None,
    include_vectors: bool = True,
) -> Dict[str, Any]:
    collections = get_collections(client)
    existing_collections = [
        col.name if hasattr(col, "name") else str(col) for col in collections
    ]

    # Ensure destination collection exists
    if dest_collection not in existing_collections:
        # Determine schema for destination collection
        if "text" in dest_collection.lower():
            schema = TEXT_SCHEMA
        elif "image" in dest_collection.lower():
            schema = IMAGE_SCHEMA
        else:
            schema = TEXT_SCHEMA

        # Check if source is multi-tenant to replicate structure
        source_tenants = []
        try:
            source_tenants = get_tenants(client, source_collection, getNames=True)
            has_tenants = bool(source_tenants)
        except Exception:
            has_tenants = False

        create_collection(
            client,
            dest_collection,
            schema,
            existing_collections,
            multi_tenancy=has_tenants,
        )

    # Get source collection tenants
    source_tenants = []
    try:
        source_tenants = get_tenants(client, source_collection, getNames=True)
    except Exception:
        source_tenants = [None]  # Single-tenant collection

    migration_results = {}

    for source_tenant in source_tenants:
        # Determine destination tenant
        dest_tenant = source_tenant
        if tenant_mapping and source_tenant in tenant_mapping:
            dest_tenant = tenant_mapping[source_tenant]

        # Create destination tenant if needed
        if dest_tenant and dest_tenant != source_tenant:
            try:
                add_tenant(client, dest_collection, dest_tenant)
            except Exception as e:
                print(f"Warning: Could not create tenant {dest_tenant}: {e}")

        # Get objects from source
        source_objects = get_objects(
            client,
            source_collection,
            source_tenant,
            get_properties=True,
            get_vectors=include_vectors,
        )

        # Transform objects for destination collection
        transformed_objects = []
        for obj in source_objects:
            new_obj = {
                "uuid": generate_uuid(f"{dest_collection}_{obj['uuid']}"),
                "properties": obj["properties"].copy(),
            }

            # Apply property mapping
            if property_mapping:
                new_properties = {}
                for source_prop, dest_prop in property_mapping.items():
                    if source_prop in new_obj["properties"]:
                        new_properties[dest_prop] = new_obj["properties"][source_prop]

                # Keep unmapped properties
                for prop, value in new_obj["properties"].items():
                    if prop not in property_mapping:
                        new_properties[prop] = value

                new_obj["properties"] = new_properties

            if include_vectors and obj.get("vector"):
                new_obj["vector"] = obj["vector"]

            transformed_objects.append(new_obj)

        # Upload to destination collection
        if transformed_objects:
            uploaded, failed = batch_upload_objects(
                client, dest_collection, transformed_objects, dest_tenant
            )

            tenant_key = dest_tenant or "single_tenant"
            migration_results[tenant_key] = {
                "uploaded": uploaded,
                "failed": len(failed),
                "source_tenant": source_tenant,
            }

    return {
        "source_collection": source_collection,
        "dest_collection": dest_collection,
        "migration_results": migration_results,
        "property_mapping": property_mapping,
        "tenant_mapping": tenant_mapping,
        "timestamp": datetime.now().isoformat(),
    }
