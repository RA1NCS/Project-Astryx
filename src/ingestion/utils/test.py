import weaviate.classes as wvc
from weaviate import connect_to_weaviate_cloud
from migrate import (
    backup_collection,
    restore_collection,
    migrate_between_clusters,
    migrate_between_collections,
)
from objects import get_objects
from tenant import add_tenant, get_tenants

# Cluster 1 credentials
WEAVIATE_URL_1 = "zwsdlfdnshikjxrgcjvubq.c0.us-east1.gcp.weaviate.cloud"
WEAVIATE_API_KEY_1 = "RkVESXNLRjU5TTZmY2JaTl8zYnhlU21Yclh5VlR0ZEM0NFhBT2UyMlZwNnhXcHJPUThYczV2K0gwR2U0PV92MjAw"

# Cluster 2 credentials
WEAVIATE_URL_2 = "tzuurbs0unas7tqgp1jq.c0.us-east1.gcp.weaviate.cloud"
WEAVIATE_API_KEY_2 = "aHdneWw1dUFRdm1sZ0RsZ19PL2txVGJaWjFER3NOVDZLK1g0ZmszSjZ1a2hzSEZDL2gwTkhQMk1KRE5FPV92MjAw"


def get_client_1():
    return connect_to_weaviate_cloud(
        cluster_url=WEAVIATE_URL_1,
        auth_credentials=wvc.init.Auth.api_key(WEAVIATE_API_KEY_1),
    )


def get_client_2():
    return connect_to_weaviate_cloud(
        cluster_url=WEAVIATE_URL_2,
        auth_credentials=wvc.init.Auth.api_key(WEAVIATE_API_KEY_2),
    )


def check_references(client, collection_name, tenant_name):
    try:
        objects = get_objects(client, collection_name, tenant_name, get_references=True)
        with_refs = [obj for obj in objects if obj.get("references")]
        return len(objects), len(with_refs)
    except Exception as e:
        print(f"Error checking {collection_name}: {e}")
        return 0, 0


def test_references():
    client = get_client_1()

    text_total, text_refs = check_references(client, "TextChunk", "dev")
    image_total, image_refs = check_references(client, "ImageChunk", "dev")

    print(f"TextChunk: {text_refs}/{text_total} with references")
    print(f"ImageChunk: {image_refs}/{image_total} with references")

    client.close()
    return text_refs > 0, image_refs > 0


def test_backup_restore():
    try:
        client = get_client_1()

        backup_url = backup_collection(client, "TextChunk", "dev", True, True)

        import urllib.parse

        decoded_url = urllib.parse.unquote(backup_url)
        result = restore_collection(
    client,
            decoded_url,
            target_collection_name="TextChunk_Test",
            overwrite=False,
        )

        add_tenant(client, "TextChunk_Test", "test_restore")

        restored_total, restored_refs = check_references(
            client, "TextChunk_Test", "test_restore"
        )
        print(f"Restored: {restored_refs}/{restored_total} with references")

        client.close()
        return result
    except Exception as e:
        print(f"Backup/restore error: {e}")
        return False


def test_cluster_migration():
    try:
        result = migrate_between_clusters(
            source_cluster_url=WEAVIATE_URL_1,
            source_api_key=WEAVIATE_API_KEY_1,
            dest_cluster_url=WEAVIATE_URL_2,
            dest_api_key=WEAVIATE_API_KEY_2,
            collection_names=["TextChunk"],
            include_vectors=True,
        )

        client2 = get_client_2()
        tenants = get_tenants(client2, "TextChunk", getNames=True)

        if tenants:
            migrated_total, migrated_refs = check_references(
                client2, "TextChunk", tenants[0]
)
            print(f"Migrated: {migrated_refs}/{migrated_total} with references")

        client2.close()
        return result
    except Exception as e:
        print(f"Cluster migration error: {e}")
        return False


def test_collection_migration():
    try:
        client = get_client_1()

        result = migrate_between_collections(
            client=client,
            source_collection="TextChunk",
            dest_collection="TextChunk_Migration_Test",
            property_mapping=None,
            tenant_mapping={"dev": "test_migration"},
            include_vectors=True,
        )

client.close()
        return result
    except Exception as e:
        print(f"Collection migration error: {e}")
        return False


def main():
    print("🚀 Testing Migration & References\n")

    text_refs, image_refs = test_references()
    backup_result = test_backup_restore()
    cluster_result = test_cluster_migration()
    collection_result = test_collection_migration()

    print(f"\n📊 Results:")
    print(f"TextChunk references: {'✅' if text_refs else '❌'}")
    print(f"ImageChunk references: {'✅' if image_refs else '❌'}")
    print(f"Backup/restore: {'✅' if backup_result else '❌'}")
    print(f"Cluster migration: {'✅' if cluster_result else '❌'}")
    print(f"Collection migration: {'✅' if collection_result else '❌'}")


if __name__ == "__main__":
    main()
