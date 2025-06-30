from functools import wraps
import weaviate


# Handle collection-related errors with descriptive messages
def handle_collection_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except weaviate.exceptions.UnexpectedStatusCodeError as e:
            if "not found" in str(e).lower():
                collection_name = args[1] if len(args) > 1 else "unknown"
                error_msg = f"[ERROR] Collection Not Found - Collection '{collection_name}' does not exist"
                print(error_msg)
                raise ValueError(error_msg)
            error_msg = f"[ERROR] Collection Operation Failed - {str(e)}"
            print(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"[ERROR] Unexpected Collection Error - Function '{func.__name__}': {str(e)}"
            print(error_msg)
            raise ValueError(error_msg)

    return wrapper


# Handle tenant-specific errors with descriptive messages
def handle_tenant_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except weaviate.exceptions.UnexpectedStatusCodeError as e:
            if "tenant" in str(e).lower() and "not found" in str(e).lower():
                tenant_name = args[2] if len(args) > 2 else "unknown"
                error_msg = (
                    f"[ERROR] Tenant Not Found - Tenant '{tenant_name}' does not exist"
                )
                print(error_msg)
                raise ValueError(error_msg)
            error_msg = f"[ERROR] Tenant Operation Failed - {str(e)}"
            print(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"[ERROR] Unexpected Tenant Error - Function '{func.__name__}': {str(e)}"
            print(error_msg)
            raise ValueError(error_msg)

    return wrapper


# Handle reference operations between collections with descriptive messages
def handle_reference_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except weaviate.exceptions.UnexpectedStatusCodeError as e:
            if "not found" in str(e).lower():
                from_collection = args[1] if len(args) > 1 else "unknown"
                target_collection = args[3] if len(args) > 3 else "unknown"
                error_msg = f"[ERROR] Reference Operation Failed - Collections not found: '{from_collection}' or '{target_collection}'"
                print(error_msg)
                raise ValueError(error_msg)
            error_msg = f"[ERROR] Reference Operation Failed - {str(e)}"
            print(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"[ERROR] Unexpected Reference Error - Function '{func.__name__}': {str(e)}"
            print(error_msg)
            raise ValueError(error_msg)

    return wrapper


# Handle object CRUD operations with descriptive error messages
def handle_object_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except weaviate.exceptions.UnexpectedStatusCodeError as e:
            if "not found" in str(e).lower():
                collection_name = args[1] if len(args) > 1 else "unknown"
                tenant_name = args[2] if len(args) > 2 else "unknown"
                error_msg = f"[ERROR] Object Operation Failed - Collection: '{collection_name}', Tenant: '{tenant_name}' not found"
                print(error_msg)
                raise ValueError(error_msg)
            error_msg = f"[ERROR] Object Operation Failed - {str(e)}"
            print(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"[ERROR] Unexpected Object Error - Function '{func.__name__}': {str(e)}"
            print(error_msg)
            raise ValueError(error_msg)

    return wrapper


# Handle search and query operations with descriptive error messages
def handle_query_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except weaviate.exceptions.UnexpectedStatusCodeError as e:
            if "not found" in str(e).lower():
                collection_name = args[1] if len(args) > 1 else "unknown"
                tenant_name = args[2] if len(args) > 2 else "unknown"
                error_msg = f"[ERROR] Query Operation Failed - Collection: '{collection_name}', Tenant: '{tenant_name}' not found"
                print(error_msg)
                raise ValueError(error_msg)
            error_msg = f"[ERROR] Query Operation Failed - {str(e)}"
            print(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = (
                f"[ERROR] Unexpected Query Error - Function '{func.__name__}': {str(e)}"
            )
            print(error_msg)
            raise ValueError(error_msg)

    return wrapper
