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
                raise ValueError(
                    f"Collection not found: {args[1] if len(args) > 1 else 'unknown'}"
                )
            raise ValueError(f"Collection operation failed: {str(e)}")
        except Exception as e:
            raise ValueError(f"Unexpected error in {func.__name__}: {str(e)}")

    return wrapper


# Handle tenant-specific errors with descriptive messages
def handle_tenant_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except weaviate.exceptions.UnexpectedStatusCodeError as e:
            if "tenant" in str(e).lower() and "not found" in str(e).lower():
                raise ValueError(
                    f"Tenant not found: {args[2] if len(args) > 2 else 'unknown'}"
                )
            raise ValueError(f"Tenant operation failed: {str(e)}")
        except Exception as e:
            raise ValueError(f"Unexpected error in {func.__name__}: {str(e)}")

    return wrapper


# Handle reference operations between collections with descriptive messages
def handle_reference_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except weaviate.exceptions.UnexpectedStatusCodeError as e:
            if "not found" in str(e).lower():
                raise ValueError(
                    f"Collection not found for reference operation: {args[1] if len(args) > 1 else 'unknown'} or {args[3] if len(args) > 3 else 'unknown'}"
                )
            raise ValueError(f"Reference operation failed: {str(e)}")
        except Exception as e:
            raise ValueError(f"Unexpected error in {func.__name__}: {str(e)}")

    return wrapper
 