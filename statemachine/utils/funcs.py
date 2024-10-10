import sys
from functools import wraps
from typing import Callable


def _is_running_unit_test() -> bool:
    return "pytest" in sys.modules


def protected(func: Callable) -> Callable:
    """This decorator will ensure data connectors are not called in unit-test mode"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        if _is_running_unit_test():
            try:
                module_name = args[0].__class__.__name__
            except Exception:
                module_name = ""
            raise IOError(
                f"Trying to call function '{module_name}"
                f".{func.__name__}' while running in unit-test mode: "
                f"forbidden."
            )
        return func(*args, **kwargs)

    return wrapper
