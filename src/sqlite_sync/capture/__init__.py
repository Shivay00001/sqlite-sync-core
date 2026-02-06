"""
capture - Change capture module.
"""

from sqlite_sync.capture.change_capture import (
    enable_change_capture,
    disable_change_capture,
    is_capture_enabled,
    get_captured_tables,
)

__all__ = [
    "enable_change_capture",
    "disable_change_capture",
    "is_capture_enabled",
    "get_captured_tables",
]
