"""
node.py - SyncNode re-export for backward compatibility.

This module provides the SyncNode class at the expected import path.
"""

from sqlite_sync.ext.node import SyncNode

__all__ = ["SyncNode"]
