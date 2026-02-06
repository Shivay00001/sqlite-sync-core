"""
bundle - Bundle generation and validation.
"""

from sqlite_sync.bundle.format import (
    BundleMetadata,
    metadata_from_row,
    metadata_to_row,
    get_bundle_schema_sql,
    BUNDLE_EXTENSION,
)
from sqlite_sync.bundle.generate import generate_bundle
from sqlite_sync.bundle.validate import validate_bundle

__all__ = [
    # format
    "BundleMetadata",
    "metadata_from_row",
    "metadata_to_row",
    "get_bundle_schema_sql",
    "BUNDLE_EXTENSION",
    # generate
    "generate_bundle",
    # validate
    "validate_bundle",
]
