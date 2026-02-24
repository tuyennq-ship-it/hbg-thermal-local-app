"""
Backward-compatible wrapper module.

New code lives in `thermal_local.db.migrations`.
"""

from thermal_local.db.migrations import migrate_sqlite  # noqa: F401

