from __future__ import annotations

import sqlite3
from pathlib import Path


def _existing_columns(cur: sqlite3.Cursor, table: str) -> set[str]:
    cur.execute(f"PRAGMA table_info({table})")
    return {r[1] for r in cur.fetchall()}  # r[1] = column name


def _add_column_if_missing(cur: sqlite3.Cursor, table: str, col: str, col_def: str) -> None:
    existing = _existing_columns(cur, table)
    if col in existing:
        return
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")


def migrate_sqlite(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    # Keep schema aligned with sync from server.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        role TEXT,
        active INTEGER,
        hashed_password TEXT,
        created_at TEXT,
        is_delete INTEGER DEFAULT 0
    );
    """)

    # If DB already existed with older schema, upgrade it.
    # (SQLite doesn't apply new columns when using CREATE TABLE IF NOT EXISTS.)
    _add_column_if_missing(cur, "users", "role", "TEXT")
    _add_column_if_missing(cur, "users", "active", "INTEGER")
    _add_column_if_missing(cur, "users", "hashed_password", "TEXT")
    _add_column_if_missing(cur, "users", "created_at", "TEXT")
    _add_column_if_missing(cur, "users", "is_delete", "INTEGER DEFAULT 0")

    # ---------------- Devices ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS devices (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        structure_json TEXT,
        experiment_by TEXT,
        created_by TEXT,
        created_at TEXT,
        is_delete INTEGER DEFAULT 0
    );
    """)
    _add_column_if_missing(cur, "devices", "structure_json", "TEXT")
    _add_column_if_missing(cur, "devices", "experiment_by", "TEXT")
    _add_column_if_missing(cur, "devices", "created_by", "TEXT")
    _add_column_if_missing(cur, "devices", "created_at", "TEXT")
    _add_column_if_missing(cur, "devices", "is_delete", "INTEGER DEFAULT 0")

    # ---------------- Measurements ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS measurements (
        id TEXT PRIMARY KEY,
        name TEXT NULL,
        device_id TEXT NOT NULL,
        num_order INTEGER NULL,
        created_by TEXT NOT NULL,
        created_at TEXT,
        is_delete INTEGER DEFAULT 0,

        FOREIGN KEY (device_id)
            REFERENCES devices(id)
            ON DELETE CASCADE,

        UNIQUE (device_id, num_order)
    );
    """)
    _add_column_if_missing(cur, "measurements", "name", "TEXT")
    _add_column_if_missing(cur, "measurements", "num_order", "INTEGER")
    _add_column_if_missing(cur, "measurements", "created_at", "TEXT")
    _add_column_if_missing(cur, "measurements", "is_delete", "INTEGER DEFAULT 0")

    # ---------------- Nanothickness ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS nanothickness (
        id TEXT PRIMARY KEY,
        measurement_id TEXT NOT NULL,
        pos1 REAL,
        pos2 REAL,
        pos3 REAL,
        pos4 REAL,
        pos5 REAL,
        is_delete INTEGER DEFAULT 0,

        FOREIGN KEY (measurement_id)
            REFERENCES measurements(id)
            ON DELETE CASCADE
    );
    """)
    _add_column_if_missing(cur, "nanothickness", "is_delete", "INTEGER DEFAULT 0")

    # ---------------- ColeCole ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cole_cole (
        id TEXT PRIMARY KEY,
        measurement_id TEXT NOT NULL,
        frequency REAL,
        resistance REAL,
        reactance REAL,
        capacitance REAL,
        is_delete INTEGER DEFAULT 0,

        FOREIGN KEY (measurement_id)
            REFERENCES measurements(id)
            ON DELETE CASCADE
    );
    """)
    _add_column_if_missing(cur, "cole_cole", "is_delete", "INTEGER DEFAULT 0")

    # ---------------- StandardPlot ----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS standard_plot (
        id TEXT PRIMARY KEY,
        measurement_id TEXT NOT NULL,
        time REAL,
        voltage REAL,
        is_delete INTEGER DEFAULT 0,

        FOREIGN KEY (measurement_id)
            REFERENCES measurements(id)
            ON DELETE CASCADE
    );
    """)
    _add_column_if_missing(cur, "standard_plot", "is_delete", "INTEGER DEFAULT 0")

    conn.commit()
    conn.close()

