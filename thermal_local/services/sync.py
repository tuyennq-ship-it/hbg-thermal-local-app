from __future__ import annotations

import datetime
import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

from thermal_local.config import SERVER_DB_CONFIG


def _normalize_value(v: Any) -> Any:
    if isinstance(v, (dict, list)):
        return json.dumps(v)
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime.datetime):
        return v.isoformat()
    return v


def sync_server_to_sqlite(sqlite_path: Path) -> None:
    """
    One-way sync: PostgreSQL server -> local SQLite.

    Note: Clears local tables (bottom-up) before re-inserting.
    """
    # Lazy import to avoid hard dependency at import-time (helps dev/test environments).
    import psycopg2

    pg_conn = psycopg2.connect(**SERVER_DB_CONFIG)
    pg_cur = pg_conn.cursor()

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_cur = sqlite_conn.cursor()
    sqlite_cur.execute("PRAGMA foreign_keys = ON")

    # =========================
    # CLEAR DATA (BOTTOM-UP)
    # =========================
    sqlite_cur.execute("DELETE FROM cole_cole")
    sqlite_cur.execute("DELETE FROM standard_plot")
    sqlite_cur.execute("DELETE FROM nanothickness")
    sqlite_cur.execute("DELETE FROM measurements")
    sqlite_cur.execute("DELETE FROM devices")
    sqlite_cur.execute("DELETE FROM users")

    # =========================
    # USERS
    # =========================
    pg_cur.execute("SELECT id, username, role, active, hashed_password, created_at FROM users")
    sqlite_cur.executemany(
        "INSERT INTO users (id, username, role, active, hashed_password, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        [tuple(_normalize_value(v) for v in r) for r in pg_cur.fetchall()],
    )

    # =========================
    # DEVICES
    # =========================
    pg_cur.execute("""
        SELECT id, name, structure_json, experiment_by, created_by, created_at
        FROM devices
    """)
    sqlite_cur.executemany(
        "INSERT INTO devices (id, name, structure_json, experiment_by, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        [tuple(_normalize_value(v) for v in r) for r in pg_cur.fetchall()],
    )

    # =========================
    # MEASUREMENTS
    # =========================
    pg_cur.execute("""
        SELECT
            m.id,
            m.device_id,
            m.num_order,
            m.name,
            m.created_by,
            m.created_at
        FROM measurements m
        JOIN devices d ON m.device_id = d.id
    """)
    rows = [tuple(_normalize_value(v) for v in r) for r in pg_cur.fetchall()]
    sqlite_cur.executemany("""
        INSERT INTO measurements
        (id, device_id, num_order, name, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, rows)

    # =========================
    # COLE-COLE
    # =========================
    pg_cur.execute("""
        SELECT id, measurement_id, frequency, resistance, reactance, capacitance
        FROM cole_cole
    """)
    sqlite_cur.executemany(
        "INSERT INTO cole_cole (id, measurement_id, frequency, resistance, reactance, capacitance) VALUES (?, ?, ?, ?, ?, ?)",
        [tuple(_normalize_value(v) for v in r) for r in pg_cur.fetchall()],
    )

    # =========================
    # STANDARD PLOT
    # =========================
    pg_cur.execute("""
        SELECT id, measurement_id, time, voltage
        FROM standard_plot
    """)
    sqlite_cur.executemany(
        "INSERT INTO standard_plot (id, measurement_id, time, voltage) VALUES (?, ?, ?, ?)",
        [tuple(_normalize_value(v) for v in r) for r in pg_cur.fetchall()],
    )

    sqlite_conn.commit()

    sqlite_cur.close()
    sqlite_conn.close()
    pg_cur.close()
    pg_conn.close()


def read_cole_cole_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    required = {"frequency", "resistance", "reactance", "capacitance"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"Cole-Cole CSV missing columns: {required - set(df.columns)}")
    return df[["frequency", "resistance", "reactance", "capacitance"]]


def read_standard_plot_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    required = {"time", "voltage"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"Standard plot CSV missing columns: {required - set(df.columns)}")
    return df[["time", "voltage"]]

