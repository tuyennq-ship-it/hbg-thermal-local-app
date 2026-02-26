from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from thermal_local.config import SERVER_DB_CONFIG


@dataclass(frozen=True)
class LocalContext:
    db_path: Path
    data_root: Path


def open_sqlite(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def get_devices_and_measurements(db_path: Path) -> dict[str, list[str]]:
    conn = open_sqlite(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT d.name, m.name
        FROM devices d
        LEFT JOIN measurements m
            ON d.id = m.device_id
            AND m.is_delete = 0
        WHERE d.is_delete = 0
        ORDER BY d.name, m.created_at
    """)
    data: dict[str, list[str]] = {}
    for device_name, measurement_name in cur.fetchall():
        data.setdefault(device_name, [])
        if measurement_name:
            data[device_name].append(measurement_name)
    conn.close()
    return data


def get_device_id(db_path: Path, device_name: str) -> str:
    conn = open_sqlite(db_path)
    cur = conn.cursor()
    cur.execute("SELECT id FROM devices WHERE name = ?", (device_name,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise RuntimeError("Device not found")
    return row[0]


def get_measurement_id(db_path: Path, device_name: str, measurement_name: str) -> str:
    conn = open_sqlite(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT m.id
        FROM measurements m
        JOIN devices d ON m.device_id = d.id
        WHERE m.name = ? AND d.name = ? AND m.is_delete = 0
        """,
        (measurement_name, device_name),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise RuntimeError("Measurement not found")
    return row[0]


def get_device_structure(db_path: Path, device_name: str):
    conn = open_sqlite(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT structure_json
        FROM devices
        WHERE name = ?
        """,
        (device_name,),
    )
    row = cur.fetchone()
    conn.close()
    if not row or not row[0]:
        return None
    try:
        return json.loads(row[0])
    except json.JSONDecodeError:
        return {"_error": "Invalid JSON in structure_json"}


def create_measurement(
    ctx: LocalContext,
    *,
    device_name: str,
    device_id: str,
    measurement_name: str,
    created_by: str,
) -> None:
    measurement_name = measurement_name.strip()
    conn = open_sqlite(ctx.db_path)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT 1 FROM measurements
        WHERE device_id = ? AND name = ? AND is_delete = 0
        """,
        (device_id, measurement_name),
    )
    if cur.fetchone():
        conn.close()
        raise ValueError("Measurement name already exists for this device")

    m_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO measurements (id, device_id, name, created_by, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (m_id, device_id, measurement_name, created_by, datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()

    # create folder on filesystem
    path = ctx.data_root / "devices" / device_name / measurement_name
    path.mkdir(parents=True, exist_ok=True)


def sync_db_to_filesystem(ctx: LocalContext) -> None:
    base = ctx.data_root / "devices"
    base.mkdir(exist_ok=True)

    conn = open_sqlite(ctx.db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT d.name, m.name
        FROM devices d
        LEFT JOIN measurements m
            ON d.id = m.device_id
            AND m.is_delete = 0
        WHERE d.is_delete = 0
    """)
    for device_name, measurement_name in cur.fetchall():
        device_dir = base / device_name
        device_dir.mkdir(exist_ok=True)
        if measurement_name:
            meas_dir = device_dir / measurement_name
            meas_dir.mkdir(exist_ok=True)
    conn.close()


def read_cole_cole_from_db(db_path: Path, measurement_id: str) -> pd.DataFrame:
    conn = open_sqlite(db_path)
    df = pd.read_sql_query(
        """
        SELECT frequency, resistance, reactance, capacitance
        FROM cole_cole
        WHERE measurement_id = ? AND is_delete = 0
        """,
        conn,
        params=(measurement_id,),
    )
    conn.close()
    return df


def read_standard_plot_from_db(db_path: Path, measurement_id: str) -> pd.DataFrame:
    conn = open_sqlite(db_path)
    df = pd.read_sql_query(
        """
        SELECT time, voltage
        FROM standard_plot
        WHERE measurement_id = ? AND is_delete = 0
        """,
        conn,
        params=(measurement_id,),
    )
    conn.close()
    return df


def read_nanothickness_from_db(db_path: Path, measurement_id: str) -> pd.DataFrame:
    conn = open_sqlite(db_path)
    df = pd.read_sql_query(
        """
        SELECT pos1, pos2, pos3, pos4, pos5
        FROM nanothickness
        WHERE measurement_id = ? AND is_delete = 0
        """,
        conn,
        params=(measurement_id,),
    )
    conn.close()
    return df


def has_cole_cole(db_path: Path, measurement_id: str) -> bool:
    conn = open_sqlite(db_path)
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM cole_cole WHERE measurement_id = ? AND is_delete = 0 LIMIT 1",
        (measurement_id,),
    )
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def has_standard_plot(db_path: Path, measurement_id: str) -> bool:
    conn = open_sqlite(db_path)
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM standard_plot WHERE measurement_id = ? AND is_delete = 0 LIMIT 1",
        (measurement_id,),
    )
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def has_nanothickness(db_path: Path, measurement_id: str) -> bool:
    conn = open_sqlite(db_path)
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM nanothickness WHERE measurement_id = ? AND is_delete = 0 LIMIT 1",
        (measurement_id,),
    )
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def is_measurement_owner(db_path: Path, measurement_id: str, username: str) -> bool:
    conn = open_sqlite(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT created_by
        FROM measurements
        WHERE id = ? AND is_delete = 0
        """,
        (measurement_id,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return False
    return row[0] == username

def insert_cole_cole(db_path: Path, measurement_id: str, df: pd.DataFrame) -> None:
    conn = open_sqlite(db_path)
    cur = conn.cursor()
    for _, r in df.iterrows():
        cur.execute(
            """
            INSERT INTO cole_cole (
                id, measurement_id, frequency, resistance, reactance, capacitance
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                measurement_id,
                float(r["frequency"]),
                float(r["resistance"]),
                float(r["reactance"]),
                float(r["capacitance"]),
            ),
        )
    conn.commit()
    conn.close()


def insert_standard_plot(db_path: Path, measurement_id: str, df: pd.DataFrame) -> None:
    conn = open_sqlite(db_path)
    cur = conn.cursor()
    for _, r in df.iterrows():
        cur.execute(
            """
            INSERT INTO standard_plot (id, measurement_id, time, voltage)
            VALUES (?, ?, ?, ?)
            """,
            (str(uuid.uuid4()), measurement_id, float(r["time"]), float(r["voltage"])),
        )
    conn.commit()
    conn.close()


def insert_nanothickness(db_path: Path, measurement_id: str, df: pd.DataFrame) -> None:
    conn = open_sqlite(db_path)
    cur = conn.cursor()
    for _, r in df.iterrows():
        cur.execute(
            """
            INSERT INTO nanothickness (id, measurement_id, pos1, pos2, pos3, pos4, pos5)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (str(uuid.uuid4()), measurement_id, float(r["pos1"]), float(r["pos2"]), float(r["pos3"]), float(r["pos4"]), float(r["pos5"])),
        )
    conn.commit()
    conn.close()


def sync_measurement_to_server(db_path: Path, measurement_id: str) -> None:
    # Lazy import to avoid hard dependency at import-time (helps dev/test environments).
    import psycopg2

    # ---------- local ----------
    s_conn = open_sqlite(db_path)
    s_cur = s_conn.cursor()
    s_cur.execute(
        """
        SELECT id, device_id, name, created_by, created_at
        FROM measurements
        WHERE id = ? AND is_delete = 0
        """,
        (measurement_id,),
    )
    row = s_cur.fetchone()
    s_conn.close()
    if not row:
        raise RuntimeError("Measurement not found")
    m_id, device_id, name, created_by, created_at = row

    # ---------- server ----------
    pg_conn = psycopg2.connect(**SERVER_DB_CONFIG)
    pg_cur = pg_conn.cursor()
    pg_cur.execute(
        """
        SELECT COALESCE(MAX(num_order), 0) + 1
        FROM measurements
        WHERE device_id = %s
        """,
        (device_id,),
    )
    num_order = pg_cur.fetchone()[0]
    pg_cur.execute(
        """
        INSERT INTO measurements (
            id, device_id, num_order, name, created_by, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
        """,
        (m_id, device_id, num_order, name, created_by, created_at),
    )
    pg_conn.commit()
    pg_conn.close()


def sync_sqlite_to_server(db_path: Path, measurement_id: str) -> None:
    # Lazy import to avoid hard dependency at import-time (helps dev/test environments).
    import psycopg2

    sync_measurement_to_server(db_path, measurement_id)

    sqlite_conn = open_sqlite(db_path)
    s_cur = sqlite_conn.cursor()

    pg_conn = psycopg2.connect(**SERVER_DB_CONFIG)
    p_cur = pg_conn.cursor()

    # ---------- Cole-Cole ----------
    s_cur.execute(
        """
        SELECT frequency, resistance, reactance, capacitance
        FROM cole_cole
        WHERE measurement_id = ? AND is_delete = 0
        """,
        (measurement_id,),
    )
    for r in s_cur.fetchall():
        p_cur.execute(
            """
            INSERT INTO cole_cole (
                id, measurement_id, frequency, resistance, reactance, capacitance
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (str(uuid.uuid4()), measurement_id, r[0], r[1], r[2], r[3]),
        )

    # ---------- Standard Plot ----------
    s_cur.execute(
        """
        SELECT time, voltage
        FROM standard_plot
        WHERE measurement_id = ? AND is_delete = 0
        """,
        (measurement_id,),
    )
    for r in s_cur.fetchall():
        p_cur.execute(
            """
            INSERT INTO standard_plot (id, measurement_id, time, voltage)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (str(uuid.uuid4()), measurement_id, r[0], r[1]),
        )
    # ---------- Nanothickness ----------
    s_cur.execute(
        """
        SELECT pos1, pos2, pos3, pos4, pos5
        FROM nanothickness
        WHERE measurement_id = ? AND is_delete = 0
        """,
        (measurement_id,),
    )
    for r in s_cur.fetchall():
        p_cur.execute(
            """
            INSERT INTO nanothickness (id, measurement_id, pos1, pos2, pos3, pos4, pos5)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (str(uuid.uuid4()), measurement_id, r[0], r[1], r[2], r[3], r[4]),
        )

    pg_conn.commit()
    sqlite_conn.close()
    pg_conn.close()


def _sync_soft_delete_to_server(db_path: Path, measurement_id: str) -> None:
    """Sync soft-delete (is_delete=1) to server DB for measurement and related tables."""
    import psycopg2

    pg_conn = psycopg2.connect(**SERVER_DB_CONFIG)
    pg_cur = pg_conn.cursor()
    try:
        pg_cur.execute(
            "UPDATE measurements SET is_delete = TRUE WHERE id = %s",
            (measurement_id,),
        )
        pg_cur.execute(
            "UPDATE cole_cole SET is_delete = TRUE WHERE measurement_id = %s",
            (measurement_id,),
        )
        pg_cur.execute(
            "UPDATE standard_plot SET is_delete = TRUE WHERE measurement_id = %s",
            (measurement_id,),
        )
        pg_cur.execute(
            "UPDATE nanothickness SET is_delete = TRUE WHERE measurement_id = %s",
            (measurement_id,),
        )
        pg_conn.commit()
    finally:
        pg_cur.close()
        pg_conn.close()


def soft_delete_measurement(
    db_path: Path,
    *,
    device_name: str,
    measurement_name: str,
    username: str,
) -> None:
    """
    Soft delete a measurement and its related data (cole_cole, standard_plot, nanothickness).
    Only allowed if created_by matches the logged-in username.
    Syncs is_delete to server DB.
    """
    conn = open_sqlite(db_path)
    cur = conn.cursor()

    # Find measurement id and creator
    cur.execute(
        """
        SELECT m.id, m.created_by
        FROM measurements m
        JOIN devices d ON m.device_id = d.id
        WHERE m.name = ? AND d.name = ? AND m.is_delete = 0
        """,
        (measurement_name, device_name),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise RuntimeError("Measurement not found")

    measurement_id, created_by = row
    if created_by != username:
        conn.close()
        raise PermissionError("You can only delete measurements you created")

    # Soft delete measurement and related records (local)
    cur.execute(
        "UPDATE measurements SET is_delete = 1 WHERE id = ?",
        (measurement_id,),
    )
    cur.execute(
        "UPDATE cole_cole SET is_delete = 1 WHERE measurement_id = ?",
        (measurement_id,),
    )
    cur.execute(
        "UPDATE standard_plot SET is_delete = 1 WHERE measurement_id = ?",
        (measurement_id,),
    )
    cur.execute(
        "UPDATE nanothickness SET is_delete = 1 WHERE measurement_id = ?",
        (measurement_id,),
    )

    conn.commit()
    conn.close()

    # Sync soft-delete to server DB (requires is_delete column on server)
    try:
        _sync_soft_delete_to_server(db_path, measurement_id)
    except Exception as e:
        # Re-raise so UI can show error (e.g. if server lacks is_delete column)
        raise RuntimeError(f"Local delete succeeded, but server sync failed: {e}") from e

