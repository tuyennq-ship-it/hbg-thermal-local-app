from __future__ import annotations

import os
import platform
import subprocess
from pathlib import Path

import pandas as pd
import streamlit as st

from thermal_local.db.migrations import migrate_sqlite
from thermal_local.paths import get_paths
from thermal_local.utils import Hasher
from thermal_local.services.measurements import (
    LocalContext,
    create_measurement,
    get_device_id,
    get_device_structure,
    get_devices_and_measurements,
    get_measurement_id,
    has_cole_cole,
    has_standard_plot,
    has_nanothickness,
    is_measurement_owner,
    insert_cole_cole,
    insert_standard_plot,
    insert_nanothickness,
    read_cole_cole_from_db,
    read_nanothickness_from_db,
    read_standard_plot_from_db,
    soft_delete_measurement,
    sync_db_to_filesystem,
    sync_sqlite_to_server,
)
from thermal_local.services.sync import (
    read_cole_cole_csv,
    read_standard_plot_csv,
    read_nanothickness_csv,
    sync_server_to_sqlite,
)


def _open_folder(path: Path) -> None:
    if not path.exists():
        st.warning("Folder does not exist")
        return

    system = platform.system()
    try:
        if system == "Darwin":  # macOS
            subprocess.Popen(["open", str(path)])
        elif system == "Windows":
            os.startfile(str(path))
        elif system == "Linux":
            subprocess.Popen(["xdg-open", str(path)])
    except Exception as e:
        st.error(f"Cannot open folder: {e}")


def _init_session_state() -> None:
    defaults = {
        "creating_measurement_for": None,
        "selected_measurement": None,
        "selected_view": None,
        "selected_device_structure": None,
        "show_all_structures": False,
        "cole_cole_synced": False,
        "standard_plot_synced": False,
        "nanothickness_synced": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _bootstrap(paths) -> None:
    paths.data_root.mkdir(exist_ok=True)
    paths.db_dir.mkdir(exist_ok=True)
    (paths.data_root / "devices").mkdir(exist_ok=True)

    if "bootstrapped" in st.session_state:
        return

    # Always run migrations to upgrade existing DBs too.
    migrate_sqlite(paths.db_path)

    # Always sync from server on start (keeps behavior of original app.py)
    sync_server_to_sqlite(paths.db_path)

    st.session_state.bootstrapped = True
    st.session_state.logged_in = False
    st.session_state.username = None


def run() -> None:
    st.set_page_config(
        page_title="Thermal Data Local Measurement Manager",
        layout="wide",
    )

    paths = get_paths()
    ctx = LocalContext(db_path=paths.db_path, data_root=paths.data_root)

    _init_session_state()
    _bootstrap(paths)

    # ================================
    # HEADER
    # ================================
    col_title, col_user = st.columns([8, 2])
    with col_title:
        st.title("Thermal Data Local Measurement Manager")
    with col_user:
        if st.session_state.get("logged_in"):
            col_u1, col_u2 = st.columns([3, 2])
            with col_u1:
                st.markdown(
                    f"""
                    <div style="text-align: right; padding-top: 8px;">
                        👤 <b>{st.session_state.username}</b>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with col_u2:
                if st.button("Logout"):
                    st.session_state.logged_in = False
                    st.session_state.username = None
                    st.session_state.selected_measurement = None
                    st.session_state.selected_view = None
                    st.session_state.selected_device_structure = None
                    st.session_state.show_all_structures = False
                    st.rerun()

    # ================================
    # LOGIN
    # ================================
    if not st.session_state.logged_in:
        st.subheader("🔐 Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        if st.button("Login"):
            if not username or not password:
                st.error("Username and password are required")
                return

            import sqlite3

            conn = sqlite3.connect(paths.db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT username, active, hashed_password
                FROM users
                WHERE username = ?
                """,
                (username,),
            )
            row = cur.fetchone()
            conn.close()

            if not row:
                st.error("❌ User not found")
            elif not row[1]:
                st.error("🚫 User is inactive")
            else:
                db_username, active, db_hashed_password = row
                if not Hasher.verify_password(password, db_hashed_password):
                    st.error("❌ Invalid password")
                else:
                    st.session_state.logged_in = True
                    st.session_state.username = db_username
                    try:
                        sync_server_to_sqlite(paths.db_path)
                    except Exception as e:
                        st.warning(f"Could not sync from server: {e}. Using local data.")
                    sync_db_to_filesystem(ctx)
                    st.success(f"Logged in as {db_username}")
                    st.rerun()

        st.stop()

    # ================================
    # MAIN PANEL
    # ================================
    if st.session_state.show_all_structures:
        st.subheader("All Device Structures")

        import sqlite3

        conn = sqlite3.connect(paths.db_path)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT name, structure_json
            FROM devices
            WHERE structure_json IS NOT NULL
            ORDER BY name
            """
        )
        rows = cur.fetchall()
        conn.close()

        if not rows:
            st.info("No device structures defined")
        else:
            import json

            for device_name, structure_json in rows:
                col_left, col_right = st.columns([1, 7])
                with col_left:
                    if st.button(
                        f"📁 {device_name}",
                        key=f"select_from_all_{device_name}",
                        use_container_width=True,
                    ):
                        st.session_state.selected_device_structure = device_name
                        st.session_state.selected_measurement = None
                        st.session_state.selected_view = None
                        st.session_state.show_all_structures = False
                        st.rerun()
                with col_right:
                    try:
                        structure = json.loads(structure_json) if structure_json else None
                        if isinstance(structure, dict):
                            df = pd.DataFrame([structure])
                            st.dataframe(df, use_container_width=True)
                        else:
                            st.warning("Structure must be a JSON object")
                    except json.JSONDecodeError:
                        st.error("Invalid JSON in structure_json")
                st.divider()

    if st.session_state.selected_device_structure:
        device_name = st.session_state.selected_device_structure
        st.subheader("Device Structure")
        st.caption(f"Device: {device_name}")
        structure = get_device_structure(paths.db_path, device_name)

        if structure is None:
            st.warning("No structure defined for this device yet")
        elif "_error" in structure:
            st.error("Device structure JSON is invalid")
            st.code(structure, language="json")
        else:
            if not isinstance(structure, dict):
                st.error("Device structure must be a JSON object (dict)")
            else:
                df = pd.DataFrame([structure])
                st.dataframe(df, use_container_width=True)
        st.divider()

    if st.session_state.selected_measurement and st.session_state.selected_view:
        device_name, measurement_name = st.session_state.selected_measurement
        view = st.session_state.selected_view

        st.subheader(f"📄 {measurement_name}")
        st.caption(f"Device: {device_name}")

        base = paths.data_root / "devices" / device_name / measurement_name
        measurement_id = get_measurement_id(paths.db_path, device_name, measurement_name)
        can_edit = is_measurement_owner(paths.db_path, measurement_id, st.session_state.username)

        if view == "cole_cole":
            st.markdown("### Cole–Cole")

            if st.session_state.get("cole_cole_synced"):
                st.success("Cole–Cole synced")
                st.session_state.cole_cole_synced = False

            cc_files = list(base.glob("CC_*.csv"))

            if cc_files:
                df = read_cole_cole_csv(cc_files[0])
                st.dataframe(df)

                if not has_cole_cole(paths.db_path, measurement_id):
                    if not can_edit:
                        st.info("Cole–Cole data not in DB. Only the creator of this measurement can add or sync data.")
                    else:
                        if st.button("Add Cole–Cole to DB"):
                            insert_cole_cole(paths.db_path, measurement_id, df)
                            sync_sqlite_to_server(paths.db_path, measurement_id)
                            st.session_state.cole_cole_synced = True
                            st.rerun()
                else:
                    if not can_edit:
                        st.info("Cole–Cole data already in DB. Only the creator of this measurement can sync again.")
                    else:
                        st.info("Cole-Cole data already in DB, do you want to sync again?")
                        if st.button("Sync again"):
                            insert_cole_cole(paths.db_path, measurement_id, df)
                            sync_sqlite_to_server(paths.db_path, measurement_id)
                            st.session_state.cole_cole_synced = True
                            st.rerun()
                    # st.success("Cole–Cole already in DB")
            else:
                df = read_cole_cole_from_db(paths.db_path, measurement_id)
                if not df.empty:
                    st.info("Loaded from DB (no local CSV)")
                    st.dataframe(df)
                else:
                    st.info("No Cole–Cole data available")

        elif view == "standard_plot":
            st.markdown("### Standard Plot")

            if st.session_state.get("standard_plot_synced"):
                st.success("Standard Plot synced")
                st.session_state.standard_plot_synced = False

            sp_files = list(base.glob("_*.csv"))

            if sp_files:
                df = read_standard_plot_csv(sp_files[0])
                st.dataframe(df)

                if not has_standard_plot(paths.db_path, measurement_id):
                    if not can_edit:
                        st.info("Standard Plot data not in DB. Only the creator of this measurement can add or sync data.")
                    else:
                        if st.button("Add Standard Plot to DB"):
                            insert_standard_plot(paths.db_path, measurement_id, df)
                            sync_sqlite_to_server(paths.db_path, measurement_id)
                            st.session_state.standard_plot_synced = True
                            st.rerun()
                else:
                    if not can_edit:
                        st.info("Standard Plot data already in DB. Only the creator of this measurement can sync again.")
                    else:
                        st.info("Standard Plot data already in DB, do you want to sync again?")
                        if st.button("Sync again"):
                            insert_standard_plot(paths.db_path, measurement_id, df)
                            sync_sqlite_to_server(paths.db_path, measurement_id)
                            st.session_state.standard_plot_synced = True
                            st.rerun()
            else:
                df = read_standard_plot_from_db(paths.db_path, measurement_id)
                if not df.empty:
                    st.info("Loaded from DB (no local CSV)")
                    st.dataframe(df)
                else:
                    st.info("No Standard Plot data available")

        elif view == "nanothickness":
            st.markdown("### Nanothickness")

            if st.session_state.get("nanothickness_synced"):
                st.success("Nanothickness synced")
                st.session_state.nanothickness_synced = False

            nano_files = list(base.glob("nn_*.csv"))

            if nano_files:
                df = read_nanothickness_csv(nano_files[0])
                st.dataframe(df)

                if not has_nanothickness(paths.db_path, measurement_id):
                    if not can_edit:
                        st.info("Nanothickness data not in DB. Only the creator of this measurement can add or sync data.")
                    else:
                        if st.button("Add Nanothickness to DB"):
                            insert_nanothickness(paths.db_path, measurement_id, df)
                            sync_sqlite_to_server(paths.db_path, measurement_id)
                            st.session_state.nanothickness_synced = True
                            st.rerun()
                else:
                    if not can_edit:
                        st.info("Nanothickness data already in DB. Only the creator of this measurement can sync again.")
                    else:
                        st.info("Nanothickness data already in DB, do you want to sync again?")
                        if st.button("Sync again"):
                            insert_nanothickness(paths.db_path, measurement_id, df)
                            sync_sqlite_to_server(paths.db_path, measurement_id)
                            st.session_state.nanothickness_synced = True
                            st.rerun()
            else:
                df = read_nanothickness_from_db(paths.db_path, measurement_id)
                if not df.empty:
                    st.info("Loaded from DB (no local CSV)")
                    st.dataframe(df)
                else:
                    st.info("No Nanothickness data available")
    # ================================
    # SIDEBAR
    # ================================
    st.sidebar.title("📂 Devices Structures")

    if st.sidebar.button("Show all device structures", use_container_width=True):
        st.session_state.show_all_structures = True
        st.session_state.selected_device_structure = None
        st.session_state.selected_measurement = None
        st.session_state.selected_view = None
        st.rerun()

    st.sidebar.title("📂 Devices and Measurements")

    devices = get_devices_and_measurements(paths.db_path)
    for device_name, measurements in devices.items():
        is_selected = False
        if st.session_state.selected_device_structure == device_name:
            is_selected = True
        elif (
            st.session_state.selected_measurement is not None
            and st.session_state.selected_measurement[0] == device_name
        ):
            is_selected = True

        with st.sidebar.expander(f"📁 {device_name}", expanded=is_selected):
            device_id = get_device_id(paths.db_path, device_name)

            for m in measurements:
                with st.expander(f"📁 {m}"):
                    if st.button("Cole–Cole", key=f"cc_{device_name}_{m}", use_container_width=True):
                        st.session_state.selected_device_structure = device_name
                        st.session_state.selected_measurement = (device_name, m)
                        st.session_state.selected_view = "cole_cole"
                        st.session_state.show_all_structures = False
                        st.rerun()

                    if st.button("Standard Plot", key=f"sp_{device_name}_{m}", use_container_width=True):
                        st.session_state.selected_device_structure = device_name
                        st.session_state.selected_measurement = (device_name, m)
                        st.session_state.selected_view = "standard_plot"
                        st.session_state.show_all_structures = False
                        st.rerun()

                    if st.button("Nanothickness", key=f"nano_{device_name}_{m}", use_container_width=True):
                        st.session_state.selected_device_structure = device_name
                        st.session_state.selected_measurement = (device_name, m)
                        st.session_state.selected_view = "nanothickness"
                        st.session_state.show_all_structures = False
                        st.rerun()

                    # Soft delete measurement (and related data) if created_by matches logged-in user
                    if st.button("🗑 Delete measurement", key=f"del_{device_name}_{m}", use_container_width=True):
                        try:
                            soft_delete_measurement(
                                paths.db_path,
                                device_name=device_name,
                                measurement_name=m,
                                username=st.session_state.username,
                            )
                            st.success("Measurement deleted")
                            st.rerun()
                        except PermissionError:
                            st.error("You can only delete measurements you created")
                        except Exception as e:
                            st.error(f"Error deleting measurement: {e}")

            # ===== CREATE FLOW =====
            device_folder = paths.data_root / "devices" / device_name
            if st.session_state.creating_measurement_for != device_name:
                if st.button("➕ New Measurement", key=f"new_{device_name}", use_container_width=True):
                    st.session_state.creating_measurement_for = device_name
                    st.rerun()

                if st.button("📂 Open in Folder", key=f"open_folder_{device_name}", use_container_width=True):
                    _open_folder(device_folder)
            else:
                name = st.text_input("Measurement name", key=f"name_{device_name}")
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Create", key=f"create_{device_name}"):
                        if not name.strip():
                            st.warning("Measurement name is required")
                        else:
                            try:
                                create_measurement(
                                    ctx,
                                    device_name=device_name,
                                    device_id=device_id,
                                    measurement_name=name,
                                    created_by=st.session_state.username,
                                )
                                st.session_state.creating_measurement_for = None
                                st.rerun()
                            except ValueError as e:
                                st.error(str(e))
                with col2:
                    if st.button("Cancel", key=f"cancel_{device_name}"):
                        st.session_state.creating_measurement_for = None
                        st.rerun()

