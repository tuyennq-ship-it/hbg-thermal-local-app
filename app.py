"""
Root Streamlit entrypoint.

The application has been refactored into the `thermal_local` package to keep the
UI, DB layer, and services modular and easier to extend.
"""

from thermal_local.ui.app import run


run()

