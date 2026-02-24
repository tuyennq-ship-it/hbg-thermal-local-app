from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppPaths:
    project_dir: Path
    data_root: Path
    db_dir: Path
    db_path: Path


def get_paths(project_dir: Path | None = None) -> AppPaths:
    base = project_dir or Path(__file__).resolve().parents[1]
    data_root = base / "root"
    db_dir = data_root / "db"
    db_path = db_dir / "app.db"
    return AppPaths(
        project_dir=base,
        data_root=data_root,
        db_dir=db_dir,
        db_path=db_path,
    )

