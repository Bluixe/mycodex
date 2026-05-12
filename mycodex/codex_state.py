from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any


DEFAULT_CODEX_STATE_DB = Path(
    os.environ.get("MYCODEX_CODEX_STATE_DB", "~/.codex/state_5.sqlite")
).expanduser()


def discover_codex_projects(
    *,
    db_path: str | Path | None = None,
    limit: int = 50,
    include_archived: bool = False,
    search: str | None = None,
) -> list[dict[str, Any]]:
    """Return unique Codex project candidates grouped by session cwd."""

    state_db = Path(db_path).expanduser() if db_path else DEFAULT_CODEX_STATE_DB
    if not state_db.exists():
        raise FileNotFoundError(f"Codex state DB not found: {state_db}")

    uri = f"file:{state_db}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        where = ["cwd IS NOT NULL", "cwd != ''"]
        params: list[Any] = []
        if not include_archived:
            where.append("archived = 0")
        if search:
            where.append("(cwd LIKE ? OR title LIKE ? OR first_user_message LIKE ?)")
            pattern = f"%{search}%"
            params.extend([pattern, pattern, pattern])

        where_sql = " AND ".join(where)
        grouped = list(
            conn.execute(
                f"""
                SELECT
                    cwd,
                    COUNT(*) AS session_count,
                    MAX(updated_at) AS latest_updated_at,
                    GROUP_CONCAT(DISTINCT source) AS sources
                FROM threads
                WHERE {where_sql}
                GROUP BY cwd
                ORDER BY latest_updated_at DESC
                LIMIT ?
                """,
                (*params, limit),
            )
        )

        projects: list[dict[str, Any]] = []
        for row in grouped:
            latest = conn.execute(
                f"""
                SELECT id, title, first_user_message, source, updated_at
                FROM threads
                WHERE {where_sql} AND cwd = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (*params, row["cwd"]),
            ).fetchone()
            projects.append(
                {
                    "cwd": row["cwd"],
                    "session_count": row["session_count"],
                    "latest_updated_at": row["latest_updated_at"],
                    "sources": row["sources"] or "",
                    "latest_thread_id": latest["id"] if latest else None,
                    "latest_title": latest["title"] if latest else "",
                    "latest_preview": latest["first_user_message"] if latest else "",
                }
            )
        return projects
    finally:
        conn.close()
