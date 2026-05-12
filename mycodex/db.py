from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable


DEFAULT_DB_PATH = Path(os.environ.get("MYCODEX_DB", "data/my_codex.sqlite"))


def now_ts() -> int:
    return int(time.time())


def resolve_db_path(path: str | Path | None = None) -> Path:
    db_path = Path(path) if path else DEFAULT_DB_PATH
    return db_path.expanduser().resolve()


class Store:
    def __init__(self, db_path: str | Path | None = None) -> None:
        self.path = resolve_db_path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.init_schema()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                path TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL DEFAULT '',
                default_thread_id TEXT,
                sandbox TEXT NOT NULL DEFAULT 'read-only',
                approval_policy TEXT NOT NULL DEFAULT 'never',
                model TEXT,
                reasoning_effort TEXT,
                daily_summary_enabled INTEGER NOT NULL DEFAULT 0,
                daily_summary_time TEXT NOT NULL DEFAULT '18:00',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                thread_id TEXT PRIMARY KEY,
                project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                session_id TEXT,
                path TEXT,
                cwd TEXT NOT NULL,
                preview TEXT NOT NULL DEFAULT '',
                name TEXT,
                source TEXT,
                model_provider TEXT,
                cli_version TEXT,
                status_type TEXT NOT NULL DEFAULT 'unknown',
                status_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER,
                updated_at INTEGER,
                git_sha TEXT,
                git_branch TEXT,
                git_origin_url TEXT,
                synced_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_sessions_project_updated
                ON sessions(project_id, updated_at DESC);

            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                thread_id TEXT,
                kind TEXT NOT NULL,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL,
                output_text TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT '{}',
                started_at INTEGER NOT NULL,
                completed_at INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_runs_project_started
                ON runs(project_id, started_at DESC);

            CREATE TABLE IF NOT EXISTS rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                kind TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                time_of_day TEXT,
                interval_seconds INTEGER,
                prompt TEXT NOT NULL DEFAULT '',
                last_run_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(project_id, name)
            );
            """
        )
        self.conn.commit()

    def add_project(
        self,
        name: str,
        path: str | Path,
        *,
        description: str = "",
        sandbox: str = "read-only",
        approval_policy: str = "never",
        model: str | None = None,
        reasoning_effort: str | None = None,
        daily_summary_enabled: bool = False,
        daily_summary_time: str = "18:00",
    ) -> sqlite3.Row:
        ts = now_ts()
        resolved_path = str(Path(path).expanduser().resolve())
        self.conn.execute(
            """
            INSERT INTO projects (
                name, path, description, sandbox, approval_policy, model,
                reasoning_effort, daily_summary_enabled, daily_summary_time,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                path = excluded.path,
                description = excluded.description,
                sandbox = excluded.sandbox,
                approval_policy = excluded.approval_policy,
                model = excluded.model,
                reasoning_effort = excluded.reasoning_effort,
                daily_summary_enabled = excluded.daily_summary_enabled,
                daily_summary_time = excluded.daily_summary_time,
                updated_at = excluded.updated_at
            """,
            (
                name,
                resolved_path,
                description,
                sandbox,
                approval_policy,
                model,
                reasoning_effort,
                int(daily_summary_enabled),
                daily_summary_time,
                ts,
                ts,
            ),
        )
        self.conn.commit()
        project = self.get_project(name)
        if project is None:
            raise RuntimeError("project insert failed")
        return project

    def list_projects(self) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM projects ORDER BY name COLLATE NOCASE"
            )
        )

    def get_project(self, name: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM projects WHERE name = ?", (name,)
        ).fetchone()

    def get_project_by_path(self, path: str | Path) -> sqlite3.Row | None:
        resolved_path = str(Path(path).expanduser().resolve())
        return self.conn.execute(
            "SELECT * FROM projects WHERE path = ?", (resolved_path,)
        ).fetchone()

    def remove_project(self, name: str) -> bool:
        cur = self.conn.execute("DELETE FROM projects WHERE name = ?", (name,))
        self.conn.commit()
        return cur.rowcount > 0

    def update_default_thread(self, project_id: int, thread_id: str | None) -> None:
        self.conn.execute(
            "UPDATE projects SET default_thread_id = ?, updated_at = ? WHERE id = ?",
            (thread_id, now_ts(), project_id),
        )
        self.conn.commit()

    def upsert_session(self, project_id: int, thread: dict[str, Any]) -> None:
        status = thread.get("status") or {}
        git = thread.get("gitInfo") or {}
        self.conn.execute(
            """
            INSERT INTO sessions (
                thread_id, project_id, session_id, path, cwd, preview, name,
                source, model_provider, cli_version, status_type, status_json,
                created_at, updated_at, git_sha, git_branch, git_origin_url,
                synced_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(thread_id) DO UPDATE SET
                project_id = excluded.project_id,
                session_id = excluded.session_id,
                path = excluded.path,
                cwd = excluded.cwd,
                preview = excluded.preview,
                name = excluded.name,
                source = excluded.source,
                model_provider = excluded.model_provider,
                cli_version = excluded.cli_version,
                status_type = excluded.status_type,
                status_json = excluded.status_json,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                git_sha = excluded.git_sha,
                git_branch = excluded.git_branch,
                git_origin_url = excluded.git_origin_url,
                synced_at = excluded.synced_at
            """,
            (
                thread["id"],
                project_id,
                thread.get("sessionId"),
                thread.get("path"),
                thread.get("cwd") or "",
                thread.get("preview") or "",
                thread.get("name"),
                thread.get("source"),
                thread.get("modelProvider"),
                thread.get("cliVersion"),
                status.get("type", "unknown"),
                json.dumps(status, ensure_ascii=False),
                thread.get("createdAt"),
                thread.get("updatedAt"),
                git.get("sha"),
                git.get("branch"),
                git.get("originUrl"),
                now_ts(),
            ),
        )

    def commit(self) -> None:
        self.conn.commit()

    def list_sessions(self, project_id: int) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT * FROM sessions
                WHERE project_id = ?
                ORDER BY COALESCE(updated_at, 0) DESC, thread_id
                """,
                (project_id,),
            )
        )

    def latest_session(self, project_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT * FROM sessions
            WHERE project_id = ?
            ORDER BY COALESCE(updated_at, 0) DESC, thread_id
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()

    def add_run(
        self,
        project_id: int,
        *,
        kind: str,
        prompt: str,
        thread_id: str | None = None,
        status: str = "running",
    ) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO runs (project_id, thread_id, kind, prompt, status, started_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (project_id, thread_id, kind, prompt, status, now_ts()),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        output_text: str = "",
        error: str = "",
        raw_json: dict[str, Any] | None = None,
        thread_id: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE runs
            SET status = ?, output_text = ?, error = ?, raw_json = ?,
                thread_id = COALESCE(?, thread_id), completed_at = ?
            WHERE id = ?
            """,
            (
                status,
                output_text,
                error,
                json.dumps(raw_json or {}, ensure_ascii=False),
                thread_id,
                now_ts(),
                run_id,
            ),
        )
        self.conn.commit()

    def list_runs(self, project_id: int, limit: int = 10) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT * FROM runs
                WHERE project_id = ?
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (project_id, limit),
            )
        )

    def add_rule(
        self,
        project_id: int,
        *,
        name: str,
        kind: str,
        time_of_day: str | None = None,
        interval_seconds: int | None = None,
        prompt: str = "",
        enabled: bool = True,
    ) -> None:
        ts = now_ts()
        self.conn.execute(
            """
            INSERT INTO rules (
                project_id, name, kind, enabled, time_of_day,
                interval_seconds, prompt, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id, name) DO UPDATE SET
                kind = excluded.kind,
                enabled = excluded.enabled,
                time_of_day = excluded.time_of_day,
                interval_seconds = excluded.interval_seconds,
                prompt = excluded.prompt,
                updated_at = excluded.updated_at
            """,
            (
                project_id,
                name,
                kind,
                int(enabled),
                time_of_day,
                interval_seconds,
                prompt,
                ts,
                ts,
            ),
        )
        self.conn.commit()

    def list_rules(self, project_id: int | None = None) -> list[sqlite3.Row]:
        if project_id is None:
            return list(
                self.conn.execute(
                    """
                    SELECT rules.*, projects.name AS project_name
                    FROM rules
                    JOIN projects ON projects.id = rules.project_id
                    ORDER BY projects.name COLLATE NOCASE, rules.name
                    """
                )
            )
        return list(
            self.conn.execute(
                """
                SELECT rules.*, projects.name AS project_name
                FROM rules
                JOIN projects ON projects.id = rules.project_id
                WHERE project_id = ?
                ORDER BY rules.name
                """,
                (project_id,),
            )
        )

    def update_rule_run(self, rule_id: int) -> None:
        self.conn.execute(
            "UPDATE rules SET last_run_at = ?, updated_at = ? WHERE id = ?",
            (now_ts(), now_ts(), rule_id),
        )
        self.conn.commit()

    def has_run_since(
        self,
        project_id: int,
        *,
        kind: str,
        since_ts: int,
        statuses: Iterable[str] = ("completed", "running"),
    ) -> bool:
        placeholders = ",".join("?" for _ in statuses)
        row = self.conn.execute(
            f"""
            SELECT 1 FROM runs
            WHERE project_id = ? AND kind = ? AND started_at >= ?
              AND status IN ({placeholders})
            LIMIT 1
            """,
            (project_id, kind, since_ts, *statuses),
        ).fetchone()
        return row is not None
