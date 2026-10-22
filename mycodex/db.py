from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable, Sequence


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
        self.conn = sqlite3.connect(self.path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA busy_timeout = 30000")
        self.conn.execute("PRAGMA journal_mode = WAL")
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

            CREATE TABLE IF NOT EXISTS experiments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                objective TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'planned',
                phase TEXT NOT NULL DEFAULT 'initialized',
                phase_message TEXT NOT NULL DEFAULT '',
                latest_decision TEXT NOT NULL DEFAULT '',
                last_metric_json TEXT NOT NULL DEFAULT '{}',
                artifact_ref TEXT NOT NULL DEFAULT '',
                max_iterations INTEGER,
                current_iteration INTEGER NOT NULL DEFAULT 0,
                remote_ref TEXT NOT NULL DEFAULT '',
                config_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                started_at INTEGER,
                completed_at INTEGER,
                UNIQUE(project_id, name)
            );

            CREATE INDEX IF NOT EXISTS idx_experiments_project_status
                ON experiments(project_id, status, updated_at DESC);

            CREATE TABLE IF NOT EXISTS experiment_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                experiment_id INTEGER NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
                iteration INTEGER,
                event_type TEXT NOT NULL,
                level TEXT NOT NULL DEFAULT 'info',
                message TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                source TEXT NOT NULL DEFAULT 'manual',
                run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
                created_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_experiment_events_experiment_created
                ON experiment_events(experiment_id, created_at DESC, id DESC);

            CREATE TABLE IF NOT EXISTS experiment_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                experiment_id INTEGER NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
                task_type TEXT NOT NULL DEFAULT 'execution_contract',
                status TEXT NOT NULL DEFAULT 'proposed',
                execution_class TEXT NOT NULL DEFAULT '',
                idempotency_key TEXT NOT NULL,
                contract_json TEXT NOT NULL DEFAULT '{}',
                source_event_id INTEGER REFERENCES experiment_events(id) ON DELETE SET NULL,
                run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
                lease_owner TEXT NOT NULL DEFAULT '',
                lease_expires_at INTEGER,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                result_json TEXT NOT NULL DEFAULT '{}',
                error TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                ready_at INTEGER,
                started_at INTEGER,
                completed_at INTEGER,
                UNIQUE(experiment_id, idempotency_key)
            );

            CREATE INDEX IF NOT EXISTS idx_experiment_tasks_experiment_status
                ON experiment_tasks(experiment_id, status, updated_at DESC, id DESC);

            CREATE TABLE IF NOT EXISTS research_threads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                question TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'planned',
                phase TEXT NOT NULL DEFAULT 'initialized',
                phase_message TEXT NOT NULL DEFAULT '',
                latest_decision TEXT NOT NULL DEFAULT '',
                max_iterations INTEGER,
                current_iteration INTEGER NOT NULL DEFAULT 0,
                config_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                started_at INTEGER,
                completed_at INTEGER,
                UNIQUE(project_id, name)
            );

            CREATE INDEX IF NOT EXISTS idx_research_threads_project_status
                ON research_threads(project_id, status, updated_at DESC);

            CREATE TABLE IF NOT EXISTS research_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                research_id INTEGER NOT NULL REFERENCES research_threads(id) ON DELETE CASCADE,
                iteration INTEGER,
                event_type TEXT NOT NULL,
                level TEXT NOT NULL DEFAULT 'info',
                message TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                source TEXT NOT NULL DEFAULT 'manual',
                run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
                created_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_research_events_research_created
                ON research_events(research_id, created_at DESC, id DESC);

            CREATE TABLE IF NOT EXISTS research_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                research_id INTEGER NOT NULL REFERENCES research_threads(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                url TEXT NOT NULL DEFAULT '',
                source_type TEXT NOT NULL DEFAULT '',
                role TEXT NOT NULL DEFAULT '',
                depth TEXT NOT NULL DEFAULT 'title',
                status TEXT NOT NULL DEFAULT 'candidate',
                relevance INTEGER NOT NULL DEFAULT 0,
                evidence_quality INTEGER NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(research_id, url)
            );

            CREATE INDEX IF NOT EXISTS idx_research_sources_research_status
                ON research_sources(research_id, status, role, updated_at DESC);

            CREATE TABLE IF NOT EXISTS research_claims (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                research_id INTEGER NOT NULL REFERENCES research_threads(id) ON DELETE CASCADE,
                source_id INTEGER REFERENCES research_sources(id) ON DELETE SET NULL,
                claim TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'uncertain',
                support TEXT NOT NULL DEFAULT '',
                confidence INTEGER NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_research_claims_research_status
                ON research_claims(research_id, status, updated_at DESC);

            CREATE TABLE IF NOT EXISTS research_ideas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                research_id INTEGER NOT NULL REFERENCES research_threads(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                hypothesis TEXT NOT NULL DEFAULT '',
                mechanism TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'candidate',
                score_json TEXT NOT NULL DEFAULT '{}',
                minimal_test TEXT NOT NULL DEFAULT '',
                kill_criteria TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_research_ideas_research_status
                ON research_ideas(research_id, status, updated_at DESC);

            CREATE TABLE IF NOT EXISTS research_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                research_id INTEGER NOT NULL REFERENCES research_threads(id) ON DELETE CASCADE,
                task_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                title TEXT NOT NULL DEFAULT '',
                query TEXT NOT NULL DEFAULT '',
                source_id INTEGER REFERENCES research_sources(id) ON DELETE SET NULL,
                idea_id INTEGER REFERENCES research_ideas(id) ON DELETE SET NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                result_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_research_tasks_research_status
                ON research_tasks(research_id, status, updated_at DESC);

            CREATE TABLE IF NOT EXISTS todos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                body TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'open',
                priority INTEGER NOT NULL DEFAULT 2,
                source_run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
                due_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_todos_project_status
                ON todos(project_id, status, priority, updated_at DESC);

            CREATE TABLE IF NOT EXISTS rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                kind TEXT NOT NULL,
                thread_id TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                time_of_day TEXT,
                interval_seconds INTEGER,
                prompt TEXT NOT NULL DEFAULT '',
                last_run_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(project_id, name)
            );

            CREATE TABLE IF NOT EXISTS telegram_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL UNIQUE,
                username TEXT NOT NULL DEFAULT '',
                allowed INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS telegram_bindings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                message_thread_id TEXT NOT NULL DEFAULT '',
                project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                thread_id TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(chat_id, message_thread_id)
            );

            CREATE INDEX IF NOT EXISTS idx_telegram_bindings_project
                ON telegram_bindings(project_id);

            CREATE TABLE IF NOT EXISTS token_usage_events (
                event_key TEXT PRIMARY KEY,
                response_id TEXT NOT NULL DEFAULT '',
                log_id INTEGER NOT NULL DEFAULT 0,
                ts INTEGER NOT NULL,
                thread_id TEXT NOT NULL DEFAULT '',
                cwd TEXT NOT NULL DEFAULT '',
                process_uuid TEXT NOT NULL DEFAULT '',
                project_id INTEGER REFERENCES projects(id) ON DELETE SET NULL,
                project_name TEXT NOT NULL DEFAULT '',
                model TEXT NOT NULL DEFAULT 'unknown',
                input_tokens INTEGER NOT NULL DEFAULT 0,
                output_tokens INTEGER NOT NULL DEFAULT 0,
                total_tokens INTEGER NOT NULL DEFAULT 0,
                cached_tokens INTEGER NOT NULL DEFAULT 0,
                reasoning_tokens INTEGER NOT NULL DEFAULT 0,
                first_seen_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_token_usage_ts
                ON token_usage_events(ts DESC);

            CREATE INDEX IF NOT EXISTS idx_token_usage_project_ts
                ON token_usage_events(project_name, ts DESC);
            """
        )
        self._ensure_column("experiments", "phase", "TEXT NOT NULL DEFAULT 'initialized'")
        self._ensure_column("experiments", "phase_message", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column("experiments", "latest_decision", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column("experiments", "last_metric_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column("experiments", "artifact_ref", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column("rules", "thread_id", "TEXT")
        self.conn.commit()
        from .jobs import initialize_job_schema

        initialize_job_schema(self.conn)

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        columns = {row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

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

    def get_run(self, run_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT runs.*, projects.name AS project_name, projects.path AS project_path
            FROM runs
            JOIN projects ON projects.id = runs.project_id
            WHERE runs.id = ?
            """,
            (run_id,),
        ).fetchone()

    def active_run_for_thread(self, thread_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT runs.*, projects.name AS project_name
            FROM runs
            JOIN projects ON projects.id = runs.project_id
            WHERE runs.thread_id = ? AND runs.status = 'running'
            ORDER BY runs.started_at DESC
            LIMIT 1
            """,
            (thread_id,),
        ).fetchone()

    def active_run_for_project(
        self,
        project_id: int,
        *,
        kind_prefix: str | None = None,
    ) -> sqlite3.Row | None:
        params: list[Any] = [project_id]
        kind_clause = ""
        if kind_prefix:
            kind_clause = "AND runs.kind LIKE ?"
            params.append(f"{kind_prefix}%")
        return self.conn.execute(
            f"""
            SELECT runs.*, projects.name AS project_name
            FROM runs
            JOIN projects ON projects.id = runs.project_id
            WHERE runs.project_id = ? AND runs.status = 'running'
            {kind_clause}
            ORDER BY runs.started_at DESC
            LIMIT 1
            """,
            params,
        ).fetchone()

    def active_runs_for_project(
        self,
        project_id: int,
        *,
        kind_prefix: str | None = None,
        limit: int = 50,
    ) -> list[sqlite3.Row]:
        params: list[Any] = [project_id]
        kind_clause = ""
        if kind_prefix:
            kind_clause = "AND runs.kind LIKE ?"
            params.append(f"{kind_prefix}%")
        params.append(limit)
        return list(
            self.conn.execute(
                f"""
                SELECT runs.*, projects.name AS project_name
                FROM runs
                JOIN projects ON projects.id = runs.project_id
                WHERE runs.project_id = ? AND runs.status = 'running'
                {kind_clause}
                ORDER BY runs.started_at DESC
                LIMIT ?
                """,
                params,
            )
        )

    def add_experiment(
        self,
        project_id: int,
        *,
        name: str,
        thread_id: str,
        objective: str = "",
        max_iterations: int | None = None,
        remote_ref: str = "",
        config: dict[str, Any] | None = None,
        status: str = "planned",
    ) -> sqlite3.Row:
        name = name.strip()
        if not name:
            raise ValueError("experiment name is required")
        thread_id = thread_id.strip()
        if not thread_id:
            raise ValueError("experiment thread_id is required")
        status = _validate_experiment_status(status)
        if max_iterations is not None and max_iterations < 1:
            raise ValueError("experiment max_iterations must be positive")
        ts = now_ts()
        started_at = ts if status == "running" else None
        completed_at = ts if status in {"completed", "failed"} else None
        self.conn.execute(
            """
            INSERT INTO experiments (
                project_id, name, thread_id, objective, status, max_iterations,
                phase, phase_message, remote_ref, config_json, created_at,
                updated_at, started_at, completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id, name) DO UPDATE SET
                thread_id = excluded.thread_id,
                objective = excluded.objective,
                status = excluded.status,
                max_iterations = excluded.max_iterations,
                phase = CASE
                    WHEN experiments.phase = 'initialized' THEN excluded.phase
                    ELSE experiments.phase
                END,
                phase_message = CASE
                    WHEN experiments.phase_message = '' THEN excluded.phase_message
                    ELSE experiments.phase_message
                END,
                remote_ref = excluded.remote_ref,
                config_json = excluded.config_json,
                updated_at = excluded.updated_at,
                started_at = COALESCE(experiments.started_at, excluded.started_at),
                completed_at = excluded.completed_at
            """,
            (
                project_id,
                name,
                thread_id,
                objective.strip(),
                status,
                max_iterations,
                _phase_for_status(status),
                "Experiment configured.",
                remote_ref.strip(),
                json.dumps(config or {}, ensure_ascii=False),
                ts,
                ts,
                started_at,
                completed_at,
            ),
        )
        self.conn.commit()
        experiment = self.get_experiment(project_id, name)
        if experiment is None:
            raise RuntimeError("experiment insert failed")
        return experiment

    def get_experiment(self, project_id: int, experiment: int | str) -> sqlite3.Row | None:
        if isinstance(experiment, int) or str(experiment).isdigit():
            where = "experiments.id = ? AND experiments.project_id = ?"
            params: tuple[Any, ...] = (int(experiment), project_id)
        else:
            where = "experiments.name = ? AND experiments.project_id = ?"
            params = (str(experiment), project_id)
        return self.conn.execute(
            f"""
            SELECT experiments.*, projects.name AS project_name, projects.path AS project_path
            FROM experiments
            JOIN projects ON projects.id = experiments.project_id
            WHERE {where}
            """,
            params,
        ).fetchone()

    def list_experiments(
        self,
        project_id: int,
        *,
        include_archived: bool = False,
    ) -> list[sqlite3.Row]:
        clauses = ["experiments.project_id = ?"]
        params: list[Any] = [project_id]
        if not include_archived:
            clauses.append("experiments.status != 'archived'")
        return list(
            self.conn.execute(
                f"""
                SELECT experiments.*, projects.name AS project_name
                FROM experiments
                JOIN projects ON projects.id = experiments.project_id
                WHERE {" AND ".join(clauses)}
                ORDER BY
                    CASE experiments.status
                        WHEN 'running' THEN 0
                        WHEN 'planned' THEN 1
                        WHEN 'paused' THEN 2
                        WHEN 'failed' THEN 3
                        WHEN 'completed' THEN 4
                        ELSE 5
                    END,
                    experiments.updated_at DESC,
                    experiments.id DESC
                """,
                params,
            )
        )

    def update_experiment_status(self, experiment_id: int, status: str) -> None:
        status = _validate_experiment_status(status)
        ts = now_ts()
        started_at_expr = "COALESCE(started_at, ?)" if status == "running" else "started_at"
        completed_at_value = ts if status in {"completed", "failed"} else None
        self.conn.execute(
            f"""
            UPDATE experiments
            SET status = ?,
                started_at = {started_at_expr},
                completed_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (status, ts, completed_at_value, ts, experiment_id)
            if status == "running"
            else (status, completed_at_value, ts, experiment_id),
        )
        self.conn.commit()

    def add_experiment_event(
        self,
        experiment_id: int,
        *,
        event_type: str,
        iteration: int | None = None,
        level: str = "info",
        message: str = "",
        payload: dict[str, Any] | None = None,
        source: str = "manual",
        run_id: int | None = None,
        status: str | None = None,
        phase: str | None = None,
        phase_message: str | None = None,
    ) -> int:
        event_type = event_type.strip()
        if not event_type:
            raise ValueError("experiment event_type is required")
        level = _validate_event_level(level)
        if iteration is not None and iteration < 0:
            raise ValueError("experiment iteration must be non-negative")
        ts = now_ts()
        try:
            cur = self.conn.execute(
                """
                INSERT INTO experiment_events (
                    experiment_id, iteration, event_type, level, message,
                    payload_json, source, run_id, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    experiment_id,
                    iteration,
                    event_type,
                    level,
                    message.strip(),
                    json.dumps(payload or {}, ensure_ascii=False),
                    source.strip() or "manual",
                    run_id,
                    ts,
                ),
            )
            fields = ["updated_at = ?"]
            params: list[Any] = [ts]
            if iteration is not None:
                fields.append("current_iteration = MAX(current_iteration, ?)")
                params.append(iteration)
            if status is not None:
                status = _validate_experiment_status(status)
                fields.append("status = ?")
                params.append(status)
                if phase is None and _phase_for_event(event_type, None) is None:
                    phase = _phase_for_status(status)
                if status == "running":
                    fields.append("started_at = COALESCE(started_at, ?)")
                    params.append(ts)
                if status in {"completed", "failed"}:
                    fields.append("completed_at = ?")
                    params.append(ts)
            resolved_phase = _phase_for_event(event_type, phase)
            if resolved_phase is not None:
                fields.append("phase = ?")
                params.append(resolved_phase)
            resolved_message = phase_message or _phase_message_for_event(event_type, message)
            if resolved_message:
                fields.append("phase_message = ?")
                params.append(resolved_message)
            if event_type in {"decision", "blocked", "failed", "completed", "wandb_run_finished"} and message.strip():
                fields.append("latest_decision = ?")
                params.append(message.strip())
            if event_type == "metric":
                fields.append("last_metric_json = ?")
                params.append(json.dumps(payload or {}, ensure_ascii=False))
            artifact = _artifact_ref_for_event(event_type, payload or {})
            if artifact:
                fields.append("artifact_ref = ?")
                params.append(artifact)
            params.append(experiment_id)
            self.conn.execute(
                f"UPDATE experiments SET {', '.join(fields)} WHERE id = ?",
                params,
            )
            self.conn.commit()
            return int(cur.lastrowid)
        except Exception:
            self.conn.rollback()
            raise

    def list_experiment_events(
        self,
        experiment_id: int,
        *,
        limit: int = 20,
    ) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT experiment_events.*, runs.status AS run_status
                FROM experiment_events
                LEFT JOIN runs ON runs.id = experiment_events.run_id
                WHERE experiment_events.experiment_id = ?
                ORDER BY experiment_events.created_at DESC, experiment_events.id DESC
                LIMIT ?
                """,
                (experiment_id, limit),
            )
        )

    def get_latest_experiment_event(
        self,
        experiment_id: int,
        event_type: str,
    ) -> sqlite3.Row | None:
        event_type = event_type.strip()
        if not event_type:
            raise ValueError("experiment event_type is required")
        return self.conn.execute(
            """
            SELECT experiment_events.*, runs.status AS run_status
            FROM experiment_events
            LEFT JOIN runs ON runs.id = experiment_events.run_id
            WHERE experiment_events.experiment_id = ?
              AND experiment_events.event_type = ?
            ORDER BY experiment_events.created_at DESC, experiment_events.id DESC
            LIMIT 1
            """,
            (experiment_id, event_type),
        ).fetchone()

    def upsert_experiment_task(
        self,
        experiment_id: int,
        *,
        task_type: str = "execution_contract",
        status: str = "proposed",
        execution_class: str = "",
        idempotency_key: str,
        contract: dict[str, Any] | None = None,
        source_event_id: int | None = None,
        run_id: int | None = None,
        max_attempts: int = 3,
        result: dict[str, Any] | None = None,
        error: str = "",
    ) -> sqlite3.Row:
        task_type = task_type.strip() or "execution_contract"
        status = _validate_experiment_task_status(status)
        idempotency_key = idempotency_key.strip()
        if not idempotency_key:
            raise ValueError("experiment task idempotency_key is required")
        if max_attempts < 1:
            raise ValueError("experiment task max_attempts must be positive")
        ts = now_ts()
        ready_at = ts if status == "ready" else None
        completed_at = ts if status in EXPERIMENT_TASK_TERMINAL_STATUSES else None
        payload = json.dumps(contract or {}, ensure_ascii=False, sort_keys=True)
        result_json = json.dumps(result or {}, ensure_ascii=False, sort_keys=True)
        try:
            if source_event_id is not None:
                source_event = self.conn.execute(
                    """
                    SELECT id
                    FROM experiment_events
                    WHERE id = ? AND experiment_id = ?
                    """,
                    (source_event_id, experiment_id),
                ).fetchone()
                if source_event is None:
                    source_event_id = None
            if run_id is not None and self.get_run(run_id) is None:
                run_id = None
            existing = self.conn.execute(
                """
                SELECT *
                FROM experiment_tasks
                WHERE experiment_id = ? AND idempotency_key = ?
                """,
                (experiment_id, idempotency_key),
            ).fetchone()
            if existing is None:
                cur = self.conn.execute(
                    """
                    INSERT INTO experiment_tasks (
                        experiment_id, task_type, status, execution_class,
                        idempotency_key, contract_json, source_event_id, run_id,
                        max_attempts, result_json, error, created_at, updated_at,
                        ready_at, completed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        experiment_id,
                        task_type,
                        status,
                        execution_class.strip(),
                        idempotency_key,
                        payload,
                        source_event_id,
                        run_id,
                        max_attempts,
                        result_json,
                        error.strip(),
                        ts,
                        ts,
                        ready_at,
                        completed_at,
                    ),
                )
                task_id = int(cur.lastrowid)
            else:
                task_id = int(existing["id"])
                if existing["status"] in {"proposed", "ready"}:
                    self.conn.execute(
                        """
                        UPDATE experiment_tasks
                        SET task_type = ?,
                            status = ?,
                            execution_class = ?,
                            contract_json = ?,
                            source_event_id = COALESCE(?, source_event_id),
                            run_id = COALESCE(?, run_id),
                            max_attempts = ?,
                            result_json = ?,
                            error = ?,
                            updated_at = ?,
                            ready_at = CASE
                                WHEN ? = 'ready' THEN COALESCE(ready_at, ?)
                                ELSE ready_at
                            END,
                            completed_at = CASE
                                WHEN ? IN ('succeeded', 'failed', 'blocked', 'cancelled') THEN COALESCE(completed_at, ?)
                                ELSE NULL
                            END
                        WHERE id = ? AND status IN ('proposed', 'ready')
                        """,
                        (
                            task_type,
                            status,
                            execution_class.strip(),
                            payload,
                            source_event_id,
                            run_id,
                            max_attempts,
                            result_json,
                            error.strip(),
                            ts,
                            status,
                            ts,
                            status,
                            ts,
                            task_id,
                        ),
                    )
            self.conn.commit()
            task = self.get_experiment_task(experiment_id, task_id)
            if task is None:
                raise RuntimeError("experiment task insert failed")
            return task
        except Exception:
            self.conn.rollback()
            raise

    def get_experiment_task(self, experiment_id: int, task_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT *
            FROM experiment_tasks
            WHERE experiment_id = ? AND id = ?
            """,
            (experiment_id, task_id),
        ).fetchone()

    def list_experiment_tasks(
        self,
        experiment_id: int,
        *,
        statuses: list[str] | tuple[str, ...] | None = None,
        limit: int = 20,
    ) -> list[sqlite3.Row]:
        clauses = ["experiment_id = ?"]
        params: list[Any] = [experiment_id]
        if statuses:
            normalized = [_validate_experiment_task_status(status) for status in statuses]
            placeholders = ", ".join("?" for _ in normalized)
            clauses.append(f"status IN ({placeholders})")
            params.extend(normalized)
        params.append(limit)
        return list(
            self.conn.execute(
                f"""
                SELECT *
                FROM experiment_tasks
                WHERE {" AND ".join(clauses)}
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                params,
            )
        )

    def lease_next_experiment_task(
        self,
        experiment_id: int,
        *,
        owner: str,
        lease_seconds: int = 300,
        now: int | None = None,
    ) -> sqlite3.Row | None:
        owner = owner.strip()
        if not owner:
            raise ValueError("experiment task lease owner is required")
        if lease_seconds < 1:
            raise ValueError("experiment task lease_seconds must be positive")
        ts = now_ts() if now is None else int(now)
        try:
            task = self.conn.execute(
                """
                UPDATE experiment_tasks
                SET status = 'leased', lease_owner = ?, lease_expires_at = ?,
                    attempts = attempts + 1, updated_at = ?,
                    started_at = COALESCE(started_at, ?)
                WHERE id = (
                    SELECT id FROM experiment_tasks
                    WHERE experiment_id = ? AND attempts < max_attempts
                      AND (status = 'ready' OR
                           (status = 'leased' AND COALESCE(lease_expires_at, 0) <= ?))
                    ORDER BY ready_at ASC, created_at ASC, id ASC LIMIT 1
                )
                RETURNING *
                """,
                (owner, ts + lease_seconds, ts, ts, experiment_id, ts),
            ).fetchone()
            self.conn.commit()
            return task
        except Exception:
            self.conn.rollback()
            raise

    def lease_experiment_task(
        self,
        experiment_id: int,
        task_id: int,
        *,
        owner: str,
        lease_seconds: int = 300,
        now: int | None = None,
    ) -> sqlite3.Row | None:
        owner = owner.strip()
        if not owner:
            raise ValueError("experiment task lease owner is required")
        if lease_seconds < 1:
            raise ValueError("experiment task lease_seconds must be positive")
        ts = now_ts() if now is None else int(now)
        try:
            task = self.conn.execute(
                """
                UPDATE experiment_tasks
                SET status = 'leased', lease_owner = ?, lease_expires_at = ?,
                    attempts = attempts + 1, updated_at = ?,
                    started_at = COALESCE(started_at, ?)
                WHERE experiment_id = ?
                  AND id = ?
                  AND attempts < max_attempts
                  AND (
                    status = 'ready'
                    OR (status = 'leased' AND COALESCE(lease_expires_at, 0) <= ?)
                  )
                RETURNING *
                """,
                (owner, ts + lease_seconds, ts, ts, experiment_id, task_id, ts),
            ).fetchone()
            self.conn.commit()
            return task
        except Exception:
            self.conn.rollback()
            raise

    def renew_experiment_task_lease(
        self, experiment_id: int, task_id: int, *, owner: str,
        lease_seconds: int = 900, now: int | None = None,
    ) -> bool:
        if not owner.strip() or lease_seconds < 1:
            raise ValueError("owner and positive lease duration are required")
        ts = now_ts() if now is None else int(now)
        cursor = self.conn.execute("""UPDATE experiment_tasks SET lease_expires_at=?,updated_at=?
            WHERE experiment_id=? AND id=? AND lease_owner=?
              AND status IN ('leased','running') AND lease_expires_at>?""",
            (ts + lease_seconds, ts, experiment_id, task_id, owner, ts))
        self.conn.commit()
        return bool(cursor.rowcount)

    def start_experiment_task_if_owned(
        self, experiment_id: int, task_id: int, *, owner: str,
        now: int | None = None,
    ) -> sqlite3.Row | None:
        """Start only the current, unexpired lease acquisition."""
        if not owner.strip():
            raise ValueError("experiment task lease owner is required")
        ts = now_ts() if now is None else int(now)
        try:
            task = self.conn.execute("""UPDATE experiment_tasks
                SET status='running', updated_at=?, started_at=COALESCE(started_at,?)
                WHERE experiment_id=? AND id=? AND lease_owner=?
                  AND status='leased' AND lease_expires_at>?
                RETURNING *""", (ts, ts, experiment_id, task_id, owner, ts)).fetchone()
            self.conn.commit()
            return task
        except Exception:
            self.conn.rollback()
            raise

    def complete_experiment_task_if_owned(
        self, experiment_id: int, task_id: int, status: str, *, owner: str,
        result: dict[str, Any] | None = None, error: str = "",
        now: int | None = None,
    ) -> sqlite3.Row | None:
        """Fence terminal writes against expired or reassigned leases."""
        status = _validate_experiment_task_status(status)
        if status not in EXPERIMENT_TASK_TERMINAL_STATUSES:
            raise ValueError("owned completion requires a terminal task status")
        if not owner.strip():
            raise ValueError("experiment task lease owner is required")
        ts = now_ts() if now is None else int(now)
        try:
            task = self.conn.execute("""UPDATE experiment_tasks
                SET status=?, updated_at=?, completed_at=COALESCE(completed_at,?),
                    result_json=COALESCE(?,result_json), error=?,
                    lease_owner='', lease_expires_at=NULL
                WHERE experiment_id=? AND id=? AND lease_owner=?
                  AND status IN ('leased','running') AND lease_expires_at>?
                RETURNING *""", (
                    status, ts, ts,
                    json.dumps(result, ensure_ascii=False, sort_keys=True) if result is not None else None,
                    error.strip(), experiment_id, task_id, owner, ts,
                )).fetchone()
            self.conn.commit()
            return task
        except Exception:
            self.conn.rollback()
            raise

    def update_experiment_task_status(
        self,
        experiment_id: int,
        task_id: int,
        status: str,
        *,
        result: dict[str, Any] | None = None,
        error: str = "",
        lease_owner: str | None = None,
    ) -> sqlite3.Row:
        status = _validate_experiment_task_status(status)
        ts = now_ts()
        terminal = status in EXPERIMENT_TASK_TERMINAL_STATUSES
        fields = ["status = ?", "updated_at = ?"]
        params: list[Any] = [status, ts]
        if result is not None:
            fields.append("result_json = ?")
            params.append(json.dumps(result, ensure_ascii=False, sort_keys=True))
        if error:
            fields.append("error = ?")
            params.append(error.strip())
        if lease_owner is not None:
            fields.append("lease_owner = ?")
            params.append(lease_owner.strip())
        if terminal:
            fields.extend(["completed_at = COALESCE(completed_at, ?)", "lease_owner = ''", "lease_expires_at = NULL"])
            params.append(ts)
        elif status == "ready":
            fields.extend(["ready_at = COALESCE(ready_at, ?)", "lease_owner = ''", "lease_expires_at = NULL"])
            params.append(ts)
        params.extend([experiment_id, task_id])
        self.conn.execute(
            f"""
            UPDATE experiment_tasks
            SET {', '.join(fields)}
            WHERE experiment_id = ? AND id = ?
            """,
            params,
        )
        self.conn.commit()
        task = self.get_experiment_task(experiment_id, task_id)
        if task is None:
            raise ValueError(f"unknown experiment task #{task_id}")
        return task

    def add_research_thread(
        self,
        project_id: int,
        *,
        name: str,
        thread_id: str,
        question: str = "",
        max_iterations: int | None = None,
        config: dict[str, Any] | None = None,
        status: str = "planned",
    ) -> sqlite3.Row:
        name = name.strip()
        if not name:
            raise ValueError("research name is required")
        thread_id = thread_id.strip()
        if not thread_id:
            raise ValueError("research thread_id is required")
        status = _validate_research_status(status)
        if max_iterations is not None and max_iterations < 1:
            raise ValueError("research max_iterations must be positive")
        ts = now_ts()
        started_at = ts if status == "running" else None
        completed_at = ts if status in {"completed", "failed"} else None
        self.conn.execute(
            """
            INSERT INTO research_threads (
                project_id, name, thread_id, question, status, max_iterations,
                phase, phase_message, config_json, created_at, updated_at,
                started_at, completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id, name) DO UPDATE SET
                thread_id = excluded.thread_id,
                question = excluded.question,
                status = excluded.status,
                max_iterations = excluded.max_iterations,
                phase = CASE
                    WHEN research_threads.phase = 'initialized' THEN excluded.phase
                    ELSE research_threads.phase
                END,
                phase_message = CASE
                    WHEN research_threads.phase_message = '' THEN excluded.phase_message
                    ELSE research_threads.phase_message
                END,
                config_json = excluded.config_json,
                updated_at = excluded.updated_at,
                started_at = COALESCE(research_threads.started_at, excluded.started_at),
                completed_at = excluded.completed_at
            """,
            (
                project_id,
                name,
                thread_id,
                question.strip(),
                status,
                max_iterations,
                _phase_for_status(status),
                "Research thread configured.",
                json.dumps(config or {}, ensure_ascii=False),
                ts,
                ts,
                started_at,
                completed_at,
            ),
        )
        self.conn.commit()
        research = self.get_research_thread(project_id, name)
        if research is None:
            raise RuntimeError("research insert failed")
        return research

    def get_research_thread(self, project_id: int, research: int | str) -> sqlite3.Row | None:
        if isinstance(research, int) or str(research).isdigit():
            where = "research_threads.id = ? AND research_threads.project_id = ?"
            params: tuple[Any, ...] = (int(research), project_id)
        else:
            where = "research_threads.name = ? AND research_threads.project_id = ?"
            params = (str(research), project_id)
        return self.conn.execute(
            f"""
            SELECT research_threads.*, projects.name AS project_name, projects.path AS project_path
            FROM research_threads
            JOIN projects ON projects.id = research_threads.project_id
            WHERE {where}
            """,
            params,
        ).fetchone()

    def list_research_threads(
        self,
        project_id: int,
        *,
        include_archived: bool = False,
    ) -> list[sqlite3.Row]:
        clauses = ["research_threads.project_id = ?"]
        params: list[Any] = [project_id]
        if not include_archived:
            clauses.append("research_threads.status != 'archived'")
        return list(
            self.conn.execute(
                f"""
                SELECT research_threads.*, projects.name AS project_name
                FROM research_threads
                JOIN projects ON projects.id = research_threads.project_id
                WHERE {" AND ".join(clauses)}
                ORDER BY
                    CASE research_threads.status
                        WHEN 'running' THEN 0
                        WHEN 'planned' THEN 1
                        WHEN 'paused' THEN 2
                        WHEN 'failed' THEN 3
                        WHEN 'completed' THEN 4
                        ELSE 5
                    END,
                    research_threads.updated_at DESC,
                    research_threads.id DESC
                """,
                params,
            )
        )

    def search_research_threads(
        self,
        project_id: int,
        query: str,
        *,
        limit: int = 10,
        include_archived: bool = False,
    ) -> list[dict[str, Any]]:
        tokens = _search_tokens(query)
        if not tokens:
            return []
        matches: list[dict[str, Any]] = []
        for research in self.list_research_threads(project_id, include_archived=include_archived):
            score, fields, snippets = self._score_research_match(research, tokens)
            if score <= 0:
                continue
            matches.append(
                {
                    "research": research,
                    "score": score,
                    "matched_fields": sorted(fields),
                    "snippets": snippets[:5],
                }
            )
        matches.sort(
            key=lambda item: (
                item["score"],
                int(item["research"]["updated_at"] or 0),
                int(item["research"]["id"]),
            ),
            reverse=True,
        )
        return matches[: max(1, int(limit or 1))]

    def _score_research_match(
        self,
        research: sqlite3.Row,
        tokens: Sequence[str],
    ) -> tuple[int, set[str], list[str]]:
        score = 0
        fields: set[str] = set()
        snippets: list[str] = []

        def add_match(label: str, text: str, weight: int) -> None:
            nonlocal score
            if not text:
                return
            lower = text.lower()
            hits = [token for token in tokens if token in lower]
            if not hits:
                return
            score += weight * len(hits)
            fields.add(label)
            if len(snippets) < 8:
                snippets.append(f"{label}: {_snippet(text, hits[0])}")

        add_match("research", " ".join(str(research[key] or "") for key in ("name", "question", "phase_message", "latest_decision")), 12)
        for row in self.list_research_sources(research["id"], limit=80):
            add_match("source", " ".join(str(row[key] or "") for key in ("title", "url", "role", "notes")), 7)
        for row in self.list_research_claims(research["id"], limit=80):
            add_match("claim", " ".join(str(row[key] or "") for key in ("claim", "support", "notes")), 8)
        for row in self.list_research_ideas(research["id"], limit=80):
            add_match(
                "idea",
                " ".join(
                    str(row[key] or "")
                    for key in (
                        "title",
                        "hypothesis",
                        "mechanism",
                        "minimal_test",
                        "kill_criteria",
                        "notes",
                    )
                ),
                10,
            )
        for row in self.list_research_tasks(research["id"], limit=60):
            add_match("task", " ".join(str(row[key] or "") for key in ("task_type", "title", "query")), 5)
        for row in self.list_research_events(research["id"], limit=60):
            add_match("event", str(row["message"] or ""), 3)
        return score, fields, snippets

    def update_research_status(self, research_id: int, status: str) -> None:
        status = _validate_research_status(status)
        ts = now_ts()
        started_at_expr = "COALESCE(started_at, ?)" if status == "running" else "started_at"
        completed_at_value = ts if status in {"completed", "failed"} else None
        self.conn.execute(
            f"""
            UPDATE research_threads
            SET status = ?,
                started_at = {started_at_expr},
                completed_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (status, ts, completed_at_value, ts, research_id)
            if status == "running"
            else (status, completed_at_value, ts, research_id),
        )
        self.conn.commit()

    def add_research_event(
        self,
        research_id: int,
        *,
        event_type: str,
        iteration: int | None = None,
        level: str = "info",
        message: str = "",
        payload: dict[str, Any] | None = None,
        source: str = "manual",
        run_id: int | None = None,
        status: str | None = None,
        phase: str | None = None,
        phase_message: str | None = None,
    ) -> int:
        event_type = event_type.strip()
        if not event_type:
            raise ValueError("research event_type is required")
        level = _validate_event_level(level)
        if iteration is not None and iteration < 0:
            raise ValueError("research iteration must be non-negative")
        ts = now_ts()
        cur = self.conn.execute(
            """
            INSERT INTO research_events (
                research_id, iteration, event_type, level, message,
                payload_json, source, run_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                research_id,
                iteration,
                event_type,
                level,
                message.strip(),
                json.dumps(payload or {}, ensure_ascii=False),
                source.strip() or "manual",
                run_id,
                ts,
            ),
        )
        fields = ["updated_at = ?"]
        params: list[Any] = [ts]
        if iteration is not None:
            fields.append("current_iteration = MAX(current_iteration, ?)")
            params.append(iteration)
        if status is not None:
            status = _validate_research_status(status)
            fields.append("status = ?")
            params.append(status)
            if phase is None and _research_phase_for_event(event_type, None) is None:
                phase = _phase_for_status(status)
            if status == "running":
                fields.append("started_at = COALESCE(started_at, ?)")
                params.append(ts)
            if status in {"completed", "failed"}:
                fields.append("completed_at = ?")
                params.append(ts)
        resolved_phase = _research_phase_for_event(event_type, phase)
        if resolved_phase is not None:
            fields.append("phase = ?")
            params.append(resolved_phase)
        resolved_message = phase_message or _research_phase_message_for_event(event_type, message)
        if resolved_message:
            fields.append("phase_message = ?")
            params.append(resolved_message)
        if event_type in {"decision", "blocked", "failed", "completed"} and message.strip():
            fields.append("latest_decision = ?")
            params.append(message.strip())
        params.append(research_id)
        self.conn.execute(
            f"UPDATE research_threads SET {', '.join(fields)} WHERE id = ?",
            params,
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def list_research_events(
        self,
        research_id: int,
        *,
        limit: int = 20,
    ) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT research_events.*, runs.status AS run_status
                FROM research_events
                LEFT JOIN runs ON runs.id = research_events.run_id
                WHERE research_events.research_id = ?
                ORDER BY research_events.created_at DESC, research_events.id DESC
                LIMIT ?
                """,
                (research_id, limit),
            )
        )

    def upsert_research_source(
        self,
        research_id: int,
        *,
        title: str,
        url: str = "",
        source_type: str = "",
        role: str = "",
        depth: str = "title",
        status: str = "candidate",
        relevance: int = 0,
        evidence_quality: int = 0,
        notes: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> int:
        title = title.strip()
        url = url.strip()
        if not title:
            title = url
        if not title:
            raise ValueError("research source title or url is required")
        if not url:
            url = f"manual:{title}"
        status = _validate_research_source_status(status)
        ts = now_ts()
        cur = self.conn.execute(
            """
            INSERT INTO research_sources (
                research_id, title, url, source_type, role, depth, status,
                relevance, evidence_quality, notes, metadata_json,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(research_id, url) DO UPDATE SET
                title = COALESCE(NULLIF(excluded.title, ''), research_sources.title),
                source_type = excluded.source_type,
                role = excluded.role,
                depth = excluded.depth,
                status = excluded.status,
                relevance = excluded.relevance,
                evidence_quality = excluded.evidence_quality,
                notes = excluded.notes,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                research_id,
                title,
                url,
                source_type.strip(),
                role.strip(),
                depth.strip() or "title",
                status,
                int(relevance or 0),
                int(evidence_quality or 0),
                notes.strip(),
                json.dumps(metadata or {}, ensure_ascii=False),
                ts,
                ts,
            ),
        )
        self.conn.commit()
        row = self.conn.execute(
            "SELECT id FROM research_sources WHERE research_id = ? AND url = ?",
            (research_id, url),
        ).fetchone()
        return int(row["id"])

    def list_research_sources(
        self,
        research_id: int,
        *,
        limit: int = 20,
    ) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT *
                FROM research_sources
                WHERE research_id = ?
                ORDER BY
                    CASE status
                        WHEN 'kept' THEN 0
                        WHEN 'candidate' THEN 1
                        WHEN 'rejected' THEN 2
                        ELSE 3
                    END,
                    relevance DESC,
                    evidence_quality DESC,
                    updated_at DESC,
                    id DESC
                LIMIT ?
                """,
                (research_id, limit),
            )
        )

    def get_research_source(self, research_id: int, source_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT *
            FROM research_sources
            WHERE research_id = ? AND id = ?
            """,
            (research_id, source_id),
        ).fetchone()

    def update_research_source(
        self,
        source_id: int,
        *,
        status: str | None = None,
        depth: str | None = None,
        relevance: int | None = None,
        evidence_quality: int | None = None,
        notes: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        fields = ["updated_at = ?"]
        params: list[Any] = [now_ts()]
        if status is not None:
            fields.append("status = ?")
            params.append(_validate_research_source_status(status))
        if depth is not None:
            fields.append("depth = ?")
            params.append(depth.strip() or "title")
        if relevance is not None:
            fields.append("relevance = ?")
            params.append(int(relevance or 0))
        if evidence_quality is not None:
            fields.append("evidence_quality = ?")
            params.append(int(evidence_quality or 0))
        if notes is not None:
            fields.append("notes = ?")
            params.append(notes.strip())
        if metadata is not None:
            fields.append("metadata_json = ?")
            params.append(json.dumps(metadata, ensure_ascii=False))
        params.append(source_id)
        self.conn.execute(
            f"UPDATE research_sources SET {', '.join(fields)} WHERE id = ?",
            params,
        )
        self.conn.commit()

    def add_research_claim(
        self,
        research_id: int,
        *,
        claim: str,
        status: str = "uncertain",
        support: str = "",
        source_id: int | None = None,
        confidence: int = 0,
        notes: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> int:
        claim = claim.strip()
        if not claim:
            raise ValueError("research claim is required")
        status = _validate_research_claim_status(status)
        metadata = dict(metadata or {})
        metadata.setdefault("verification_status", "legacy_unverified" if status in {"supported", "contradicted"} else "unverified")
        ts = now_ts()
        cur = self.conn.execute(
            """
            INSERT INTO research_claims (
                research_id, source_id, claim, status, support, confidence,
                notes, metadata_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                research_id,
                source_id,
                claim,
                status,
                support.strip(),
                int(confidence or 0),
                notes.strip(),
                json.dumps(metadata or {}, ensure_ascii=False),
                ts,
                ts,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def list_research_claims(self, research_id: int, *, limit: int = 20) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT research_claims.*, research_sources.title AS source_title
                FROM research_claims
                LEFT JOIN research_sources ON research_sources.id = research_claims.source_id
                WHERE research_claims.research_id = ?
                ORDER BY confidence DESC, updated_at DESC, id DESC
                LIMIT ?
                """,
                (research_id, limit),
            )
        )

    def add_research_idea(
        self,
        research_id: int,
        *,
        title: str,
        hypothesis: str = "",
        mechanism: str = "",
        status: str = "candidate",
        score: dict[str, Any] | None = None,
        minimal_test: str = "",
        kill_criteria: str = "",
        notes: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> int:
        title = title.strip()
        if not title:
            raise ValueError("research idea title is required")
        status = _validate_research_idea_status(status)
        ts = now_ts()
        cur = self.conn.execute(
            """
            INSERT INTO research_ideas (
                research_id, title, hypothesis, mechanism, status, score_json,
                minimal_test, kill_criteria, notes, metadata_json,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                research_id,
                title,
                hypothesis.strip(),
                mechanism.strip(),
                status,
                json.dumps(score or {}, ensure_ascii=False),
                minimal_test.strip(),
                kill_criteria.strip(),
                notes.strip(),
                json.dumps(metadata or {}, ensure_ascii=False),
                ts,
                ts,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def list_research_ideas(self, research_id: int, *, limit: int = 20) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT *
                FROM research_ideas
                WHERE research_id = ?
                ORDER BY
                    CASE status
                        WHEN 'selected' THEN 0
                        WHEN 'active' THEN 1
                        WHEN 'experiment_ready' THEN 2
                        WHEN 'candidate' THEN 3
                        WHEN 'rejected' THEN 4
                        ELSE 5
                    END,
                    updated_at DESC,
                    id DESC
                LIMIT ?
                """,
                (research_id, limit),
            )
        )

    def get_research_idea(self, research_id: int, idea_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT *
            FROM research_ideas
            WHERE research_id = ? AND id = ?
            """,
            (research_id, idea_id),
        ).fetchone()

    def update_research_idea(
        self,
        idea_id: int,
        *,
        title: str | None = None,
        hypothesis: str | None = None,
        mechanism: str | None = None,
        status: str | None = None,
        score: dict[str, Any] | None = None,
        minimal_test: str | None = None,
        kill_criteria: str | None = None,
        notes: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        fields = ["updated_at = ?"]
        params: list[Any] = [now_ts()]
        if title is not None:
            title = title.strip()
            if not title:
                raise ValueError("research idea title is required")
            fields.append("title = ?")
            params.append(title)
        if hypothesis is not None:
            fields.append("hypothesis = ?")
            params.append(hypothesis.strip())
        if mechanism is not None:
            fields.append("mechanism = ?")
            params.append(mechanism.strip())
        if status is not None:
            fields.append("status = ?")
            params.append(_validate_research_idea_status(status))
        if score is not None:
            fields.append("score_json = ?")
            params.append(json.dumps(score, ensure_ascii=False))
        if minimal_test is not None:
            fields.append("minimal_test = ?")
            params.append(minimal_test.strip())
        if kill_criteria is not None:
            fields.append("kill_criteria = ?")
            params.append(kill_criteria.strip())
        if notes is not None:
            fields.append("notes = ?")
            params.append(notes.strip())
        if metadata is not None:
            fields.append("metadata_json = ?")
            params.append(json.dumps(metadata, ensure_ascii=False))
        params.append(idea_id)
        self.conn.execute(
            f"UPDATE research_ideas SET {', '.join(fields)} WHERE id = ?",
            params,
        )
        self.conn.commit()

    def add_research_task(
        self,
        research_id: int,
        *,
        task_type: str,
        title: str = "",
        query: str = "",
        status: str = "queued",
        source_id: int | None = None,
        idea_id: int | None = None,
        payload: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
    ) -> int:
        task_type = task_type.strip()
        if not task_type:
            raise ValueError("research task_type is required")
        status = _validate_research_task_status(status)
        ts = now_ts()
        cur = self.conn.execute(
            """
            INSERT INTO research_tasks (
                research_id, task_type, status, title, query, source_id,
                idea_id, payload_json, result_json, created_at, updated_at,
                completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                research_id,
                task_type,
                status,
                title.strip(),
                query.strip(),
                source_id,
                idea_id,
                json.dumps(payload or {}, ensure_ascii=False),
                json.dumps(result or {}, ensure_ascii=False),
                ts,
                ts,
                ts if status in {"done", "blocked", "failed"} else None,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def list_research_tasks(
        self,
        research_id: int,
        *,
        limit: int = 20,
    ) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT research_tasks.*, research_sources.title AS source_title,
                       research_ideas.title AS idea_title
                FROM research_tasks
                LEFT JOIN research_sources ON research_sources.id = research_tasks.source_id
                LEFT JOIN research_ideas ON research_ideas.id = research_tasks.idea_id
                WHERE research_tasks.research_id = ?
                ORDER BY
                    CASE research_tasks.status
                        WHEN 'running' THEN 0
                        WHEN 'queued' THEN 1
                        WHEN 'blocked' THEN 2
                        WHEN 'failed' THEN 3
                        WHEN 'done' THEN 4
                        ELSE 5
                    END,
                    research_tasks.updated_at DESC,
                    research_tasks.id DESC
                LIMIT ?
                """,
                (research_id, limit),
            )
        )

    def get_research_task(self, research_id: int, task_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT research_tasks.*, research_sources.title AS source_title,
                   research_ideas.title AS idea_title
            FROM research_tasks
            LEFT JOIN research_sources ON research_sources.id = research_tasks.source_id
            LEFT JOIN research_ideas ON research_ideas.id = research_tasks.idea_id
            WHERE research_tasks.research_id = ? AND research_tasks.id = ?
            """,
            (research_id, task_id),
        ).fetchone()

    def next_research_task(
        self,
        research_id: int,
        *,
        status: str = "queued",
    ) -> sqlite3.Row | None:
        status = _validate_research_task_status(status)
        return self.conn.execute(
            """
            SELECT research_tasks.*, research_sources.title AS source_title,
                   research_ideas.title AS idea_title
            FROM research_tasks
            LEFT JOIN research_sources ON research_sources.id = research_tasks.source_id
            LEFT JOIN research_ideas ON research_ideas.id = research_tasks.idea_id
            WHERE research_tasks.research_id = ? AND research_tasks.status = ?
            ORDER BY research_tasks.created_at ASC, research_tasks.id ASC
            LIMIT 1
            """,
            (research_id, status),
        ).fetchone()

    def update_research_task(
        self,
        task_id: int,
        *,
        status: str | None = None,
        title: str | None = None,
        query: str | None = None,
        payload: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
    ) -> None:
        fields = ["updated_at = ?"]
        params: list[Any] = [now_ts()]
        completed_at: int | None = None
        if status is not None:
            status = _validate_research_task_status(status)
            fields.append("status = ?")
            params.append(status)
            if status in {"done", "blocked", "failed", "archived"}:
                completed_at = now_ts()
                fields.append("completed_at = ?")
                params.append(completed_at)
            elif status == "running":
                fields.append("completed_at = NULL")
        if title is not None:
            fields.append("title = ?")
            params.append(title.strip())
        if query is not None:
            fields.append("query = ?")
            params.append(query.strip())
        if payload is not None:
            fields.append("payload_json = ?")
            params.append(json.dumps(payload, ensure_ascii=False))
        if result is not None:
            fields.append("result_json = ?")
            params.append(json.dumps(result, ensure_ascii=False))
        params.append(task_id)
        self.conn.execute(
            f"UPDATE research_tasks SET {', '.join(fields)} WHERE id = ?",
            params,
        )
        self.conn.commit()

    def add_todo(
        self,
        project_id: int,
        *,
        title: str,
        body: str = "",
        priority: int = 2,
        status: str = "open",
        source_run_id: int | None = None,
        due_at: int | None = None,
    ) -> int:
        title = title.strip()
        if not title:
            raise ValueError("todo title is required")
        status = _validate_todo_status(status)
        priority = _validate_priority(priority)
        ts = now_ts()
        cur = self.conn.execute(
            """
            INSERT INTO todos (
                project_id, title, body, status, priority, source_run_id,
                due_at, created_at, updated_at, completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project_id,
                title,
                body.strip(),
                status,
                priority,
                source_run_id,
                due_at,
                ts,
                ts,
                ts if status == "done" else None,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def get_todo(self, todo_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT todos.*, projects.name AS project_name
            FROM todos
            JOIN projects ON projects.id = todos.project_id
            WHERE todos.id = ?
            """,
            (todo_id,),
        ).fetchone()

    def list_todos(
        self,
        project_id: int,
        *,
        statuses: Sequence[str] | None = None,
        include_archived: bool = False,
        limit: int | None = None,
    ) -> list[sqlite3.Row]:
        clauses = ["project_id = ?"]
        params: list[Any] = [project_id]
        if statuses:
            for status in statuses:
                _validate_todo_status(status)
            placeholders = ",".join("?" for _ in statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(statuses)
        elif not include_archived:
            clauses.append("status != 'archived'")
        sql = f"""
            SELECT *
            FROM todos
            WHERE {" AND ".join(clauses)}
            ORDER BY
                CASE status
                    WHEN 'doing' THEN 0
                    WHEN 'blocked' THEN 1
                    WHEN 'open' THEN 2
                    WHEN 'done' THEN 3
                    ELSE 4
                END,
                priority ASC,
                updated_at DESC,
                id DESC
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return list(self.conn.execute(sql, params))

    def update_todo_status(self, todo_id: int, status: str) -> None:
        status = _validate_todo_status(status)
        completed_at = now_ts() if status == "done" else None
        self.conn.execute(
            """
            UPDATE todos
            SET status = ?, completed_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, completed_at, now_ts(), todo_id),
        )
        self.conn.commit()

    def update_todo(
        self,
        todo_id: int,
        *,
        title: str | None = None,
        body: str | None = None,
        priority: int | None = None,
        due_at: int | None = None,
    ) -> None:
        fields: list[str] = []
        params: list[Any] = []
        if title is not None:
            title = title.strip()
            if not title:
                raise ValueError("todo title is required")
            fields.append("title = ?")
            params.append(title)
        if body is not None:
            fields.append("body = ?")
            params.append(body.strip())
        if priority is not None:
            fields.append("priority = ?")
            params.append(_validate_priority(priority))
        if due_at is not None:
            fields.append("due_at = ?")
            params.append(due_at)
        if not fields:
            return
        fields.append("updated_at = ?")
        params.append(now_ts())
        params.append(todo_id)
        self.conn.execute(
            f"UPDATE todos SET {', '.join(fields)} WHERE id = ?",
            params,
        )
        self.conn.commit()

    def add_rule(
        self,
        project_id: int,
        *,
        name: str,
        kind: str,
        thread_id: str | None = None,
        time_of_day: str | None = None,
        interval_seconds: int | None = None,
        prompt: str = "",
        enabled: bool = True,
    ) -> None:
        ts = now_ts()
        self.conn.execute(
            """
            INSERT INTO rules (
                project_id, name, kind, thread_id, enabled, time_of_day,
                interval_seconds, prompt, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id, name) DO UPDATE SET
                kind = excluded.kind,
                thread_id = excluded.thread_id,
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
                thread_id,
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

    def allow_telegram_user(self, user_id: int | str, username: str = "") -> None:
        ts = now_ts()
        self.conn.execute(
            """
            INSERT INTO telegram_users (user_id, username, allowed, created_at, updated_at)
            VALUES (?, ?, 1, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                allowed = 1,
                updated_at = excluded.updated_at
            """,
            (str(user_id), username or "", ts, ts),
        )
        self.conn.commit()

    def deny_telegram_user(self, user_id: int | str) -> None:
        self.conn.execute(
            "UPDATE telegram_users SET allowed = 0, updated_at = ? WHERE user_id = ?",
            (now_ts(), str(user_id)),
        )
        self.conn.commit()

    def get_telegram_user(self, user_id: int | str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM telegram_users WHERE user_id = ?",
            (str(user_id),),
        ).fetchone()

    def telegram_user_allowed(self, user_id: int | str) -> bool:
        user = self.get_telegram_user(user_id)
        return bool(user and user["allowed"])

    def list_telegram_users(self) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM telegram_users ORDER BY allowed DESC, user_id"
            )
        )

    def set_telegram_binding(
        self,
        *,
        chat_id: int | str,
        message_thread_id: int | str | None,
        project_id: int,
        thread_id: str | None,
    ) -> None:
        ts = now_ts()
        self.conn.execute(
            """
            INSERT INTO telegram_bindings (
                chat_id, message_thread_id, project_id, thread_id, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, message_thread_id) DO UPDATE SET
                project_id = excluded.project_id,
                thread_id = excluded.thread_id,
                updated_at = excluded.updated_at
            """,
            (
                str(chat_id),
                _telegram_thread_key(message_thread_id),
                project_id,
                thread_id,
                ts,
                ts,
            ),
        )
        self.conn.commit()

    def get_telegram_binding(
        self,
        *,
        chat_id: int | str,
        message_thread_id: int | str | None,
    ) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT telegram_bindings.*, projects.name AS project_name, projects.path AS project_path
            FROM telegram_bindings
            JOIN projects ON projects.id = telegram_bindings.project_id
            WHERE chat_id = ? AND message_thread_id = ?
            """,
            (str(chat_id), _telegram_thread_key(message_thread_id)),
        ).fetchone()

    def list_telegram_bindings(self) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT telegram_bindings.*, projects.name AS project_name, projects.path AS project_path
                FROM telegram_bindings
                JOIN projects ON projects.id = telegram_bindings.project_id
                ORDER BY chat_id, message_thread_id
                """
            )
        )

    def remove_telegram_binding(
        self,
        *,
        chat_id: int | str,
        message_thread_id: int | str | None,
    ) -> bool:
        cur = self.conn.execute(
            """
            DELETE FROM telegram_bindings
            WHERE chat_id = ? AND message_thread_id = ?
            """,
            (str(chat_id), _telegram_thread_key(message_thread_id)),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def upsert_token_usage_events(self, events: Sequence[dict[str, Any]]) -> None:
        if not events:
            return
        seen_at = now_ts()
        rows = []
        for event in events:
            event_key = event.get("event_key") or event.get("response_id") or f"log:{event['log_id']}"
            rows.append(
                (
                    event_key,
                    event.get("response_id") or "",
                    int(event.get("log_id") or 0),
                    int(event["ts"]),
                    event.get("thread_id") or "",
                    event.get("cwd") or "",
                    event.get("process_uuid") or "",
                    event.get("project_id"),
                    event.get("project") or "",
                    event.get("model") or "unknown",
                    int(event.get("input_tokens") or 0),
                    int(event.get("output_tokens") or 0),
                    int(event.get("total_tokens") or 0),
                    int(event.get("cached_tokens") or 0),
                    int(event.get("reasoning_tokens") or 0),
                    seen_at,
                    seen_at,
                )
            )
        self.conn.execute("PRAGMA busy_timeout = 1000")
        try:
            self.conn.executemany(
                """
                INSERT INTO token_usage_events (
                    event_key, response_id, log_id, ts, thread_id, cwd, process_uuid,
                    project_id, project_name, model, input_tokens, output_tokens,
                    total_tokens, cached_tokens, reasoning_tokens, first_seen_at,
                    last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_key) DO UPDATE SET
                    response_id = COALESCE(NULLIF(excluded.response_id, ''), token_usage_events.response_id),
                    log_id = MAX(token_usage_events.log_id, excluded.log_id),
                    ts = excluded.ts,
                    thread_id = COALESCE(NULLIF(excluded.thread_id, ''), token_usage_events.thread_id),
                    cwd = COALESCE(NULLIF(excluded.cwd, ''), token_usage_events.cwd),
                    process_uuid = COALESCE(NULLIF(excluded.process_uuid, ''), token_usage_events.process_uuid),
                    project_id = COALESCE(excluded.project_id, token_usage_events.project_id),
                    project_name = COALESCE(NULLIF(excluded.project_name, ''), token_usage_events.project_name),
                    model = COALESCE(NULLIF(excluded.model, ''), token_usage_events.model),
                    input_tokens = excluded.input_tokens,
                    output_tokens = excluded.output_tokens,
                    total_tokens = excluded.total_tokens,
                    cached_tokens = excluded.cached_tokens,
                    reasoning_tokens = excluded.reasoning_tokens,
                    last_seen_at = excluded.last_seen_at
                """,
                rows,
            )
            self.conn.commit()
        except sqlite3.OperationalError:
            self.conn.rollback()
            raise
        finally:
            self.conn.execute("PRAGMA busy_timeout = 30000")

    def token_usage_event_keys(self, event_keys: Sequence[str]) -> set[str]:
        if not event_keys:
            return set()
        known: set[str] = set()
        for chunk in _chunks(list(event_keys), 700):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""
                SELECT event_key
                FROM token_usage_events
                WHERE event_key IN ({placeholders})
                """,
                chunk,
            )
            known.update(row["event_key"] for row in rows)
        return known

    def list_token_usage_events(self, *, start_ts: int, limit: int = 20000) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT *
                FROM token_usage_events
                WHERE ts >= ?
                ORDER BY ts DESC, log_id DESC
                LIMIT ?
                """,
                (start_ts, limit),
            )
        )


TODO_STATUSES = {"open", "doing", "blocked", "done", "archived"}
EXPERIMENT_STATUSES = {"planned", "running", "paused", "completed", "failed", "archived"}
EXPERIMENT_TASK_STATUSES = {
    "proposed",
    "ready",
    "leased",
    "running",
    "succeeded",
    "failed",
    "blocked",
    "cancelled",
}
EXPERIMENT_TASK_TERMINAL_STATUSES = {"succeeded", "failed", "blocked", "cancelled"}
RESEARCH_STATUSES = {"planned", "running", "paused", "completed", "failed", "archived"}
RESEARCH_SOURCE_STATUSES = {"candidate", "kept", "rejected", "stale"}
RESEARCH_CLAIM_STATUSES = {"supported", "contradicted", "uncertain"}
RESEARCH_IDEA_STATUSES = {"candidate", "active", "selected", "rejected", "experiment_ready"}
RESEARCH_TASK_STATUSES = {"queued", "running", "done", "blocked", "failed", "archived"}
EVENT_LEVELS = {"debug", "info", "warning", "error"}


def _validate_todo_status(status: str) -> str:
    if status not in TODO_STATUSES:
        raise ValueError(f"invalid todo status {status!r}; choose one of {sorted(TODO_STATUSES)}")
    return status


def _validate_experiment_status(status: str) -> str:
    if status not in EXPERIMENT_STATUSES:
        raise ValueError(
            f"invalid experiment status {status!r}; choose one of {sorted(EXPERIMENT_STATUSES)}"
        )
    return status


def _validate_experiment_task_status(status: str) -> str:
    if status not in EXPERIMENT_TASK_STATUSES:
        raise ValueError(
            f"invalid experiment task status {status!r}; choose one of {sorted(EXPERIMENT_TASK_STATUSES)}"
        )
    return status


def _validate_research_status(status: str) -> str:
    if status not in RESEARCH_STATUSES:
        raise ValueError(
            f"invalid research status {status!r}; choose one of {sorted(RESEARCH_STATUSES)}"
        )
    return status


def _validate_research_source_status(status: str) -> str:
    if status not in RESEARCH_SOURCE_STATUSES:
        raise ValueError(
            f"invalid research source status {status!r}; choose one of {sorted(RESEARCH_SOURCE_STATUSES)}"
        )
    return status


def _validate_research_claim_status(status: str) -> str:
    if status not in RESEARCH_CLAIM_STATUSES:
        raise ValueError(
            f"invalid research claim status {status!r}; choose one of {sorted(RESEARCH_CLAIM_STATUSES)}"
        )
    return status


def _validate_research_idea_status(status: str) -> str:
    if status not in RESEARCH_IDEA_STATUSES:
        raise ValueError(
            f"invalid research idea status {status!r}; choose one of {sorted(RESEARCH_IDEA_STATUSES)}"
        )
    return status


def _validate_research_task_status(status: str) -> str:
    if status not in RESEARCH_TASK_STATUSES:
        raise ValueError(
            f"invalid research task status {status!r}; choose one of {sorted(RESEARCH_TASK_STATUSES)}"
        )
    return status


def _validate_event_level(level: str) -> str:
    if level not in EVENT_LEVELS:
        raise ValueError(f"invalid event level {level!r}; choose one of {sorted(EVENT_LEVELS)}")
    return level


def _phase_for_status(status: str) -> str:
    return {
        "planned": "waiting_for_plan",
        "running": "running",
        "paused": "paused",
        "completed": "completed",
        "failed": "failed",
        "archived": "archived",
    }.get(status, "initialized")


def _phase_for_event(event_type: str, phase: str | None) -> str | None:
    if phase:
        return phase
    return {
        "created": "waiting_for_plan",
        "objective_updated": "waiting_for_plan",
        "remote_probe": "remote_ready",
        "control_turn_started": "analyzing",
        "agent_message_delta": "analyzing",
        "control_turn_completed": "waiting_for_decision",
        "git_checked": "checking_git",
        "code_change_started": "modifying_code",
        "code_committed": "ready_to_push",
        "code_push_started": "pushing_code",
        "code_pushed": "waiting_remote_pull",
        "remote_launch_dry_run": "waiting_for_experiment_start",
        "remote_pull_started": "syncing_remote",
        "remote_pull_completed": "remote_ready",
        "remote_job_started": "experiment_running",
        "remote_job_status": "experiment_running",
        "wandb_poll_started": "monitoring_wandb",
        "wandb_run_finished": "waiting_for_decision",
        "metric": "monitoring_wandb",
        "artifact": "monitoring_wandb",
        "decision": "waiting_for_decision",
        "blocked": "blocked",
        "failed": "failed",
        "completed": "completed",
    }.get(event_type)


def _phase_message_for_event(event_type: str, message: str) -> str:
    if message.strip():
        return message.strip()
    return {
        "control_turn_started": "Analyzing previous results and planning the next iteration.",
        "control_turn_completed": "Control analysis completed; waiting for decision or next action.",
        "git_checked": "Checking local git state.",
        "code_change_started": "Applying local code changes.",
        "code_committed": "Local code is committed.",
        "code_push_started": "Pushing local code to GitHub.",
        "code_pushed": "Local changes pushed; waiting for remote pull.",
        "remote_launch_dry_run": "Remote launch command prepared.",
        "remote_pull_started": "Remote server is syncing code from GitHub.",
        "remote_pull_completed": "Remote code is synced.",
        "remote_job_started": "Remote experiment is running.",
        "remote_job_status": "Remote experiment status checked.",
        "wandb_poll_started": "Polling W&B for experiment metrics.",
        "wandb_run_finished": "W&B run finished; waiting for next decision.",
        "metric": "W&B metrics updated.",
        "decision": "Waiting for the next decision.",
    }.get(event_type, "")


def _research_phase_for_event(event_type: str, phase: str | None) -> str | None:
    if phase:
        return phase
    return {
        "created": "scan",
        "control_turn_started": "analyzing",
        "agent_message_delta": "analyzing",
        "control_turn_completed": "waiting_for_decision",
        "contract_parsed": "updating_ledgers",
        "supervisor_control_started": "analyzing",
        "supervisor_idle": "idle",
        "task_started": "running_task",
        "task_completed": "waiting_for_decision",
        "task_failed": "blocked",
        "source_added": "triage",
        "claim_added": "synthesis",
        "idea_added": "idea_tournament",
        "task_queued": "queued",
        "decision": "waiting_for_decision",
        "blocked": "blocked",
        "failed": "failed",
        "completed": "completed",
    }.get(event_type)


def _research_phase_message_for_event(event_type: str, message: str) -> str:
    if message.strip():
        return message.strip()
    return {
        "created": "Research thread configured.",
        "control_turn_started": "Research control turn started.",
        "control_turn_completed": "Research control turn completed; ledgers are ready for review.",
        "contract_parsed": "Research contract parsed and applied.",
        "supervisor_control_started": "Research supervisor is triggering the control session.",
        "supervisor_idle": "Research supervisor is idle.",
        "task_started": "Research task is running.",
        "task_completed": "Research task completed.",
        "task_failed": "Research task failed.",
        "source_added": "Source ledger updated.",
        "claim_added": "Claim ledger updated.",
        "idea_added": "Idea ledger updated.",
        "task_queued": "Next research task queued.",
        "decision": "Waiting for the next research decision.",
    }.get(event_type, "")


def _artifact_ref_for_event(event_type: str, payload: dict[str, Any]) -> str:
    if event_type not in {"artifact", "remote_job_started"}:
        return ""
    for key in ("path", "url", "wandb_url", "checkpoint", "log_path"):
        value = payload.get(key)
        if value:
            return str(value)
    return ""


def _search_tokens(query: str) -> list[str]:
    tokens = [
        token.lower()
        for token in re.split(r"[\s,;，。:：/\\|()\[\]{}<>\"'`]+", query.strip())
        if token.strip()
    ]
    deduped: list[str] = []
    for token in tokens:
        if token not in deduped:
            deduped.append(token)
    return deduped[:12]


def _snippet(text: str, token: str, *, width: int = 96) -> str:
    clean = " ".join(str(text or "").split())
    lower = clean.lower()
    index = lower.find(token.lower())
    if index < 0:
        return clean[:width]
    start = max(0, index - width // 3)
    end = min(len(clean), start + width)
    snippet = clean[start:end]
    if start > 0:
        snippet = "..." + snippet
    if end < len(clean):
        snippet += "..."
    return snippet


def _validate_priority(priority: int) -> int:
    if priority not in {1, 2, 3}:
        raise ValueError("todo priority must be 1, 2, or 3")
    return priority


def _telegram_thread_key(message_thread_id: int | str | None) -> str:
    if message_thread_id is None:
        return ""
    return str(message_thread_id)


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]
