"""Durable local jobs. A lost running lease is ambiguous, never an automatic retry."""
from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .db import Store

JOB_KINDS = {"prompt", "daily_summary", "sync_sessions", "research_task", "experiment_task", "review_synthesis", "insight_refresh"}
TERMINAL = {"succeeded", "failed", "cancelled", "blocked"}


def initialize_job_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
            kind TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
            payload_json TEXT NOT NULL DEFAULT '{}', request_json TEXT, resource_key TEXT NOT NULL,
            access_mode TEXT NOT NULL DEFAULT 'write',
            idempotency_key TEXT, source TEXT NOT NULL,
            lease_owner TEXT, lease_expires_at INTEGER, cancel_requested_at INTEGER,
            attempts INTEGER NOT NULL DEFAULT 0, result_json TEXT NOT NULL DEFAULT '{}',
            error TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL, started_at INTEGER, completed_at INTEGER,
            UNIQUE(project_id, idempotency_key)
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(status, created_at, id);
        CREATE INDEX IF NOT EXISTS idx_jobs_resource ON jobs(resource_key, status);
        CREATE TABLE IF NOT EXISTS job_dependencies (
            job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            depends_on INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            PRIMARY KEY(job_id, depends_on), CHECK(job_id != depends_on)
        );
        CREATE TABLE IF NOT EXISTS job_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            event_type TEXT NOT NULL, payload_json TEXT NOT NULL DEFAULT '{}',
            created_at INTEGER NOT NULL
        );
    """)
    if "request_json" not in {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}:
        conn.execute("ALTER TABLE jobs ADD COLUMN request_json TEXT")
    if "access_mode" not in {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}:
        conn.execute("ALTER TABLE jobs ADD COLUMN access_mode TEXT NOT NULL DEFAULT 'write'")
    columns = {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}
    for name, definition in (("pause_requested_at", "INTEGER"), ("revision", "INTEGER NOT NULL DEFAULT 0"),
                             ("checkpoint_json", "TEXT NOT NULL DEFAULT '{}'")):
        if name not in columns:
            conn.execute(f"ALTER TABLE jobs ADD COLUMN {name} {definition}")
    conn.commit()


def decode_job(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    result = dict(row)
    for key in ("payload", "result", "checkpoint"):
        result[key] = json.loads(result.pop(key + "_json"))
    return result


class JobQueue:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.conn = store.conn

    def submit(self, project_name: str, kind: str, payload: dict[str, Any] | None = None,
               *, source: str = "cli", idempotency_key: str | None = None,
               depends_on: list[int] | None = None) -> dict[str, Any]:
        if kind not in JOB_KINDS:
            raise ValueError(f"unsupported job kind: {kind}")
        project = self.store.get_project(project_name)
        if project is None:
            raise ValueError(f"unknown project: {project_name}")
        if payload is not None and not isinstance(payload, dict):
            raise ValueError("payload must be an object")
        payload = dict(payload or {})
        if not isinstance(depends_on or [], list) or any(type(item) is not int or item <= 0 for item in depends_on or []):
            raise ValueError("dependency ids must be positive integers")
        dependencies = sorted(set(depends_on or []))
        request_json = json.dumps({"payload": payload, "depends_on": dependencies}, sort_keys=True)
        common = {"timeout", "sandbox", "approval_policy", "model", "reasoning_effort", "thread_id",
                  "new_thread", "max_total_tokens", "update_default_thread", "notification"}
        specific = {"prompt": {"prompt", "kind"}, "daily_summary": set(), "sync_sessions": set(),
                    "research_task": {"name", "task_id"}, "experiment_task": {"name"},
                    "review_synthesis": {"prompt", "review_job_ids", "kind"},
                    "insight_refresh": {"project_names", "use_model", "force"}}
        if set(payload) - common - specific[kind]:
            raise ValueError("unknown payload fields: " + ", ".join(sorted(set(payload) - common - specific[kind])))
        if any(type(payload[key]) is not bool for key in ("new_thread", "update_default_thread") if key in payload):
            raise ValueError("thread options must be booleans")
        for key in ("prompt", "name", "thread_id", "model", "reasoning_effort", "kind"):
            if key in payload and payload[key] is not None and not isinstance(payload[key], str):
                raise ValueError(f"{key} must be a string")
        if "task_id" in payload and (type(payload["task_id"]) is not int or payload["task_id"] <= 0):
            raise ValueError("task_id must be a positive integer")
        if kind in {"prompt", "review_synthesis"} and not str(payload.get("prompt") or "").strip():
            raise ValueError("prompt is required")
        if kind == "review_synthesis":
            if (not isinstance(payload.get("review_job_ids"), list) or not payload["review_job_ids"]
                    or any(type(value) is not int for value in payload["review_job_ids"])
                    or set(payload["review_job_ids"]) != set(dependencies)):
                raise ValueError("synthesis review ids must match dependency ids")
            payload.update(sandbox="read-only", approval_policy="never", new_thread=True, update_default_thread=False)
        if kind == "insight_refresh":
            from .insight_tasks import validate_insight_scope
            payload['project_names'] = validate_insight_scope(self.store, payload.get('project_names'))
            if project_name not in payload['project_names']:
                raise ValueError('insight job owner must be included in its selected project scope')
            for key in ('use_model', 'force'):
                if key in payload and type(payload[key]) is not bool:
                    raise ValueError(f'{key} must be a boolean')
            payload.update(sandbox='read-only', approval_policy='never', new_thread=True,
                           update_default_thread=False, notification={'enabled': False})
        if kind in {"research_task", "experiment_task"} and not payload.get("name"):
            raise ValueError("research/experiment name is required")
        timeout = payload.get("timeout", 1800)
        if type(timeout) is not int or not 1 <= timeout <= 86400:
            raise ValueError("timeout must be an integer between 1 and 86400 seconds")
        budget = payload.get("max_total_tokens")
        if budget is not None and (type(budget) is not int or budget <= 0):
            raise ValueError("max_total_tokens must be a positive integer")
        # Resolve authorization defaults at submission, not at eventual execution.
        for key in ("sandbox", "approval_policy", "model", "reasoning_effort"):
            payload.setdefault(key, project[key])
        if payload["sandbox"] not in {"read-only", "workspace-write", "danger-full-access"}:
            raise ValueError("invalid sandbox")
        if payload["approval_policy"] not in {"never", "on-request", "on-failure", "untrusted"}:
            raise ValueError("invalid approval policy")
        if not payload.get("new_thread"):
            if not payload.get("thread_id"):
                latest = self.store.latest_session(project["id"])
                payload["thread_id"] = project["default_thread_id"] or (latest["thread_id"] if latest else None)
            if not payload["thread_id"] and kind in {"prompt", "daily_summary"}:
                payload["new_thread"] = True
        resource = "thread:" + payload["thread_id"] if payload.get("thread_id") and not payload.get("new_thread") else f"project:{project['id']}"
        if payload.get("new_thread") and payload["sandbox"] == "read-only":
            resource = "isolated-read:" + uuid.uuid4().hex
        if kind == "research_task":
            resource = f"research:{project['id']}:{payload['name']}"
        access_mode = "read" if payload["sandbox"] == "read-only" and kind != "experiment_task" else "write"
        if kind == 'insight_refresh':
            # The sources stay read-only, while local reports/profile updates
            # must serialize across project scopes to avoid duplicate inference.
            resource, access_mode = 'insights:local', 'write'
        ts = int(time.time())
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            if kind == 'insight_refresh':
                pending = self.conn.execute("""SELECT id FROM jobs WHERE kind=? AND request_json=?
                    AND status IN ('queued','leased','running') ORDER BY id LIMIT 1""",
                    (kind, request_json)).fetchone()
                if pending is not None:
                    self.conn.commit()
                    return self.get(pending['id'])
            if idempotency_key:
                prior = self.conn.execute("SELECT * FROM jobs WHERE project_id=? AND idempotency_key=?",
                                          (project["id"], idempotency_key)).fetchone()
                if prior is not None:
                    same_request = prior["request_json"] == request_json if prior["request_json"] else json.loads(prior["payload_json"]) == payload
                    if prior["kind"] != kind or not same_request:
                        raise ValueError("idempotency key was already used for a different request")
                    self.conn.commit()
                    return self.get(prior["id"])
            for dependency in dependencies:
                if type(dependency) is not int or dependency <= 0:
                    raise ValueError("dependency ids must be positive integers")
                parent = self.get(dependency)
                if parent is None or parent["project_id"] != project["id"]:
                    raise ValueError("dependencies must be existing jobs in the same project")
            if kind == "review_synthesis":
                from .review_workflow import validate_review_jobs
                validate_review_jobs(self.store, project["id"], dependencies, require_success=False)
            cursor = self.conn.execute("""INSERT INTO jobs
                (project_id, kind, payload_json, request_json, resource_key, access_mode, idempotency_key, source, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (project["id"], kind, json.dumps(payload), request_json, resource, access_mode, idempotency_key, source, ts, ts))
            job_id = cursor.lastrowid
            self.conn.executemany("INSERT INTO job_dependencies VALUES (?, ?)",
                                  [(job_id, dependency) for dependency in dependencies])
            self._event(job_id, "submitted", {"source": source, "depends_on": dependencies})
            self.conn.commit()
            return self.get(job_id)
        except Exception:
            self.conn.rollback()
            raise

    def get(self, job_id: int) -> dict[str, Any] | None:
        row = self.conn.execute("""SELECT jobs.*, projects.name AS project_name FROM jobs
            JOIN projects ON projects.id=jobs.project_id WHERE jobs.id=?""", (job_id,)).fetchone()
        return decode_job(row)

    def list(self, project_name: str | None = None, *, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.conn.execute("""SELECT jobs.*, projects.name AS project_name FROM jobs
            JOIN projects ON projects.id=jobs.project_id
            WHERE (? IS NULL OR projects.name=?) ORDER BY jobs.id DESC LIMIT ?""",
            (project_name, project_name, max(1, min(limit, 500)))).fetchall()
        return [decode_job(row) for row in rows]

    def events(self, job_id: int, *, limit: int = 200) -> list[dict[str, Any]]:
        return [{**dict(row), "payload": json.loads(row["payload_json"])} for row in self.conn.execute(
            "SELECT * FROM job_events WHERE job_id=? ORDER BY id DESC LIMIT ?",
            (job_id, max(1, min(limit, 1000))))]

    def claim(self, owner: str, *, lease_seconds: int = 60, job_id: int | None = None) -> dict[str, Any] | None:
        if not owner or lease_seconds < 1:
            raise ValueError("owner and positive lease duration are required")
        ts = int(time.time())
        try:
            row = self.conn.execute("""UPDATE jobs SET status='leased', lease_owner=?,
                lease_expires_at=?, attempts=attempts+1, updated_at=?
                WHERE id=(SELECT j.id FROM jobs j
                    WHERE j.status='queued' AND j.cancel_requested_at IS NULL
                      AND (? IS NULL OR j.id=?)
                      AND NOT EXISTS (SELECT 1 FROM jobs a WHERE a.status IN ('leased','running')
                          AND (a.resource_key=j.resource_key OR
                               (a.project_id=j.project_id AND (a.access_mode='write' OR j.access_mode='write'))))
                      AND NOT EXISTS (SELECT 1 FROM job_dependencies d JOIN jobs p ON p.id=d.depends_on
                                      WHERE d.job_id=j.id AND p.status!='succeeded')
                    ORDER BY j.id LIMIT 1) RETURNING id""",
                (owner, ts + lease_seconds, ts, job_id, job_id)).fetchone()
            if row:
                self._event(row["id"], "leased", {"owner": owner})
            self.conn.commit()
            return self.get(row["id"]) if row else None
        except Exception:
            self.conn.rollback()
            raise

    def start(self, job_id: int, owner: str) -> bool:
        ts = int(time.time())
        cur = self.conn.execute("""UPDATE jobs SET status='running', started_at=?, updated_at=?
            WHERE id=? AND status='leased' AND lease_owner=? AND lease_expires_at>?
              AND cancel_requested_at IS NULL""", (ts, ts, job_id, owner, ts))
        if cur.rowcount:
            self._event(job_id, "started", {})
        self.conn.commit()
        return bool(cur.rowcount)

    def heartbeat(self, job_id: int, owner: str, *, lease_seconds: int = 60) -> bool:
        ts = int(time.time())
        cur = self.conn.execute("""UPDATE jobs SET lease_expires_at=?, updated_at=?
            WHERE id=? AND lease_owner=? AND status IN ('leased','running')
              AND lease_expires_at>?""", (ts + lease_seconds, ts, job_id, owner, ts))
        self.conn.commit()
        return bool(cur.rowcount)

    def observe_usage(self, job_id: int, owner: str, total: int | None, budget: int | None,
                      attempt: int | None = None) -> bool:
        ts = int(time.time())
        row = self.conn.execute("""UPDATE jobs SET result_json=json_set(result_json,'$.budget_usage',json(?))
            WHERE id=? AND lease_owner=? AND status='running' AND lease_expires_at>? RETURNING id""",
            (json.dumps({'total_tokens': total, 'attempt_tokens': attempt,
                         'max_total_tokens': budget, 'provisional': True}), job_id, owner, ts)).fetchone()
        if row:
            self._event(job_id, 'usage_observed', {'total_tokens': total, 'attempt_tokens': attempt, 'max_total_tokens': budget})
        self.conn.commit()
        return row is not None

    def finish(self, job_id: int, owner: str, status: str, *, result: dict | None = None, error: str = "") -> bool:
        if status not in TERMINAL:
            raise ValueError("invalid terminal job status")
        ts = int(time.time())
        row = self.conn.execute("""UPDATE jobs SET
            status=CASE WHEN ?='cancelled' AND pause_requested_at IS NOT NULL AND cancel_requested_at IS NULL
                        THEN CASE WHEN json_extract(?,'$.interruption_confirmed')=1 OR json_extract(?,'$.not_started')=1
                                  THEN 'paused' ELSE 'blocked' END ELSE ? END,
            result_json=?, error=?, completed_at=?,
            updated_at=?, lease_owner=NULL, lease_expires_at=NULL
            WHERE id=? AND lease_owner=? AND status IN ('leased','running') AND lease_expires_at>?
            RETURNING status""",
            (status, json.dumps(result or {}, default=str), json.dumps(result or {}, default=str), status,
             json.dumps(result or {}, default=str), error, ts, ts, job_id, owner, ts)).fetchone()
        if row:
            self._event(job_id, row["status"], {"error": error})
        self.conn.commit()
        return row is not None

    def pause(self, job_id: int) -> dict:
        from .job_controls import pause_job
        return pause_job(self, job_id)

    def resume(self, job_id: int, *, expected_revision: int | None = None) -> dict:
        from .job_controls import resume_job
        return resume_job(self, job_id, expected_revision=expected_revision)

    def revise(self, job_id: int, prompt: str, *, expected_revision: int) -> dict:
        from .job_controls import revise_job
        return revise_job(self, job_id, prompt, expected_revision=expected_revision)

    def cancel(self, job_id: int) -> dict[str, Any]:
        job = self.get(job_id)
        if job is None:
            raise ValueError(f"unknown job: {job_id}")
        ts = int(time.time())
        self.conn.execute("""UPDATE jobs SET cancel_requested_at=?, updated_at=?,
            status=CASE WHEN status IN ('queued','leased','paused') THEN 'cancelled' ELSE status END,
            completed_at=CASE WHEN status IN ('queued','leased','paused') THEN ? ELSE completed_at END
            WHERE id=? AND status NOT IN ('succeeded','failed','cancelled','blocked')""", (ts, ts, ts, job_id))
        self._event(job_id, "cancel_requested", {})
        self.conn.commit()
        return self.get(job_id)

    def recover(self, *, now: int | None = None) -> list[dict[str, Any]]:
        ts = int(time.time()) if now is None else now
        changed = []
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            for row in self.conn.execute("""SELECT * FROM jobs WHERE
                status IN ('leased','running') AND lease_expires_at<=?""", (ts,)).fetchall():
                status = "cancelled" if row["cancel_requested_at"] else ("queued" if row["status"] == "leased" else "blocked")
                reason = "running_lease_lost_inspect_before_retry" if status == "blocked" else "expired_lease"
                self.conn.execute("""UPDATE jobs SET status=?, lease_owner=NULL, lease_expires_at=NULL,
                    error=?, updated_at=? WHERE id=?""", (status, reason, ts, row["id"]))
                self._event(row["id"], "recovered", {"status": status, "reason": reason})
                changed.append({"id": row["id"], "status": status})
            rows = self.conn.execute("""SELECT DISTINCT j.id FROM jobs j JOIN job_dependencies d ON d.job_id=j.id
                JOIN jobs p ON p.id=d.depends_on WHERE j.status='queued'
                AND p.status IN ('failed','blocked','cancelled')""").fetchall()
            for row in rows:
                self.conn.execute("UPDATE jobs SET status='blocked', error='dependency_not_successful', updated_at=? WHERE id=?", (ts, row["id"]))
                self._event(row["id"], "blocked", {"reason": "dependency_not_successful"})
                changed.append({"id": row["id"], "status": "blocked"})
            self.conn.commit()
            return changed
        except Exception:
            self.conn.rollback()
            raise

    def _event(self, job_id: int, event_type: str, payload: dict) -> None:
        self.conn.execute("INSERT INTO job_events(job_id,event_type,payload_json,created_at) VALUES (?,?,?,?)",
                          (job_id, event_type, json.dumps(payload), int(time.time())))


def run_worker(store: Store, *, once: bool = False, max_jobs: int = 1, interval: float = 2,
               job_id: int | None = None) -> dict[str, Any]:
    """Run bounded tasks. Each callback renews ownership before any continuation."""
    import threading
    from .db import Store
    queue = JobQueue(store)
    queue.recover()
    owner = f"{os.getpid()}-{uuid.uuid4().hex}"
    results = []
    while True:
        job = queue.claim(owner, job_id=job_id)
        if job is None:
            if once or job_id is not None:
                break
            time.sleep(max(0.1, interval))
            continue
        if not queue.start(job["id"], owner):
            continue
        stop_heartbeat = threading.Event()
        ownership_lost = threading.Event()

        def pulse() -> None:
            with Store(store.path) as pulse_store:
                while not stop_heartbeat.wait(10):
                    try:
                        if not JobQueue(pulse_store).heartbeat(job["id"], owner):
                            ownership_lost.set()
                            return
                    except sqlite3.Error:
                        ownership_lost.set()
                        return

        heartbeat = threading.Thread(target=pulse, daemon=True)
        heartbeat.start()
        def cancelled() -> bool:
            current = queue.get(job["id"])
            return (ownership_lost.is_set() or current is None or current["cancel_requested_at"] is not None
                    or current["pause_requested_at"] is not None
                    or current["status"] != "running" or current["lease_owner"] != owner
                    or current["lease_expires_at"] <= int(time.time()))

        usage_baseline = None
        turn_tokens = 0
        prior_tokens = (job.get("checkpoint") or {}).get("consumed_tokens") or 0
        prior_tokens_known = (job.get('checkpoint') or {}).get('consumed_tokens', 0) is not None
        usage_observed = False
        def usage_limit(usage: dict) -> bool:
            nonlocal usage_baseline, turn_tokens, usage_observed
            usage_observed = True
            limit = job["payload"].get("max_total_tokens")
            native_total = usage.get("total") or {}
            native_last = usage.get("last") or {}
            if isinstance(usage.get("turn"), dict):
                total = usage["turn"].get("totalTokens", usage["turn"].get("total_tokens", 0))
            elif native_total:
                cumulative = native_total.get("totalTokens", native_total.get("total_tokens", 0))
                last = native_last.get("totalTokens", native_last.get("total_tokens", 0))
                if usage_baseline is None:
                    usage_baseline = max(0, cumulative - last)
                total = max(0, cumulative - usage_baseline)
            else:
                total = usage.get("totalTokens", usage.get("total_tokens", 0))
            turn_tokens = max(turn_tokens, total)
            if not queue.observe_usage(job['id'], owner, prior_tokens + total if prior_tokens_known else None,
                                       limit, attempt=turn_tokens):
                ownership_lost.set()
                return False
            return not limit or (prior_tokens_known and prior_tokens + total < limit)

        execution_started = time.monotonic()
        try:
            result = _execute_job(store, job, cancelled, usage_limit)
            result["attempt_elapsed_seconds"] = round(time.monotonic() - execution_started, 6)
            result["elapsed_seconds"] = result["attempt_elapsed_seconds"] + (job.get("checkpoint") or {}).get("elapsed_seconds", 0)
            result["budget_usage"] = {"total_tokens": prior_tokens + turn_tokens if usage_observed and prior_tokens_known else None,
                                      "attempt_tokens": turn_tokens if usage_observed else None,
                                      "max_total_tokens": job["payload"].get("max_total_tokens")}
            state = str(result.get("status", "failed"))
            status = "succeeded" if state in {"completed", "succeeded", "done"} else "cancelled" if state in {"cancelled", "interrupted"} else "blocked" if state in {"blocked", "idle", "needs_reconciliation"} else "failed"
            limit = job['payload'].get('max_total_tokens')
            observed = result['budget_usage']['total_tokens']
            result['budget_usage']['exceeded'] = bool(limit is not None and observed is not None and observed > limit)
            if status == 'succeeded' and result['budget_usage']['exceeded']:
                status = 'blocked'
                result['error'] = 'observed_token_budget_exceeded; inspect the completed result before continuing'
            queue.finish(job["id"], owner, status, result=result, error=str(result.get("error") or ""))
        except Exception as exc:
            queue.finish(job["id"], owner, "failed", error=str(exc))
        finally:
            stop_heartbeat.set()
            heartbeat.join(timeout=2)
        _notify_job(store, queue.get(job["id"]))
        results.append(queue.get(job["id"]))
        if (max_jobs > 0 and len(results) >= max_jobs) or job_id is not None:
            break
    return {"processed": len(results), "jobs": results}


def _execute_job(store: Store, job: dict, cancelled, usage_limit) -> dict:
    from . import service
    payload = job["payload"]
    if job['kind'] == 'insight_refresh':
        from .insight_tasks import execute_insight_refresh
        return execute_insight_refresh(store, job, cancelled, usage_limit)
    project_name = job["project_name"]
    if cancelled():
        return {"status": "cancelled", "not_started": True}
    if job["kind"] in {"prompt", "daily_summary", "review_synthesis"}:
        from .prompts import daily_summary_prompt
        summary = job["kind"] == "daily_summary"
        prompt = daily_summary_prompt(project_name) if summary else payload["prompt"]
        checkpoint = job.get("checkpoint") or {}
        resumed_thread = checkpoint.get("thread_id")
        if resumed_thread:
            prompt = ("Resume this paused task using the existing conversation. First inspect any prior tool "
                      "or external effects; do not blindly repeat completed actions. The current goal below "
                      "replaces any earlier goal if it differs. Stay within the original permissions and budget.\n\n"
                      + prompt)
        if job["kind"] == "review_synthesis":
            from .review_workflow import synthesis_prompt
            prompt = synthesis_prompt(store, payload)
        return service.send_prompt(store, project_name, prompt,
            kind="daily_summary" if summary else payload.get("kind", "queued"),
            new_thread=False if resumed_thread else bool(payload.get("new_thread")),
            thread_id=resumed_thread or payload.get("thread_id"),
            sandbox="read-only" if summary else payload.get("sandbox"),
            approval_policy="never" if summary else payload.get("approval_policy"),
            model=payload.get("model"), reasoning_effort=payload.get("reasoning_effort"),
            timeout=max(1, payload.get("timeout", 1800) - int(checkpoint.get("elapsed_seconds", 0))), sync_first=False,
            update_default_thread=payload.get("update_default_thread", True),
            trace_id=f"job:{job['id']}", cancellation_callback=cancelled, usage_callback=usage_limit)
    if job["kind"] == "sync_sessions":
        return {"status": "completed", "sessions": len(service.sync_sessions(store, project_name))}
    if job["kind"] == "research_task":
        from .research_ops import run_research_task
        return run_research_task(store, project_name, payload["name"], task_id=payload.get("task_id"),
                                 timeout=payload.get("timeout", 60), allow_control=False)
    if job["kind"] == "experiment_task":
        from .experiment_ops import run_ready_experiment_tasks
        result = run_ready_experiment_tasks(store, project_name, payload["name"], max_tasks=1)
        tasks = result.get("tasks") or result.get("results") or []
        if not tasks:
            result["status"] = "idle"
        elif any(task.get("status") == "failed" or (task.get("task") or {}).get("status") == "failed" for task in tasks):
            result["status"] = "failed"
        elif any(task.get("status") in {"failed", "blocked"}
                 or (task.get("task") or {}).get("status") in {"failed", "blocked"}
                 or task.get("executed") is False
                 or (task.get("execution") or {}).get("executed") is False for task in tasks):
            result["status"] = "blocked"
        else:
            result["status"] = "completed"
        return result
    raise ValueError("unsupported job kind")


def _notify_job(store: Store, job: dict) -> None:
    """Best-effort configured summary notifications; never persist token values."""
    notification = job["payload"].get("notification")
    if not isinstance(notification, dict) or not notification.get("enabled"):
        return
    from .telegram import notify_project_bindings
    result = job["result"]
    message = result.get("output_text") or job["error"] or f"Task #{job['id']}: {job['status']}"
    try:
        outcome = notify_project_bindings(store, job["project_id"], message,
            token_file=notification.get("token_file"), token_env=notification.get("token_env", "MYCODEX_TELEGRAM_BOT_TOKEN"),
            proxy_url=notification.get("proxy_url"))
        JobQueue(store)._event(job["id"], "notification_result", outcome)
    except Exception as exc:
        JobQueue(store)._event(job["id"], "notification_failed", {"error": type(exc).__name__})
    store.commit()


def enqueue_scheduled_jobs(store: Store, *, timeout: int = 1800, notification: dict | None = None) -> list[dict]:
    from datetime import datetime
    from .daemon import _rule_due, _today_at
    queue = JobQueue(store)
    queued = []
    now = datetime.now()
    day_start = int(datetime(now.year, now.month, now.day).timestamp())
    for project in store.list_projects():
        pending = queue.list(project["name"], limit=500)
        if project["daily_summary_enabled"] and now >= _today_at(project["daily_summary_time"]):
            if not store.has_run_since(project["id"], kind="daily_summary", since_ts=day_start):
                key = f"scheduled-summary:{project['id']}:{now.date().isoformat()}"
                prior = store.conn.execute("SELECT id FROM jobs WHERE project_id=? AND idempotency_key=?", (project["id"], key)).fetchone()
                if prior is None:
                    queued.append(queue.submit(project["name"], "daily_summary", {"timeout": timeout, "notification": notification or {}}, source="daemon", idempotency_key=key))
        for rule in store.list_rules(project["id"]):
            if not rule["enabled"] or not _rule_due(rule):
                continue
            kind_label = f"rule:{rule['id']}"
            if any(job["status"] in {"queued", "leased", "running", "blocked"} and job["payload"].get("kind") == kind_label for job in pending):
                continue
            payload = {"timeout": timeout, "kind": kind_label, "prompt": rule["prompt"], "notification": notification or {}}
            if rule["thread_id"]:
                payload["thread_id"] = rule["thread_id"]
            if rule["kind"] == "daily_summary":
                from .prompts import daily_summary_prompt
                payload.update(prompt=daily_summary_prompt(project["name"]), sandbox="read-only", approval_policy="never")
            key = f"scheduled-rule:{rule['id']}:{int(time.time()) // (rule['interval_seconds'] or 86400)}"
            queued.append(queue.submit(project["name"], "prompt", payload, source="daemon", idempotency_key=key))
            store.update_rule_run(rule["id"])
    return queued
