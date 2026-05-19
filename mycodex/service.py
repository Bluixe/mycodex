from __future__ import annotations

import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from .codex_app import CodexAppServerClient
from .db import Store
from .prompts import daily_summary_prompt, experiment_control_prompt, research_control_prompt


VALID_SANDBOXES = {"read-only", "workspace-write", "danger-full-access"}
VALID_APPROVAL_POLICIES = {"untrusted", "on-failure", "on-request", "never"}


def ensure_valid_sandbox(value: str) -> str:
    if value not in VALID_SANDBOXES:
        raise ValueError(f"invalid sandbox {value!r}; choose one of {sorted(VALID_SANDBOXES)}")
    return value


def ensure_valid_approval_policy(value: str) -> str:
    if value not in VALID_APPROVAL_POLICIES:
        raise ValueError(
            f"invalid approval policy {value!r}; choose one of {sorted(VALID_APPROVAL_POLICIES)}"
        )
    return value


def sync_sessions(
    store: Store,
    project_name: str,
    *,
    limit: int = 100,
    use_state_db_only: bool = True,
) -> list[dict[str, Any]]:
    project = require_project(store, project_name)
    with CodexAppServerClient() as client:
        threads = client.list_threads(
            cwd=project["path"],
            limit=limit,
            use_state_db_only=use_state_db_only,
        )
    for thread in threads:
        store.upsert_session(project["id"], thread)
    store.commit()
    if threads and not project["default_thread_id"]:
        store.update_default_thread(project["id"], threads[0]["id"])
    return threads


def send_prompt(
    store: Store,
    project_name: str,
    prompt: str,
    *,
    kind: str = "manual",
    new_thread: bool = False,
    thread_id: str | None = None,
    timeout: int = 1800,
    sync_first: bool = True,
    sandbox: str | None = None,
    approval_policy: str | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
    update_default_thread: bool = True,
    event_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    project = require_project(store, project_name)
    if sync_first:
        try:
            sync_sessions(store, project_name)
            project = require_project(store, project_name)
        except Exception:
            # Sending can still work with a stored thread id even if sync is unavailable.
            pass

    chosen_thread_id = None if new_thread else thread_id or project["default_thread_id"]
    if chosen_thread_id is None and not new_thread:
        latest = store.latest_session(project["id"])
        chosen_thread_id = latest["thread_id"] if latest else None

    chosen_sandbox = ensure_valid_sandbox(sandbox or project["sandbox"])
    chosen_approval = ensure_valid_approval_policy(approval_policy or project["approval_policy"])
    chosen_model = model or project["model"]
    chosen_effort = reasoning_effort or project["reasoning_effort"]

    run_id = store.add_run(project["id"], kind=kind, prompt=prompt, thread_id=chosen_thread_id)
    try:
        with CodexAppServerClient() as client:
            thread = client.start_or_resume_thread(
                thread_id=chosen_thread_id,
                cwd=project["path"],
                sandbox=chosen_sandbox,
                approval_policy=chosen_approval,
                model=chosen_model,
                reasoning_effort=chosen_effort,
            )
            chosen_thread_id = thread["id"]
            store.upsert_session(project["id"], thread)
            if update_default_thread:
                store.update_default_thread(project["id"], chosen_thread_id)
            turn_id, turn = client.start_turn(
                thread_id=chosen_thread_id,
                prompt=prompt,
                cwd=project["path"],
                approval_policy=chosen_approval,
                model=chosen_model,
                reasoning_effort=chosen_effort,
            )
            result = client.wait_for_turn(
                thread_id=chosen_thread_id,
                turn_id=turn_id,
                timeout=timeout,
                on_event=event_callback,
            )
            status = "completed" if result.completed else "timeout"
            raw = {"thread": thread, "turn": turn, "result": result.raw}
            store.finish_run(
                run_id,
                status=status,
                output_text=result.output_text,
                raw_json=raw,
                thread_id=chosen_thread_id,
            )
            return {
                "run_id": run_id,
                "thread_id": chosen_thread_id,
                "turn_id": turn_id,
                "status": status,
                "output_text": result.output_text,
            }
    except Exception as exc:
        store.finish_run(
            run_id,
            status="failed",
            error=str(exc),
            thread_id=chosen_thread_id,
        )
        raise


def run_daily_summary(
    store: Store,
    project_name: str,
    *,
    timeout: int = 1800,
    sync_first: bool = True,
) -> dict[str, Any]:
    prompt = daily_summary_prompt(project_name)
    return send_prompt(
        store,
        project_name,
        prompt,
        kind="daily_summary",
        timeout=timeout,
        sync_first=sync_first,
        sandbox="read-only",
        approval_policy="never",
    )


def start_experiment_control_turn(
    store: Store,
    project_name: str,
    experiment_name: str,
    *,
    timeout: int = 1800,
    sandbox: str | None = None,
    approval_policy: str | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
) -> dict[str, Any]:
    project = require_project(store, project_name)
    experiment = store.get_experiment(project["id"], experiment_name)
    if experiment is None:
        raise ValueError(f"unknown experiment {experiment_name!r} for project {project_name!r}")
    store.update_experiment_status(experiment["id"], "running")
    store.add_experiment_event(
        experiment["id"],
        event_type="control_turn_started",
        level="info",
        message="Started Codex experiment control turn.",
        source="mycodex",
        status="running",
        phase="analyzing",
        phase_message="Analyzing previous results and planning the next iteration.",
    )
    stream = ExperimentEventStreamer(store, experiment["id"])
    prompt = experiment_control_prompt(
        project_name=project["name"],
        experiment_name=experiment["name"],
        experiment_id=experiment["id"],
        objective=experiment["objective"],
        max_iterations=experiment["max_iterations"],
        remote_ref=experiment["remote_ref"],
    )
    try:
        result = send_prompt(
            store,
            project_name,
            prompt,
            kind=f"experiment:{experiment['id']}:control",
            thread_id=experiment["thread_id"],
            timeout=timeout,
            sync_first=False,
            sandbox=sandbox,
            approval_policy=approval_policy,
            model=model,
            reasoning_effort=reasoning_effort,
            update_default_thread=False,
            event_callback=stream.handle_codex_event,
        )
    except Exception as exc:
        store.add_experiment_event(
            experiment["id"],
            event_type="control_turn_failed",
            level="error",
            message=str(exc),
            source="mycodex",
        )
        raise
    store.add_experiment_event(
        experiment["id"],
        event_type="control_turn_completed",
        level="info" if result["status"] == "completed" else "warning",
        message=f"Control turn {result['status']}.",
        payload={"thread_id": result["thread_id"], "turn_id": result["turn_id"]},
        source="mycodex",
        run_id=result["run_id"],
        phase="waiting_for_decision",
        phase_message="Control analysis completed; waiting for decision or next action.",
    )
    return result


def start_research_control_turn(
    store: Store,
    project_name: str,
    research_name: str,
    *,
    timeout: int = 1800,
    sandbox: str | None = None,
    approval_policy: str | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
) -> dict[str, Any]:
    from .research_ops import (
        apply_research_contract,
        build_research_control_context,
        extract_research_contract,
        write_research_markdown,
    )

    project = require_project(store, project_name)
    research = store.get_research_thread(project["id"], research_name)
    if research is None:
        raise ValueError(f"unknown research {research_name!r} for project {project_name!r}")
    ledger_context = build_research_control_context(store, project_name, research_name)
    store.update_research_status(research["id"], "running")
    store.add_research_event(
        research["id"],
        event_type="control_turn_started",
        level="info",
        message="Started Codex research control turn.",
        payload={"ledger_context_chars": len(ledger_context)},
        source="mycodex",
        status="running",
        phase="analyzing",
        phase_message="Research control session is reviewing persisted ledgers and choosing the next action.",
    )
    stream = ResearchEventStreamer(store, research["id"])
    prompt = research_control_prompt(
        project_name=project["name"],
        research_name=research["name"],
        research_id=research["id"],
        question=research["question"],
        max_iterations=research["max_iterations"],
        ledger_context=ledger_context,
    )
    try:
        result = send_prompt(
            store,
            project_name,
            prompt,
            kind=f"research:{research['id']}:control",
            thread_id=research["thread_id"],
            timeout=timeout,
            sync_first=False,
            sandbox=sandbox,
            approval_policy=approval_policy,
            model=model,
            reasoning_effort=reasoning_effort,
            update_default_thread=False,
            event_callback=stream.handle_codex_event,
        )
    except Exception as exc:
        store.add_research_event(
            research["id"],
            event_type="control_turn_failed",
            level="error",
            message=str(exc),
            source="mycodex",
        )
        raise
    store.add_research_event(
        research["id"],
        event_type="control_turn_completed",
        level="info" if result["status"] == "completed" else "warning",
        message=f"Research control turn {result['status']}.",
        payload={"thread_id": result["thread_id"], "turn_id": result["turn_id"]},
        source="mycodex",
        run_id=result["run_id"],
        phase="waiting_for_decision",
        phase_message="Research control analysis completed; applying contract if available.",
    )
    contract = extract_research_contract(result.get("output_text") or "")
    if contract is None:
        store.add_research_event(
            research["id"],
            event_type="blocked",
            level="warning",
            message="Research control turn did not return a parseable JSON contract.",
            payload={"run_id": result["run_id"], "status": result["status"]},
            source="mycodex",
            phase="waiting_for_decision",
        )
        try:
            write_research_markdown(store, project_name, research_name)
        except Exception:
            pass
        return {**result, "contract": None, "applied": None}
    applied = apply_research_contract(
        store,
        project_name,
        research_name,
        contract,
        run_id=result["run_id"],
    )
    return {**result, "contract": contract, "applied": applied}


class ExperimentEventStreamer:
    def __init__(
        self,
        store: Store,
        experiment_id: int,
        *,
        min_chars: int = 320,
        max_seconds: float = 8.0,
    ) -> None:
        self.store = store
        self.experiment_id = experiment_id
        self.min_chars = min_chars
        self.max_seconds = max_seconds
        self._buffer: list[str] = []
        self._last_flush = time.monotonic()

    def handle_codex_event(self, event: dict[str, Any]) -> None:
        method = event.get("method")
        params = event.get("params") or {}
        if method == "item/agentMessage/delta":
            delta = params.get("delta") or ""
            if delta:
                self._buffer.append(delta)
                if self._should_flush():
                    self.flush("agent_message_delta")
        elif method in {"item/completed", "turn/completed", "turn/failed"}:
            self.flush("agent_message_delta")
            self.store.add_experiment_event(
                self.experiment_id,
                event_type="codex_event",
                level="info" if method != "turn/failed" else "error",
                message=method,
                payload=_compact_codex_event(event),
                source="codex",
            )
        elif method in {"item/commandExecution/started", "item/commandExecution/completed"}:
            self.store.add_experiment_event(
                self.experiment_id,
                event_type="codex_tool",
                level="info",
                message=method,
                payload=_compact_codex_event(event),
                source="codex",
            )

    def _should_flush(self) -> bool:
        text_len = sum(len(chunk) for chunk in self._buffer)
        return text_len >= self.min_chars or time.monotonic() - self._last_flush >= self.max_seconds

    def flush(self, event_type: str) -> None:
        if not self._buffer:
            return
        chunk = "".join(self._buffer)
        self._buffer.clear()
        self._last_flush = time.monotonic()
        self.store.add_experiment_event(
            self.experiment_id,
            event_type=event_type,
            level="info",
            message=chunk.strip()[:500],
            payload={"text": chunk},
            source="codex",
        )


class ResearchEventStreamer:
    def __init__(
        self,
        store: Store,
        research_id: int,
        *,
        min_chars: int = 320,
        max_seconds: float = 8.0,
    ) -> None:
        self.store = store
        self.research_id = research_id
        self.min_chars = min_chars
        self.max_seconds = max_seconds
        self._buffer: list[str] = []
        self._last_flush = time.monotonic()

    def handle_codex_event(self, event: dict[str, Any]) -> None:
        method = event.get("method")
        params = event.get("params") or {}
        if method == "item/agentMessage/delta":
            delta = params.get("delta") or ""
            if delta:
                self._buffer.append(delta)
                if self._should_flush():
                    self.flush("agent_message_delta")
        elif method in {"item/completed", "turn/completed", "turn/failed"}:
            self.flush("agent_message_delta")
            self.store.add_research_event(
                self.research_id,
                event_type="codex_event",
                level="info" if method != "turn/failed" else "error",
                message=method,
                payload=_compact_codex_event(event),
                source="codex",
            )
        elif method in {"item/commandExecution/started", "item/commandExecution/completed"}:
            self.store.add_research_event(
                self.research_id,
                event_type="codex_tool",
                level="info",
                message=method,
                payload=_compact_codex_event(event),
                source="codex",
            )

    def _should_flush(self) -> bool:
        text_len = sum(len(chunk) for chunk in self._buffer)
        return text_len >= self.min_chars or time.monotonic() - self._last_flush >= self.max_seconds

    def flush(self, event_type: str) -> None:
        if not self._buffer:
            return
        chunk = "".join(self._buffer)
        self._buffer.clear()
        self._last_flush = time.monotonic()
        self.store.add_research_event(
            self.research_id,
            event_type=event_type,
            level="info",
            message=chunk.strip()[:500],
            payload={"text": chunk},
            source="codex",
        )


def _compact_codex_event(event: dict[str, Any]) -> dict[str, Any]:
    params = event.get("params") or {}
    compact: dict[str, Any] = {"method": event.get("method")}
    for key in ("threadId", "turnId"):
        if key in params:
            compact[key] = params[key]
    item = params.get("item")
    if isinstance(item, dict):
        compact["item"] = {
            key: item.get(key)
            for key in ("id", "type", "status", "title")
            if item.get(key) is not None
        }
    turn = params.get("turn")
    if isinstance(turn, dict):
        compact["turn"] = {
            key: turn.get(key)
            for key in ("id", "status")
            if turn.get(key) is not None
        }
    return compact


def require_project(store: Store, project_name: str):
    project = store.get_project(project_name)
    if project is None:
        raise ValueError(f"unknown project {project_name!r}; run `mycodex projects add` first")
    return project


def git_snapshot(path: str) -> dict[str, str]:
    root = Path(path)
    if not root.exists():
        return {"state": "missing", "summary": "path does not exist"}
    try:
        branch = _git(root, ["rev-parse", "--abbrev-ref", "HEAD"])
        sha = _git(root, ["rev-parse", "--short", "HEAD"])
        status = _git(root, ["status", "--short"])
        last_commit = _git(root, ["log", "-1", "--pretty=%h %cs %s"])
    except subprocess.CalledProcessError:
        return {"state": "not-git", "summary": "not a git repository"}
    dirty_count = len([line for line in status.splitlines() if line.strip()])
    return {
        "state": "git",
        "branch": branch,
        "sha": sha,
        "dirty_count": str(dirty_count),
        "last_commit": last_commit,
    }


def _git(path: Path, args: list[str]) -> str:
    return subprocess.check_output(
        ["git", "-C", str(path), *args],
        text=True,
        stderr=subprocess.DEVNULL,
    ).strip()


def format_ts(ts: int | None) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
