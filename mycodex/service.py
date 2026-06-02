from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from .codex_app import CodexAppServerClient
from .db import Store
from .prompts import (
    daily_summary_prompt,
    experiment_contract_repair_prompt,
    experiment_control_prompt,
    experiment_peer_review_decision_prompt,
    experiment_peer_review_prompt,
    research_control_prompt,
)


VALID_SANDBOXES = {"read-only", "workspace-write", "danger-full-access"}
VALID_APPROVAL_POLICIES = {"untrusted", "on-failure", "on-request", "never"}
NOISY_EXPERIMENT_EVENT_TYPES = {"agent_message_delta", "codex_event", "codex_tool"}
CONTROL_CONTEXT_IMPORTANT_EVENT_TYPES = {
    "blocked",
    "completed",
    "control_contract_parse_failed",
    "control_contract_parsed",
    "control_turn_failed",
    "decision",
    "failed",
    "peer_review_decision",
    "peer_review_skipped",
    "peer_review_soft_objections_recorded",
    "paused_with_reviewer_objections",
    "preflight_warning",
    "remote_anomaly",
    "remote_anomaly_control_failed",
    "remote_job_exited_early",
    "remote_job_started",
    "remote_sync_failed",
    "result_analysis",
    "supervisor_retry_scheduled",
    "watchdog_skipped",
}


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
    recent_context = _experiment_control_context(store, experiment)
    prompt = experiment_control_prompt(
        project_name=project["name"],
        experiment_name=experiment["name"],
        experiment_id=experiment["id"],
        objective=experiment["objective"],
        max_iterations=experiment["max_iterations"],
        remote_ref=experiment["remote_ref"],
        recent_context=recent_context,
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


def start_experiment_contract_repair_turn(
    store: Store,
    project_name: str,
    experiment_name: str,
    *,
    previous_output: str,
    parse_error: str = "",
    timeout: int = 900,
    sandbox: str | None = None,
    approval_policy: str | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
) -> dict[str, Any]:
    project = require_project(store, project_name)
    experiment = store.get_experiment(project["id"], experiment_name)
    if experiment is None:
        raise ValueError(f"unknown experiment {experiment_name!r} for project {project_name!r}")

    store.add_experiment_event(
        experiment["id"],
        event_type="contract_repair_started",
        level="warning",
        message="Started Codex contract repair turn after unparseable control output.",
        payload={"parse_error": parse_error},
        source="mycodex",
        status="running",
        phase="analyzing",
        phase_message="Repairing the control-session JSON contract.",
    )
    stream = ExperimentEventStreamer(store, experiment["id"])
    prompt = experiment_contract_repair_prompt(
        project_name=project["name"],
        experiment_name=experiment["name"],
        experiment_id=experiment["id"],
        parse_error=parse_error,
        previous_output=_truncate_text(previous_output or "", 60000),
    )
    try:
        result = send_prompt(
            store,
            project_name,
            prompt,
            kind=f"experiment:{experiment['id']}:contract_repair",
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
            event_type="contract_repair_failed",
            level="error",
            message=str(exc),
            source="mycodex",
            phase="waiting_for_decision",
            phase_message="Contract repair failed; supervisor will retry later.",
        )
        raise
    store.add_experiment_event(
        experiment["id"],
        event_type="contract_repair_completed",
        level="info" if result["status"] == "completed" else "warning",
        message=f"Contract repair turn {result['status']}.",
        payload={"thread_id": result["thread_id"], "turn_id": result["turn_id"]},
        source="mycodex",
        run_id=result["run_id"],
        phase="checking_git",
        phase_message="Contract repair completed; parsing repaired contract.",
    )
    return result


def _experiment_control_context(store: Store, experiment, *, limit: int = 30) -> str:
    raw_events = store.list_experiment_events(experiment["id"], limit=max(1000, limit * 50))
    noisy_omitted = sum(1 for event in raw_events if event["event_type"] in NOISY_EXPERIMENT_EVENT_TYPES)

    def event_context(event, *, payload_limit: int) -> dict[str, Any]:
        payload_json = event["payload_json"] or "{}"
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError:
            payload = {"raw": _truncate_text(payload_json, 1200)}
        return {
            "id": event["id"],
            "type": event["event_type"],
            "level": event["level"],
            "message": _truncate_text(event["message"] or "", 500),
            "payload": _truncate_payload(payload, payload_limit),
            "source": event["source"],
            "run_status": event["run_status"],
            "created_at": event["created_at"],
        }

    important_events = [
        event_context(event, payload_limit=1800)
        for event in raw_events
        if event["event_type"] in CONTROL_CONTEXT_IMPORTANT_EVENT_TYPES
        or event["level"] in {"warning", "error"}
    ][:limit]
    recent_events = [
        event_context(event, payload_limit=1000)
        for event in raw_events
        if event["event_type"] not in NOISY_EXPERIMENT_EVENT_TYPES
    ][:limit]
    latest_peer_review_decisions = [
        event_context(event, payload_limit=2600)
        for event in raw_events
        if event["event_type"] == "peer_review_decision"
    ][:5]
    snapshot = {
        "experiment": {
            "id": experiment["id"],
            "name": experiment["name"],
            "status": experiment["status"],
            "phase": experiment["phase"],
            "phase_message": experiment["phase_message"],
            "latest_decision": experiment["latest_decision"],
            "artifact_ref": experiment["artifact_ref"],
            "current_iteration": experiment["current_iteration"],
            "max_iterations": experiment["max_iterations"],
            "remote_ref": experiment["remote_ref"],
            "objective": experiment["objective"],
        },
        "latest_peer_review_decisions_desc": latest_peer_review_decisions,
        "important_events_desc": important_events,
        "recent_events_desc": recent_events,
        "recent_noisy_events_omitted": noisy_omitted,
    }
    return json.dumps(snapshot, ensure_ascii=False, indent=2)


def _truncate_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 15)] + "...<truncated>"


def _truncate_payload(value: Any, limit: int) -> Any:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    if len(text) <= limit:
        return value
    return {"truncated_json": _truncate_text(text, limit)}


def start_experiment_peer_review_turn(
    store: Store,
    project_name: str,
    experiment_name: str,
    *,
    control_output: str,
    contract: dict[str, Any],
    timeout: int = 900,
    model: str | None = None,
    reasoning_effort: str | None = None,
) -> dict[str, Any]:
    project = require_project(store, project_name)
    experiment = store.get_experiment(project["id"], experiment_name)
    if experiment is None:
        raise ValueError(f"unknown experiment {experiment_name!r} for project {project_name!r}")

    store.add_experiment_event(
        experiment["id"],
        event_type="peer_review_started",
        level="info",
        message="Started independent Codex peer-review gate.",
        source="mycodex",
        status="running",
        phase="peer_reviewing",
        phase_message="An independent Codex session is reviewing the execution contract.",
        payload={"contract": contract},
    )
    prompt = experiment_peer_review_prompt(
        project_name=project["name"],
        experiment_name=experiment["name"],
        experiment_id=experiment["id"],
        objective=experiment["objective"],
        control_output=control_output,
        contract=contract,
    )
    review_timeout = max(1, min(timeout, 600))
    decision_timeout = max(1, min(timeout, 180))
    rounds: list[dict[str, Any]] = []
    try:
        first = send_prompt(
            store,
            project_name,
            prompt,
            kind=f"experiment:{experiment['id']}:peer_review",
            new_thread=True,
            timeout=review_timeout,
            sync_first=False,
            sandbox="read-only",
            approval_policy="never",
            model=model,
            reasoning_effort=reasoning_effort,
            update_default_thread=False,
        )
        rounds.append(
            {
                "run_id": first["run_id"],
                "turn_id": first["turn_id"],
                "status": first["status"],
                "output_text": first.get("output_text") or "",
            }
        )
    except Exception as exc:
        result = {
            "thread_id": "",
            "rounds": [
                {
                    "run_id": None,
                    "turn_id": None,
                    "status": "failed",
                    "output_text": "",
                    "error": str(exc),
                }
            ],
            "output_text": "",
        }
        store.add_experiment_event(
            experiment["id"],
            event_type="peer_review_failed",
            level="error",
            message=str(exc),
            payload={"rounds": result["rounds"], "stage": "review"},
            source="mycodex",
            phase="waiting_for_decision",
            phase_message="Peer review failed; control loop will apply bypass policy if eligible.",
        )
        return result

    if first.get("status") == "completed":
        try:
            final = send_prompt(
                store,
                project_name,
                experiment_peer_review_decision_prompt(),
                kind=f"experiment:{experiment['id']}:peer_review_decision",
                thread_id=first["thread_id"],
                timeout=decision_timeout,
                sync_first=False,
                sandbox="read-only",
                approval_policy="never",
                model=model,
                reasoning_effort=reasoning_effort,
                update_default_thread=False,
            )
            rounds.append(
                {
                    "run_id": final["run_id"],
                    "turn_id": final["turn_id"],
                    "status": final["status"],
                    "output_text": final.get("output_text") or "",
                }
            )
        except Exception as exc:
            rounds.append(
                {
                    "run_id": None,
                    "turn_id": None,
                    "status": "failed",
                    "output_text": "",
                    "error": str(exc),
                }
            )
            store.add_experiment_event(
                experiment["id"],
                event_type="peer_review_failed",
                level="warning",
                message=str(exc),
                payload={"thread_id": first["thread_id"], "rounds": rounds, "stage": "decision"},
                source="mycodex",
                phase="waiting_for_decision",
                phase_message="Peer-review decision turn failed; control loop will apply bypass policy if eligible.",
            )
    else:
        rounds.append(
            {
                "run_id": None,
                "turn_id": None,
                "status": "timeout",
                "output_text": "",
                "error": "peer review critique turn did not complete before its timeout",
            }
        )

    result = {
        "thread_id": first["thread_id"],
        "rounds": rounds,
        "output_text": next((item.get("output_text") or "" for item in reversed(rounds) if item.get("output_text")), ""),
    }
    store.add_experiment_event(
        experiment["id"],
        event_type="peer_review_completed",
        level="info",
        message="Independent Codex peer-review gate completed.",
        payload={
            "thread_id": result["thread_id"],
            "rounds": [
                {"run_id": item["run_id"], "turn_id": item["turn_id"], "status": item["status"]}
                for item in result["rounds"]
            ],
        },
        source="mycodex",
        status="running",
        phase="checking_git",
        phase_message="Peer review completed; parsing reviewer decision.",
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
