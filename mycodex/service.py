from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from .codex_app import CodexAppServerClient
from .db import Store
from .prompts import daily_summary_prompt


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
