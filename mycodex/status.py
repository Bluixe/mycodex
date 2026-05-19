from __future__ import annotations

import os
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .db import Store, now_ts
from .service import format_ts, git_snapshot, require_project


def dashboard_snapshot(store: Store) -> dict[str, Any]:
    projects = store.list_projects()
    cards = [project_snapshot(store, project["name"], include_detail=False) for project in projects]
    return {
        "projects": cards,
        "project_count": len(cards),
        "daemon": daemon_snapshot(store.path),
        "generated_at": now_ts(),
        "generated_at_text": format_ts(now_ts()),
    }


def project_snapshot(
    store: Store,
    project_name: str,
    *,
    include_detail: bool = True,
) -> dict[str, Any]:
    project = require_project(store, project_name)
    sessions = store.list_sessions(project["id"])
    runs = store.list_runs(project["id"], limit=10 if include_detail else 5)
    rules = store.list_rules(project["id"])
    experiments = store.list_experiments(project["id"])
    research_threads = store.list_research_threads(project["id"])
    todos = store.list_todos(
        project["id"],
        include_archived=include_detail,
        limit=None if include_detail else 8,
    )
    git = git_snapshot(project["path"])
    health = derive_health(project, sessions, runs, todos, experiments, research_threads, git)
    experiment_events = {
        str(experiment["id"]): store.list_experiment_events(experiment["id"], limit=3)
        for experiment in experiments
    }
    research_events = {
        str(research["id"]): store.list_research_events(research["id"], limit=3)
        for research in research_threads
    }
    return {
        "project": project,
        "git": git,
        "health": health,
        "session_count": len(sessions),
        "latest_session": sessions[0] if sessions else None,
        "sessions": sessions if include_detail else [],
        "runs": runs,
        "rules": rules,
        "experiments": experiments,
        "experiment_events": experiment_events,
        "research_threads": research_threads,
        "research_events": research_events,
        "active_experiment_count": sum(
            1 for row in experiments if row["status"] in {"planned", "running", "paused"}
        ),
        "active_research_count": sum(
            1 for row in research_threads if row["status"] in {"planned", "running", "paused"}
        ),
        "todos": todos,
        "todo_counts": dict(Counter(row["status"] for row in todos)),
        "open_todo_count": sum(1 for row in todos if row["status"] in {"open", "doing", "blocked"}),
        "latest_run": runs[0] if runs else None,
        "daily_summary": daily_summary_status(store, project),
    }


def derive_health(project, sessions, runs, todos, experiments, research_threads, git: dict[str, str]) -> dict[str, Any]:
    reasons: list[str] = []
    severity = "ok"
    label = "ok"

    if git["state"] == "missing":
        return {
            "label": "missing path",
            "severity": "danger",
            "reasons": ["Project path does not exist."],
        }

    latest_run = runs[0] if runs else None
    if latest_run and latest_run["status"] == "failed":
        label = "attention"
        severity = "danger"
        reasons.append("Latest run failed.")
    elif latest_run and latest_run["status"] == "running":
        label = "running"
        severity = "info"
        reasons.append("A run is currently recorded as running.")

    if not sessions:
        if severity == "ok":
            severity = "warning"
            label = "needs sync"
        reasons.append("No Codex sessions are stored for this project.")

    blocked = [todo for todo in todos if todo["status"] == "blocked"]
    if blocked:
        if severity == "ok":
            severity = "warning"
            label = "blocked"
        reasons.append(f"{len(blocked)} todo(s) are blocked.")

    running_experiments = [experiment for experiment in experiments if experiment["status"] == "running"]
    if running_experiments:
        if severity == "ok":
            severity = "info"
            label = "experiment running"
        reasons.append(f"{len(running_experiments)} experiment(s) are running.")

    running_research = [research for research in research_threads if research["status"] == "running"]
    if running_research:
        if severity == "ok":
            severity = "info"
            label = "research running"
        reasons.append(f"{len(running_research)} research thread(s) are running.")

    if git["state"] == "git" and int(git.get("dirty_count") or "0") > 0:
        reasons.append(f"Git worktree has {git['dirty_count']} changed file(s).")

    if git["state"] == "not-git":
        reasons.append("Project path is not a git repository.")

    if not reasons:
        reasons.append("No obvious issue in stored mycodex state.")

    return {"label": label, "severity": severity, "reasons": reasons}


def daily_summary_status(store: Store, project) -> dict[str, Any]:
    enabled = bool(project["daily_summary_enabled"])
    latest = None
    for run in store.list_runs(project["id"], limit=20):
        if run["kind"] == "daily_summary":
            latest = run
            break
    return {
        "enabled": enabled,
        "time": project["daily_summary_time"],
        "latest": latest,
        "next_due_text": next_daily_summary_text(project),
    }


def next_daily_summary_text(project) -> str:
    if not project["daily_summary_enabled"]:
        return "off"
    try:
        hour, minute = [int(part) for part in project["daily_summary_time"].split(":", 1)]
    except ValueError:
        return "invalid time"
    now = datetime.now()
    scheduled = datetime(now.year, now.month, now.day, hour, minute)
    if scheduled <= now:
        scheduled += timedelta(days=1)
    return scheduled.strftime("%Y-%m-%d %H:%M")


def rule_schedule_label(rule) -> str:
    if rule["time_of_day"]:
        return f"daily {rule['time_of_day']}"
    if rule["interval_seconds"]:
        return f"every {rule['interval_seconds']}s"
    return "manual"


def daemon_snapshot(db_path: Path) -> dict[str, Any]:
    pid_file = db_path.parent / "mycodex-daemon.pid"
    if not pid_file.exists():
        return {"pid": None, "running": False, "label": "unknown"}
    try:
        pid = int(pid_file.read_text().strip())
        os.kill(pid, 0)
    except (OSError, ValueError):
        return {"pid": None, "running": False, "label": "stopped"}
    return {"pid": pid, "running": True, "label": "running"}


def row_to_dict(row) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def snapshot_to_jsonable(value):
    if isinstance(value, dict):
        return {key: snapshot_to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [snapshot_to_jsonable(item) for item in value]
    if hasattr(value, "keys"):
        return {key: snapshot_to_jsonable(value[key]) for key in value.keys()}
    return value
