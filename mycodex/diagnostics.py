from __future__ import annotations

import importlib.util
import os
import platform
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path


def doctor_snapshot() -> dict:
    """Check availability without reading credentials or making paid requests."""
    checks = []
    checks.append({"name": "python", "available": sys.version_info >= (3, 11), "detail": platform.python_version()})
    for command, required in (("codex", True), ("git", True), ("ssh", False), ("pdftotext", False)):
        if command == "codex":
            from .codex_app import _resolve_codex_bin
            resolved = _resolve_codex_bin("codex")
            executable = shutil.which(resolved)
        else:
            executable = shutil.which(command)
        item = {"name": command, "available": bool(executable), "required": required}
        if command == "codex":
            item.update(executable=executable, path_alternative=shutil.which("codex"))
        if executable:
            try:
                args = [executable, "--version"] if command != "ssh" else [executable, "-V"]
                result = subprocess.run(args, capture_output=True, text=True, timeout=10,
                                        shell=executable.lower().endswith((".cmd", ".bat")))
                item["detail"] = (result.stdout or result.stderr).splitlines()[0][:200]
            except (OSError, subprocess.SubprocessError, IndexError) as exc:
                item["detail"] = type(exc).__name__
        checks.append(item)
    for package in ("fastapi", "uvicorn", "jinja2", "openai_codex"):
        checks.append({"name": package, "available": importlib.util.find_spec(package) is not None,
                       "required": package != "openai_codex"})
    for name in ("OPENALEX_API_KEY", "SEMANTIC_SCHOLAR_API_KEY", "MYCODEX_TELEGRAM_BOT_TOKEN"):
        checks.append({"name": name, "available": bool(os.environ.get(name)), "required": False,
                       "detail": "configured" if os.environ.get(name) else "not configured"})
    return {"platform": platform.platform(), "sqlite": sqlite3.sqlite_version,
            "checks": checks, "ready": all(row["available"] for row in checks if row.get("required")),
            "notes": ["Codex authentication/config/model entitlement is not tested by this version-only local check.",
                      "Use MYCODEX_CODEX_BIN to select an explicit runtime. A desktop CODEX_CLI_PATH takes precedence over PATH; global configuration is never rewritten.",
                      "LaunchAgent is macOS-only; run worker under your OS service manager."]}


def decision_snapshot(store, project_name: str) -> dict:
    from .jobs import JobQueue
    project = store.get_project(project_name)
    if project is None:
        raise ValueError(f"unknown project: {project_name}")
    jobs = JobQueue(store).list(project_name, limit=100)
    active = [job for job in jobs if job["status"] in {"queued", "leased", "running"}]
    attention = [job for job in jobs if job["status"] in {"blocked", "failed"}]
    paused = [job for job in jobs if job['status'] == 'paused']
    from .review_workflow import list_workflow_reports
    experiments = [dict(row) for row in store.list_experiments(project["id"])]
    research = [dict(row) for row in store.list_research_threads(project["id"])]
    return {"project": project_name, "active_jobs": active, "needs_attention": attention,
            "experiments": experiments, "research": research, 'paused_jobs': paused,
            'review_workflows': list_workflow_reports(store, project['id']),
            "next_action": "Inspect failed/blocked job evidence before retrying." if attention
                           else "Run mycodex worker to consume queued work." if active
                           else 'Review the saved goal and remaining budget, then resume paused work.' if paused
                           else "No queued work. Submit a bounded task.",
            "limits": "Budget and evidence restrictions are shown per task; no cost estimate is fabricated."}
