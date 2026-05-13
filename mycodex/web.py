from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .db import Store, resolve_db_path
from .service import format_ts, run_daily_summary, send_prompt, sync_sessions
from .status import (
    dashboard_snapshot,
    project_snapshot,
    rule_schedule_label,
    snapshot_to_jsonable,
)


PACKAGE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PACKAGE_DIR / "templates"
STATIC_DIR = PACKAGE_DIR / "static"


def create_app(db_path: str | Path | None = None) -> FastAPI:
    resolved_db_path = resolve_db_path(db_path)
    app = FastAPI(title="mycodex dashboard")
    app.state.db_path = resolved_db_path
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.filters["ts"] = format_ts
    templates.env.filters["schedule"] = rule_schedule_label
    templates.env.filters["priority"] = _priority_label

    def render(request: Request, template: str, context: dict[str, Any]) -> HTMLResponse:
        base_context = {
            "request": request,
            "db_path": str(resolved_db_path),
        }
        base_context.update(context)
        return templates.TemplateResponse(request, template, base_context)

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        with Store(resolved_db_path) as store:
            snapshot = snapshot_to_jsonable(dashboard_snapshot(store))
        return render(
            request,
            "index.html",
            {
                "snapshot": snapshot,
                "page_title": "Projects",
                "refresh_interval_ms": 20000,
            },
        )

    @app.get("/projects/{project_name}", response_class=HTMLResponse)
    def project_detail(request: Request, project_name: str):
        with Store(resolved_db_path) as store:
            snapshot = snapshot_to_jsonable(project_snapshot(store, project_name))
        return render(
            request,
            "project.html",
            {
                "snapshot": snapshot,
                "page_title": project_name,
                "refresh_interval_ms": 15000,
            },
        )

    @app.get("/runs/{run_id}", response_class=HTMLResponse)
    def run_detail(request: Request, run_id: int):
        with Store(resolved_db_path) as store:
            run = store.get_run(run_id)
        if run is None:
            return render(
                request,
                "not_found.html",
                {"page_title": "Run not found", "message": f"Run #{run_id} was not found."},
            )
        return render(
            request,
            "run.html",
            {
                "run": snapshot_to_jsonable(run),
                "page_title": f"Run #{run_id}",
                "refresh_interval_ms": 5000 if run["status"] == "running" else 0,
            },
        )

    @app.post("/projects/{project_name}/sync")
    def sync_project(project_name: str, background_tasks: BackgroundTasks):
        background_tasks.add_task(_sync_project_task, resolved_db_path, project_name)
        return _redirect(f"/projects/{project_name}")

    @app.post("/projects/{project_name}/daily-summary")
    def daily_summary(project_name: str, background_tasks: BackgroundTasks):
        background_tasks.add_task(_daily_summary_task, resolved_db_path, project_name)
        return _redirect(f"/projects/{project_name}")

    @app.post("/projects/{project_name}/send")
    async def send_project_prompt(
        project_name: str,
        request: Request,
        background_tasks: BackgroundTasks,
    ):
        form = await _read_form(request)
        prompt = form.get("prompt", "").strip()
        if prompt:
            background_tasks.add_task(
                _send_prompt_task,
                resolved_db_path,
                project_name,
                prompt,
                form.get("new_thread") == "on",
            )
        return _redirect(f"/projects/{project_name}")

    @app.post("/projects/{project_name}/todos")
    async def create_todo(project_name: str, request: Request):
        form = await _read_form(request)
        with Store(resolved_db_path) as store:
            project = store.get_project(project_name)
            if project is None:
                return _redirect("/")
            store.add_todo(
                project["id"],
                title=form.get("title", ""),
                body=form.get("body", ""),
                priority=int(form.get("priority") or "2"),
            )
        return _redirect(f"/projects/{project_name}#todos")

    @app.post("/todos/{todo_id}/status")
    async def update_todo_status(todo_id: int, request: Request):
        form = await _read_form(request)
        with Store(resolved_db_path) as store:
            todo = store.get_todo(todo_id)
            if todo is not None:
                store.update_todo_status(todo_id, form.get("status", "open"))
                return _redirect(f"/projects/{todo['project_name']}#todos")
        return _redirect("/")

    @app.get("/api/projects")
    def api_projects():
        with Store(resolved_db_path) as store:
            return JSONResponse(snapshot_to_jsonable(dashboard_snapshot(store)))

    @app.get("/api/projects/{project_name}")
    def api_project(project_name: str):
        with Store(resolved_db_path) as store:
            return JSONResponse(snapshot_to_jsonable(project_snapshot(store, project_name)))

    @app.get("/api/runs/{run_id}")
    def api_run(run_id: int):
        with Store(resolved_db_path) as store:
            run = store.get_run(run_id)
        if run is None:
            return JSONResponse({"error": "not found"}, status_code=404)
        return JSONResponse(snapshot_to_jsonable(run))

    return app


def run_web(
    *,
    db_path: str | Path | None = None,
    host: str = "127.0.0.1",
    port: int = 8765,
    reload: bool = False,
) -> None:
    import uvicorn

    if reload:
        os.environ["MYCODEX_DB"] = str(resolve_db_path(db_path))
        uvicorn.run("mycodex.web:create_app", factory=True, host=host, port=port, reload=True)
        return
    app = create_app(db_path)
    uvicorn.run(app, host=host, port=port)


async def _read_form(request: Request) -> dict[str, str]:
    body = (await request.body()).decode("utf-8")
    parsed = parse_qs(body, keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}


def _redirect(path: str) -> RedirectResponse:
    return RedirectResponse(path, status_code=303)


def _sync_project_task(db_path: Path, project_name: str) -> None:
    with Store(db_path) as store:
        project = store.get_project(project_name)
        if project is None:
            return
        run_id = store.add_run(
            project["id"],
            kind="sync_sessions",
            prompt="Sync Codex sessions for dashboard",
        )
        try:
            threads = sync_sessions(store, project_name)
        except Exception as exc:
            store.finish_run(run_id, status="failed", error=str(exc))
            print(f"mycodex web sync failed for {project_name}: {exc}", file=sys.stderr)
            return
        store.finish_run(
            run_id,
            status="completed",
            output_text=f"Synced {len(threads)} session(s).",
        )


def _daily_summary_task(db_path: Path, project_name: str) -> None:
    with Store(db_path) as store:
        try:
            run_daily_summary(store, project_name)
        except Exception as exc:
            print(f"mycodex web daily summary failed for {project_name}: {exc}", file=sys.stderr)


def _send_prompt_task(
    db_path: Path,
    project_name: str,
    prompt: str,
    new_thread: bool,
) -> None:
    with Store(db_path) as store:
        try:
            send_prompt(store, project_name, prompt, new_thread=new_thread)
        except Exception as exc:
            print(f"mycodex web send failed for {project_name}: {exc}", file=sys.stderr)


def _priority_label(priority: int | str | None) -> str:
    labels = {1: "high", 2: "normal", 3: "low"}
    try:
        return labels.get(int(priority or 2), "normal")
    except (TypeError, ValueError):
        return "normal"
