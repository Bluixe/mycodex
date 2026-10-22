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
from .token_usage import token_usage_snapshot
from .jobs import JobQueue
from .diagnostics import decision_snapshot


PACKAGE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PACKAGE_DIR / "templates"
STATIC_DIR = PACKAGE_DIR / "static"


def create_app(db_path: str | Path | None = None) -> FastAPI:
    resolved_db_path = resolve_db_path(db_path)
    app = FastAPI(title="mycodex dashboard")
    app.state.db_path = resolved_db_path
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.middleware("http")
    async def same_origin_mutations(request: Request, call_next):
        origin = request.headers.get("origin")
        if request.method in {"POST", "PUT", "PATCH", "DELETE"} and origin:
            from urllib.parse import urlparse
            if urlparse(origin).netloc != request.headers.get("host"):
                return JSONResponse({"error": "cross-origin mutations are not allowed"}, status_code=403)
        return await call_next(request)

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.filters["ts"] = format_ts
    templates.env.filters["schedule"] = rule_schedule_label
    templates.env.filters["priority"] = _priority_label
    templates.env.filters["compact_int"] = _compact_int
    templates.env.filters["format_int"] = _format_int

    def render(request: Request, template: str, context: dict[str, Any]) -> HTMLResponse:
        base_context = {
            "request": request,
            "db_path": str(resolved_db_path),
            "assets_version": "20260515-daily-summary-notify",
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

    @app.get("/usage", response_class=HTMLResponse)
    def usage(request: Request, days: int = 7):
        days = _bounded_days(days)
        with Store(resolved_db_path) as store:
            snapshot = token_usage_snapshot(store, days=days)
        return render(
            request,
            "usage.html",
            {
                "snapshot": snapshot,
                "page_title": "Token Usage",
                "refresh_interval_ms": 10000,
            },
        )

    @app.post("/projects/{project_name}/sync")
    def sync_project(project_name: str, background_tasks: BackgroundTasks):
        with Store(resolved_db_path) as store:
            JobQueue(store).submit(project_name, "sync_sessions", source="web")
        return _redirect("/jobs")

    @app.post("/projects/{project_name}/daily-summary")
    def daily_summary(project_name: str, background_tasks: BackgroundTasks):
        with Store(resolved_db_path) as store:
            JobQueue(store).submit(project_name, "daily_summary", source="web")
        return _redirect("/jobs")

    @app.post("/projects/{project_name}/send")
    async def send_project_prompt(
        project_name: str,
        request: Request,
        background_tasks: BackgroundTasks,
    ):
        form = await _read_form(request)
        prompt = form.get("prompt", "").strip()
        if prompt:
            with Store(resolved_db_path) as store:
                JobQueue(store).submit(project_name, "prompt",
                    {"prompt": prompt, "new_thread": form.get("new_thread") == "on"}, source="web")
        return _redirect("/jobs")

    @app.get("/jobs", response_class=HTMLResponse)
    def jobs_page(request: Request, project: str | None = None):
        with Store(resolved_db_path) as store:
            jobs = JobQueue(store).list(project)
            projects = [dict(row) for row in store.list_projects()]
            decisions = [decision_snapshot(store, row["name"]) for row in projects]
        return render(request, "jobs.html", {"jobs": jobs, "decisions": decisions,
            "page_title": "Tasks and decisions", "refresh_interval_ms": 0})

    @app.get("/api/jobs")
    def api_jobs(project: str | None = None):
        with Store(resolved_db_path) as store:
            return JSONResponse(JobQueue(store).list(project))

    @app.get("/api/jobs/{job_id}")
    def api_job(job_id: int):
        with Store(resolved_db_path) as store:
            queue = JobQueue(store)
            job = queue.get(job_id)
            return JSONResponse({"job": job, "events": queue.events(job_id)}, status_code=200 if job else 404)

    @app.post("/jobs/{job_id}/cancel")
    def cancel_job(job_id: int):
        with Store(resolved_db_path) as store:
            if JobQueue(store).get(job_id) is None:
                return JSONResponse({"error": "unknown job"}, status_code=404)
            JobQueue(store).cancel(job_id)
        return _redirect("/jobs")

    @app.post('/jobs/{job_id}/{action}')
    async def control_job(job_id: int, action: str, request: Request):
        if action not in {'pause','resume','revise'}:
            return JSONResponse({'error': 'unknown action'}, status_code=404)
        form = await _read_form(request)
        with Store(resolved_db_path) as store:
            queue = JobQueue(store)
            if queue.get(job_id) is None:
                return JSONResponse({'error': 'unknown job'}, status_code=404)
            try:
                if action == 'pause':
                    queue.pause(job_id)
                elif action == 'resume':
                    queue.resume(job_id, expected_revision=int(form['revision']) if 'revision' in form else None)
                else:
                    queue.revise(job_id, form.get('prompt'), expected_revision=int(form.get('revision', '-1')))
            except (ValueError, TypeError) as exc:
                return JSONResponse({'error': str(exc)}, status_code=409)
        return _redirect('/jobs')

    @app.get('/api/jobs/{job_id}/trace')
    def api_trace(job_id: int):
        from .tracing import job_trace
        with Store(resolved_db_path) as store:
            try:
                return JSONResponse(job_trace(store, job_id))
            except ValueError as exc:
                return JSONResponse({'error': str(exc)}, status_code=404)

    @app.get('/api/reviews/{workflow_id}')
    def api_review(workflow_id: int):
        from .review_workflow import workflow_report
        with Store(resolved_db_path) as store:
            try:
                return JSONResponse(workflow_report(store, workflow_id))
            except ValueError as exc:
                return JSONResponse({'error': str(exc)}, status_code=404)

    @app.get("/api/projects/{project_name}/decisions")
    def api_decisions(project_name: str):
        with Store(resolved_db_path) as store:
            if store.get_project(project_name) is None:
                return JSONResponse({"error": "unknown project"}, status_code=404)
            return JSONResponse(decision_snapshot(store, project_name))

    @app.get("/api/projects/{project_name}/research/{research_name}/evidence")
    def api_evidence(project_name: str, research_name: str, query: str | None = None):
        from .research_evidence import EvidenceLedger
        with Store(resolved_db_path) as store:
            project = store.get_project(project_name)
            research = store.get_research_thread(project["id"], research_name) if project else None
            if research is None:
                return JSONResponse({"error": "unknown research"}, status_code=404)
            ledger = EvidenceLedger(store)
            return JSONResponse({"passages": ledger.search_evidence(research["id"], query or research["question"]),
                                 "links": ledger.list_evidence(research["id"]),
                                 "coverage": ledger.list_search_receipts(research["id"])})

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

    @app.get("/api/projects/{project_name}/experiments")
    def api_experiments(project_name: str):
        with Store(resolved_db_path) as store:
            project = store.get_project(project_name)
            if project is None:
                return JSONResponse({"error": "not found"}, status_code=404)
            experiments = store.list_experiments(project["id"])
        return JSONResponse(snapshot_to_jsonable(experiments))

    @app.get("/api/projects/{project_name}/experiments/{experiment_name}")
    def api_experiment(project_name: str, experiment_name: str, limit: int = 20):
        with Store(resolved_db_path) as store:
            project = store.get_project(project_name)
            if project is None:
                return JSONResponse({"error": "not found"}, status_code=404)
            experiment = store.get_experiment(project["id"], experiment_name)
            if experiment is None:
                return JSONResponse({"error": "not found"}, status_code=404)
            events = store.list_experiment_events(experiment["id"], limit=max(1, min(limit, 100)))
        return JSONResponse(
            {
                "experiment": snapshot_to_jsonable(experiment),
                "events": snapshot_to_jsonable(events),
            }
        )

    @app.get("/api/projects/{project_name}/research")
    def api_research_threads(project_name: str):
        with Store(resolved_db_path) as store:
            project = store.get_project(project_name)
            if project is None:
                return JSONResponse({"error": "not found"}, status_code=404)
            threads = store.list_research_threads(project["id"])
        return JSONResponse(snapshot_to_jsonable(threads))

    @app.get("/api/projects/{project_name}/research/{research_name}")
    def api_research_thread(project_name: str, research_name: str, limit: int = 20):
        with Store(resolved_db_path) as store:
            project = store.get_project(project_name)
            if project is None:
                return JSONResponse({"error": "not found"}, status_code=404)
            research = store.get_research_thread(project["id"], research_name)
            if research is None:
                return JSONResponse({"error": "not found"}, status_code=404)
            limit = max(1, min(limit, 100))
            events = store.list_research_events(research["id"], limit=limit)
            sources = store.list_research_sources(research["id"], limit=limit)
            claims = store.list_research_claims(research["id"], limit=limit)
            ideas = store.list_research_ideas(research["id"], limit=limit)
            tasks = store.list_research_tasks(research["id"], limit=limit)
        return JSONResponse(
            {
                "research": snapshot_to_jsonable(research),
                "events": snapshot_to_jsonable(events),
                "sources": snapshot_to_jsonable(sources),
                "claims": snapshot_to_jsonable(claims),
                "ideas": snapshot_to_jsonable(ideas),
                "tasks": snapshot_to_jsonable(tasks),
            }
        )

    @app.post("/api/projects/{project_name}/research/{research_name}/events")
    async def api_research_event(project_name: str, research_name: str, request: Request):
        data = await request.json()
        with Store(resolved_db_path) as store:
            project = store.get_project(project_name)
            if project is None:
                return JSONResponse({"error": "not found"}, status_code=404)
            research = store.get_research_thread(project["id"], research_name)
            if research is None:
                return JSONResponse({"error": "not found"}, status_code=404)
            event_id = store.add_research_event(
                research["id"],
                event_type=str(data.get("event_type") or data.get("type") or ""),
                iteration=_coerce_optional_int(data.get("iteration")),
                level=str(data.get("level") or "info"),
                message=str(data.get("message") or ""),
                payload=data.get("payload") if isinstance(data.get("payload"), dict) else {},
                source=str(data.get("source") or "api"),
                run_id=_coerce_optional_int(data.get("run_id")),
                status=data.get("status"),
                phase=data.get("phase"),
                phase_message=data.get("phase_message"),
            )
        return JSONResponse({"event_id": event_id})

    @app.post("/api/projects/{project_name}/experiments/{experiment_name}/events")
    async def api_experiment_event(project_name: str, experiment_name: str, request: Request):
        data = await request.json()
        with Store(resolved_db_path) as store:
            project = store.get_project(project_name)
            if project is None:
                return JSONResponse({"error": "not found"}, status_code=404)
            experiment = store.get_experiment(project["id"], experiment_name)
            if experiment is None:
                return JSONResponse({"error": "not found"}, status_code=404)
            event_id = store.add_experiment_event(
                experiment["id"],
                event_type=str(data.get("event_type") or data.get("type") or ""),
                iteration=_coerce_optional_int(data.get("iteration")),
                level=str(data.get("level") or "info"),
                message=str(data.get("message") or ""),
                payload=data.get("payload") if isinstance(data.get("payload"), dict) else {},
                source=str(data.get("source") or "api"),
                run_id=_coerce_optional_int(data.get("run_id")),
                status=data.get("status"),
                phase=data.get("phase"),
                phase_message=data.get("phase_message"),
            )
        return JSONResponse({"event_id": event_id})

    @app.get("/api/usage")
    def api_usage(days: int = 7):
        days = _bounded_days(days)
        with Store(resolved_db_path) as store:
            return JSONResponse(token_usage_snapshot(store, days=days))

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


def _compact_int(value: int | str | None) -> str:
    number = _coerce_int(value)
    if abs(number) >= 1_000_000_000:
        return f"{number / 1_000_000_000:.1f}B"
    if abs(number) >= 1_000_000:
        return f"{number / 1_000_000:.1f}M"
    if abs(number) >= 10_000:
        return f"{number / 1_000:.1f}K"
    return f"{number:,}"


def _format_int(value: int | str | None) -> str:
    return f"{_coerce_int(value):,}"


def _coerce_int(value: int | str | None) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _coerce_optional_int(value: int | str | None) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bounded_days(days: int | str | None) -> int:
    try:
        value = int(days or 7)
    except (TypeError, ValueError):
        value = 7
    return max(1, min(value, 30))
