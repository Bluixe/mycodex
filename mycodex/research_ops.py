from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .db import Store
from .research_connectors import ingest_text_source, search_public_sources
from .research_quality import (
    build_concept_preflight,
    coverage_summary,
    critique_idea,
    expand_queries,
    score_source,
    select_sources_for_deep_read,
    synthesize_ledgers,
    threat_queries_for_idea,
)


_JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.IGNORECASE | re.DOTALL)
_CONCEPT_PREFLIGHT_KIND = "concept_preflight"

Searcher = Callable[..., list[Any]]
Ingester = Callable[..., dict[str, Any]]


def extract_research_contract(text: str) -> dict[str, Any] | None:
    blocks = [match.group(1) for match in _JSON_BLOCK_RE.finditer(text or "")]
    candidates = list(reversed(blocks))
    stripped = (text or "").strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        candidates.append(stripped)
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and (
            "research_question" in parsed
            or "next_action" in parsed
            or "idea_ledger_delta" in parsed
            or "concept_preflight" in parsed
        ):
            return parsed
    return None


def ensure_research_concept_preflight_task(
    store: Store,
    project_name: str,
    research_name: str,
    *,
    auto_queue_search: bool = True,
) -> int | None:
    project = _require_project(store, project_name)
    research = store.get_research_thread(project["id"], research_name)
    if research is None:
        raise ValueError(f"unknown research {research_name!r} for project {project_name!r}")
    return _ensure_concept_preflight_task(
        store,
        research["id"],
        question=str(research["question"] or ""),
        auto_queue_search=auto_queue_search,
    )


def apply_research_contract(
    store: Store,
    project_name: str,
    research_name: str,
    contract: dict[str, Any],
    *,
    run_id: int | None = None,
) -> dict[str, Any]:
    project = _require_project(store, project_name)
    research = store.get_research_thread(project["id"], research_name)
    if research is None:
        raise ValueError(f"unknown research {research_name!r} for project {project_name!r}")

    source_count = 0
    claim_count = 0
    idea_count = 0
    task_count = 0
    preflight_count = 0

    contract_preflight = _concept_preflight_from_contract(contract)
    if contract_preflight:
        _record_concept_preflight(
            store,
            research,
            contract_preflight,
            source="control",
            message="Research concept preflight recorded from control contract.",
        )
        preflight_count = 1

    for source in _list_of_dicts(contract.get("source_ledger_delta")):
        title = str(source.get("title") or source.get("name") or "").strip()
        url = str(source.get("url") or source.get("link") or "").strip()
        if not title and not url:
            continue
        keep = source.get("keep")
        status = str(source.get("status") or ("kept" if keep is True else "rejected" if keep is False else "candidate"))
        store.upsert_research_source(
            research["id"],
            title=title or url,
            url=url,
            source_type=str(source.get("source_type") or source.get("type") or ""),
            role=str(source.get("role") or ""),
            depth=str(source.get("depth") or "title"),
            status=status,
            relevance=_int_or_zero(source.get("relevance")),
            evidence_quality=_int_or_zero(source.get("evidence_quality")),
            notes=str(source.get("notes") or source.get("weakness") or ""),
            metadata=source,
        )
        source_count += 1

    for claim in _list_of_dicts(contract.get("claim_ledger_delta")):
        text = str(claim.get("claim") or "").strip()
        if not text:
            continue
        store.add_research_claim(
            research["id"],
            claim=text,
            status=str(claim.get("status") or "uncertain"),
            support=str(claim.get("support") or ""),
            confidence=_int_or_zero(claim.get("confidence")),
            notes=str(claim.get("notes") or ""),
            metadata=claim,
        )
        claim_count += 1

    for idea in _list_of_dicts(contract.get("idea_ledger_delta")):
        title = str(idea.get("idea") or idea.get("title") or "").strip()
        if not title:
            continue
        kill = idea.get("kill_criteria")
        if isinstance(kill, list):
            kill_text = "\n".join(str(item) for item in kill)
        else:
            kill_text = str(kill or "")
        store.add_research_idea(
            research["id"],
            title=title,
            hypothesis=str(idea.get("hypothesis") or ""),
            mechanism=str(idea.get("mechanism") or ""),
            status=_normalize_idea_status(idea.get("status")),
            score=idea.get("score") if isinstance(idea.get("score"), dict) else {},
            minimal_test=str(idea.get("minimal_test") or ""),
            kill_criteria=kill_text,
            notes=str(idea.get("notes") or ""),
            metadata=idea,
        )
        idea_count += 1

    next_action = contract.get("next_action") if isinstance(contract.get("next_action"), dict) else {}
    if next_action:
        queries = next_action.get("queries") if isinstance(next_action.get("queries"), list) else []
        query = "\n".join(str(item) for item in queries) if queries else ""
        store.add_research_task(
            research["id"],
            task_type=str(next_action.get("type") or "research"),
            title=str(next_action.get("reason") or ""),
            query=query,
            status="queued" if bool(contract.get("auto_continue")) else "blocked",
            payload=next_action,
        )
        task_count += 1

    phase = str(contract.get("phase") or "waiting_for_decision")
    decision = _decision_message(contract, source_count, claim_count, idea_count, task_count)
    store.add_research_event(
        research["id"],
        event_type="contract_parsed",
        level="info",
        message=decision,
        payload={
            "contract": contract,
            "sources": source_count,
            "claims": claim_count,
            "ideas": idea_count,
            "tasks": task_count,
            "concept_preflight": preflight_count,
        },
        source="mycodex",
        run_id=run_id,
        phase=phase,
        phase_message=decision,
    )
    if contract.get("auto_continue") is False or next_action.get("type") == "ask_user":
        store.add_research_event(
            research["id"],
            event_type="decision",
            level="info",
            message=str(next_action.get("reason") or "Research control turn is waiting for user decision."),
            payload={"next_action": next_action, "ask_user_if": contract.get("ask_user_if") or []},
            source="mycodex",
            phase="waiting_for_decision",
        )
    _try_write_research_markdown(store, project_name, research_name)
    return {
        "sources": source_count,
        "claims": claim_count,
        "ideas": idea_count,
        "tasks": task_count,
        "concept_preflight": preflight_count,
        "auto_continue": bool(contract.get("auto_continue")),
        "phase": phase,
    }


def build_research_control_context(
    store: Store,
    project_name: str,
    research_name: str,
    *,
    source_limit: int = 14,
    claim_limit: int = 8,
    idea_limit: int = 8,
    task_limit: int = 10,
    event_limit: int = 8,
) -> str:
    project = _require_project(store, project_name)
    research = store.get_research_thread(project["id"], research_name)
    if research is None:
        raise ValueError(f"unknown research {research_name!r} for project {project_name!r}")
    sources = [_row_to_source_dict(row) for row in store.list_research_sources(research["id"], limit=source_limit)]
    claims = [_row_to_claim_dict(row) for row in store.list_research_claims(research["id"], limit=claim_limit)]
    ideas = [_row_to_idea_dict(row) for row in store.list_research_ideas(research["id"], limit=idea_limit)]
    tasks = [_row_to_task_dict(row) for row in store.list_research_tasks(research["id"], limit=task_limit)]
    events = [_row_to_event_dict(row) for row in store.list_research_events(research["id"], limit=event_limit)]
    context = {
        "research": {
            "id": research["id"],
            "name": research["name"],
            "status": research["status"],
            "phase": research["phase"],
            "phase_message": research["phase_message"],
            "latest_decision": research["latest_decision"],
            "current_iteration": research["current_iteration"],
            "max_iterations": research["max_iterations"],
        },
        "concept_preflight": _latest_concept_preflight(store, research["id"]),
        "sources": sources,
        "claims": claims,
        "ideas": ideas,
        "tasks": tasks,
        "events": events,
    }
    compact_context = _compact_for_control_context(context)
    return json.dumps(compact_context, ensure_ascii=False, indent=2)


def find_research_threads(
    store: Store,
    project_name: str,
    query: str,
    *,
    limit: int = 5,
    include_archived: bool = False,
) -> list[dict[str, Any]]:
    project = _require_project(store, project_name)
    return store.search_research_threads(
        project["id"],
        query,
        limit=limit,
        include_archived=include_archived,
    )


def update_research_from_query(
    store: Store,
    project_name: str,
    query: str,
    *,
    research_name: str | None = None,
    match_limit: int = 5,
    max_cycles: int = 5,
    interval: int = 300,
    trigger_control: bool = True,
    control_timeout: int = 1800,
    sandbox: str | None = None,
    approval_policy: str | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
    providers: list[str] | None = None,
    limit: int = 5,
    task_timeout: int = 60,
    max_chars: int = 12000,
    markdown_dir: str | Path | None = None,
) -> dict[str, Any]:
    project = _require_project(store, project_name)
    matches: list[dict[str, Any]] = []
    if research_name:
        research = store.get_research_thread(project["id"], research_name)
        if research is None:
            raise ValueError(f"unknown research {research_name!r} for project {project_name!r}")
    else:
        matches = store.search_research_threads(project["id"], query, limit=match_limit)
        if not matches:
            raise ValueError(
                f"no existing research matched {query!r}; run `mycodex research create` first"
            )
        research = matches[0]["research"]
        research_name = research["name"]

    preflight_task_id = _ensure_concept_preflight_task(
        store,
        research["id"],
        question=str(research["question"] or query),
        auto_queue_search=False,
    )
    if preflight_task_id:
        store.add_research_event(
            research["id"],
            event_type="task_queued",
            level="info",
            message=f"Queued concept preflight before incremental update for: {query}",
            payload={"task_id": preflight_task_id, "query": query},
            source="mycodex",
            status="running",
            phase="concept_preflight",
        )

    task_id = store.add_research_task(
        research["id"],
        task_type="search",
        title=f"Incremental update: {query}",
        query=query,
        payload={
            "incremental": True,
            "auto_synthesize": True,
            "auto_deep_read": True,
            "auto_refine": True,
            "matched_query": query,
        },
    )
    store.add_research_event(
        research["id"],
        event_type="task_queued",
        level="info",
        message=f"Queued incremental research update for: {query}",
        payload={"task_id": task_id, "query": query},
        source="mycodex",
        status="running",
        phase="queued",
    )

    supervisor = None
    if max_cycles > 0:
        supervisor = supervise_research(
            store,
            project_name,
            str(research_name),
            max_cycles=max_cycles,
            interval=interval,
            trigger_control=trigger_control,
            control_timeout=control_timeout,
            sandbox=sandbox,
            approval_policy=approval_policy,
            model=model,
            reasoning_effort=reasoning_effort,
            providers=providers,
            limit=limit,
            task_timeout=task_timeout,
            max_chars=max_chars,
        )

    markdown_path = write_research_markdown(
        store,
        project_name,
        str(research_name),
        markdown_dir=markdown_dir,
    )
    return {
        "research_id": research["id"],
        "project": project_name,
        "name": str(research_name),
        "task_id": task_id,
        "preflight_task_id": preflight_task_id,
        "matches": matches,
        "supervisor": supervisor,
        "markdown_path": str(markdown_path),
    }


def research_markdown_path(
    store: Store,
    project_name: str,
    research_name: str,
    *,
    markdown_dir: str | Path | None = None,
) -> Path:
    base = Path(markdown_dir).expanduser() if markdown_dir else store.path.parent / "research_markdown"
    return base / _safe_filename(project_name) / f"{_safe_filename(research_name)}.md"


def write_research_markdown(
    store: Store,
    project_name: str,
    research_name: str,
    *,
    markdown_dir: str | Path | None = None,
) -> Path:
    path = research_markdown_path(store, project_name, research_name, markdown_dir=markdown_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_research_markdown(store, project_name, research_name), encoding="utf-8")
    return path


def render_research_markdown(store: Store, project_name: str, research_name: str) -> str:
    project = _require_project(store, project_name)
    research = store.get_research_thread(project["id"], research_name)
    if research is None:
        raise ValueError(f"unknown research {research_name!r} for project {project_name!r}")
    sources = store.list_research_sources(research["id"], limit=80)
    claims = store.list_research_claims(research["id"], limit=80)
    ideas = store.list_research_ideas(research["id"], limit=80)
    tasks = store.list_research_tasks(research["id"], limit=40)
    events = store.list_research_events(research["id"], limit=30)
    concept_preflight = _latest_concept_preflight(store, research["id"])

    lines = [
        f"# Research: {_md_escape(research['name'])}",
        "",
        "> Generated from `mycodex` SQLite ledgers. SQLite is the source of truth; this Markdown is an auxiliary snapshot.",
        "",
        "## State",
        "",
        f"- Project: `{_md_escape(project_name)}`",
        f"- Status: `{_md_escape(research['status'])}`",
        f"- Phase: `{_md_escape(research['phase'])}`",
        f"- Iteration: `{research['current_iteration']}/{research['max_iterations'] or '-'}`",
        f"- Thread: `{_md_escape(research['thread_id'])}`",
        f"- Updated: `{_format_ts(research['updated_at'])}`",
        f"- Question: {_md_escape(research['question'] or '-')}",
        "",
        "## Latest Decision",
        "",
        _md_escape(research["latest_decision"] or research["phase_message"] or "-"),
        "",
        "## Concept Preflight",
        "",
    ]
    if concept_preflight:
        lines.extend(_render_concept_preflight_markdown(concept_preflight))
    else:
        lines.append("- Missing: run `mycodex research run-task` on a `concept_preflight` task before ranking ideas.")
    lines.extend(
        [
            "",
        "## Sources",
        "",
        "| ID | Status | Role | Depth | Score | Title |",
        "|---|---|---|---|---:|---|",
        ]
    )
    lines.extend(
        f"| {row['id']} | {_md_escape(row['status'])} | {_md_escape(row['role'])} | "
        f"{_md_escape(row['depth'])} | {row['relevance']}/{row['evidence_quality']} | "
        f"{_md_source_link(row)} |"
        for row in sources
    )
    if not sources:
        lines.append("| - | - | - | - | - | - |")
    lines.extend(
        [
            "",
            "## Claims",
            "",
            "| ID | Status | Confidence | Claim | Support |",
            "|---|---|---:|---|---|",
        ]
    )
    lines.extend(
        f"| {row['id']} | {_md_escape(row['status'])} | {row['confidence']} | "
        f"{_md_escape(_truncate(row['claim'], 220))} | {_md_escape(_truncate(row['support'], 120))} |"
        for row in claims
    )
    if not claims:
        lines.append("| - | - | - | - | - |")
    lines.extend(
        [
            "",
            "## Ideas",
            "",
            "| ID | Status | Idea | Minimal Test | Kill Criteria |",
            "|---|---|---|---|---|",
        ]
    )
    lines.extend(
        f"| {row['id']} | {_md_escape(row['status'])} | {_md_escape(row['title'])} | "
        f"{_md_escape(_truncate(row['minimal_test'], 180))} | {_md_escape(_truncate(row['kill_criteria'], 180))} |"
        for row in ideas
    )
    if not ideas:
        lines.append("| - | - | - | - | - |")
    lines.extend(
        [
            "",
            "## Tasks",
            "",
            "| ID | Status | Type | Target |",
            "|---|---|---|---|",
        ]
    )
    lines.extend(
        f"| {row['id']} | {_md_escape(row['status'])} | {_md_escape(row['task_type'])} | "
        f"{_md_escape(_truncate(row['title'] or row['query'], 180))} |"
        for row in tasks
    )
    if not tasks:
        lines.append("| - | - | - | - |")
    lines.extend(
        [
            "",
            "## Recent Events",
            "",
            "| Time | Level | Type | Message |",
            "|---|---|---|---|",
        ]
    )
    lines.extend(
        f"| {_format_ts(row['created_at'])} | {_md_escape(row['level'])} | "
        f"{_md_escape(row['event_type'])} | {_md_escape(_truncate(row['message'], 220))} |"
        for row in events
    )
    if not events:
        lines.append("| - | - | - | - |")
    lines.append("")
    return "\n".join(lines)


def run_research_task(
    store: Store,
    project_name: str,
    research_name: str,
    *,
    task_id: int | None = None,
    providers: list[str] | None = None,
    limit: int = 5,
    timeout: int = 60,
    max_chars: int = 12000,
    allow_control: bool = True,
    searcher: Searcher | None = None,
    ingester: Ingester | None = None,
) -> dict[str, Any]:
    project = _require_project(store, project_name)
    research = store.get_research_thread(project["id"], research_name)
    if research is None:
        raise ValueError(f"unknown research {research_name!r} for project {project_name!r}")
    task = (
        store.get_research_task(research["id"], task_id)
        if task_id is not None
        else store.next_research_task(research["id"])
    )
    if task is None:
        store.add_research_event(
            research["id"],
            event_type="supervisor_idle",
            level="info",
            message="No queued research task is available.",
            source="mycodex",
            phase="idle",
        )
        return {"status": "idle", "task_id": None}
    if task["status"] not in {"queued", "running"}:
        raise ValueError(f"research task #{task['id']} is {task['status']}, not queued")

    payload = _json_dict(task["payload_json"])
    task_type = str(task["task_type"] or "").strip().lower()
    inline_preflight: dict[str, Any] | None = None
    if task_type != _CONCEPT_PREFLIGHT_KIND and _task_requires_concept_preflight(task_type, payload):
        inline_preflight = _ensure_concept_preflight_recorded(
            store,
            research,
            payload,
            source="mycodex",
            message=f"Concept preflight auto-recorded before research task #{task['id']} ({task_type}).",
        )
    store.update_research_task(task["id"], status="running")
    store.add_research_event(
        research["id"],
        event_type="task_started",
        level="info",
        message=f"Running research task #{task['id']}: {task_type}.",
        payload={"task_id": task["id"], "task_type": task_type, "payload": payload},
        source="mycodex",
        status="running",
        phase=_phase_for_task(task_type),
    )

    try:
        if task_type == _CONCEPT_PREFLIGHT_KIND:
            result = _run_concept_preflight_task(
                store,
                research,
                task,
                payload,
            )
        elif task_type == "triage" and _triage_requires_control(task, payload):
            if not allow_control:
                raise RuntimeError(f"task type {task_type!r} requires a Codex control turn")
            result = _run_control_task(
                store,
                project_name,
                research_name,
                task,
                timeout=timeout,
            )
        elif task_type in {"search", "source_search", "literature_search", "triage"}:
            result = _run_search_like_task(
                store,
                research,
                task,
                payload,
                providers=providers,
                limit=limit,
                timeout=timeout,
                searcher=searcher or search_public_sources,
            )
        elif task_type in {"deep_read", "read", "paper_read", "ingest"}:
            result = _run_deep_read_task(
                store,
                research,
                task,
                payload,
                timeout=timeout,
                max_chars=max_chars,
                ingester=ingester or ingest_text_source,
            )
        elif task_type in {"deep_read_top", "read_top", "paper_read_top"}:
            result = _run_deep_read_top_task(
                store,
                research,
                task,
                payload,
                timeout=timeout,
                max_chars=max_chars,
                ingester=ingester or ingest_text_source,
            )
        elif task_type in {"threat_search", "nearest_threat_search"}:
            result = _run_threat_search_task(
                store,
                research,
                task,
                payload,
                providers=providers,
                limit=limit,
                timeout=timeout,
                searcher=searcher or search_public_sources,
            )
        elif task_type in {"idea_critique", "critique_ideas"}:
            result = _run_idea_critique_task(
                store,
                research,
                task,
                payload,
            )
        elif task_type in {"synthesize", "rank_ideas", "idea_tournament", "local_synthesize", "local_idea_tournament"}:
            if not bool(payload.get("use_control")):
                result = _run_local_synthesis_task(
                    store,
                    research,
                    task,
                    payload,
                )
            elif not allow_control:
                raise RuntimeError(f"task type {task_type!r} requires a Codex control turn")
            else:
                result = _run_control_task(
                    store,
                    project_name,
                    research_name,
                    task,
                    timeout=timeout,
                )
        elif task_type == "design_experiment":
            if not allow_control:
                raise RuntimeError(f"task type {task_type!r} requires a Codex control turn")
            result = _run_control_task(
                store,
                project_name,
                research_name,
                task,
                timeout=timeout,
            )
        elif task_type == "ask_user":
            result = {"status": "blocked", "reason": payload.get("reason") or task["title"]}
            store.update_research_task(task["id"], status="blocked", result=result)
            store.add_research_event(
                research["id"],
                event_type="blocked",
                level="warning",
                message=str(result["reason"] or "Research task requires user input."),
                payload={"task_id": task["id"], "task": payload},
                source="mycodex",
                phase="waiting_for_decision",
            )
            _try_write_research_markdown(store, project_name, research_name)
            return {**result, "task_id": task["id"], "task_type": task_type}
        else:
            result = _run_generic_task(
                store,
                research,
                task,
                payload,
                providers=providers,
                limit=limit,
                timeout=timeout,
                searcher=searcher or search_public_sources,
                ingester=ingester or ingest_text_source,
                max_chars=max_chars,
            )
        if inline_preflight and isinstance(result, dict):
            result = {**result, "concept_preflight": _compact_preflight_result(inline_preflight)}
        store.update_research_task(task["id"], status="done", result=result)
        store.add_research_event(
            research["id"],
            event_type="task_completed",
            level="info",
            message=_task_result_message(task_type, result),
            payload={"task_id": task["id"], "task_type": task_type, "result": result},
            source="mycodex",
            phase=_phase_after_task(task_type),
        )
        _try_write_research_markdown(store, project_name, research_name)
        return {**result, "status": "done", "task_id": task["id"], "task_type": task_type}
    except Exception as exc:
        result = {"status": "failed", "error": str(exc)}
        store.update_research_task(task["id"], status="failed", result=result)
        store.add_research_event(
            research["id"],
            event_type="task_failed",
            level="error",
            message=str(exc),
            payload={"task_id": task["id"], "task_type": task_type},
            source="mycodex",
            phase="blocked",
        )
        _try_write_research_markdown(store, project_name, research_name)
        raise


def supervise_research(
    store: Store,
    project_name: str,
    research_name: str,
    *,
    max_cycles: int = 1,
    interval: int = 300,
    trigger_control: bool = True,
    control_timeout: int = 1800,
    sandbox: str | None = None,
    approval_policy: str | None = None,
    model: str | None = None,
    reasoning_effort: str | None = None,
    providers: list[str] | None = None,
    limit: int = 5,
    task_timeout: int = 60,
    max_chars: int = 12000,
    searcher: Searcher | None = None,
    ingester: Ingester | None = None,
) -> dict[str, Any]:
    project = _require_project(store, project_name)
    research = store.get_research_thread(project["id"], research_name)
    if research is None:
        raise ValueError(f"unknown research {research_name!r} for project {project_name!r}")
    max_cycles = max(1, int(max_cycles or 1))
    cycles: list[dict[str, Any]] = []
    terminal_reason = ""
    for cycle in range(1, max_cycles + 1):
        queued = store.next_research_task(research["id"])
        if queued is not None:
            result = run_research_task(
                store,
                project_name,
                research_name,
                task_id=queued["id"],
                providers=providers,
                limit=limit,
                timeout=task_timeout,
                max_chars=max_chars,
                allow_control=trigger_control,
                searcher=searcher,
                ingester=ingester,
            )
            cycles.append({"cycle": cycle, "action": "task", "result": result})
            if result.get("status") in {"failed", "blocked"}:
                terminal_reason = str(result.get("status"))
                break
        elif trigger_control:
            from .service import start_research_control_turn

            store.add_research_event(
                research["id"],
                event_type="supervisor_control_started",
                level="info",
                message="No queued task remains; triggering research control turn.",
                source="mycodex",
                phase="analyzing",
            )
            result = start_research_control_turn(
                store,
                project_name,
                research_name,
                timeout=control_timeout,
                sandbox=sandbox,
                approval_policy=approval_policy,
                model=model,
                reasoning_effort=reasoning_effort,
            )
            cycles.append({"cycle": cycle, "action": "control", "result": _compact_control_result(result)})
            applied = result.get("applied") or {}
            if not applied.get("auto_continue") or not applied.get("tasks"):
                terminal_reason = "waiting_for_decision"
                break
        else:
            store.add_research_event(
                research["id"],
                event_type="supervisor_idle",
                level="info",
                message="Research supervisor found no queued tasks and control turns are disabled.",
                source="mycodex",
                phase="idle",
            )
            terminal_reason = "idle"
            cycles.append({"cycle": cycle, "action": "idle", "result": {"status": "idle"}})
            break
        if cycle < max_cycles:
            time.sleep(max(0, int(interval or 0)))
    _try_write_research_markdown(store, project_name, research_name)
    return {
        "research_id": research["id"],
        "project": project_name,
        "name": research_name,
        "cycles": cycles,
        "terminal_reason": terminal_reason,
    }


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _normalize_idea_status(value: Any) -> str:
    status = str(value or "candidate").strip().lower().replace("-", "_").replace(" ", "_")
    if status in {"candidate", "active", "selected", "rejected", "experiment_ready"}:
        return status
    if status in {"discarded", "eliminated", "killed", "kill", "dead"}:
        return "rejected"
    if status in {"ready", "experiment", "experiment_design_ready"}:
        return "experiment_ready"
    if status in {"chosen", "best", "primary"}:
        return "selected"
    if status in {"promising", "continue", "keep"}:
        return "active"
    return "candidate"


def _decision_message(
    contract: dict[str, Any],
    source_count: int,
    claim_count: int,
    idea_count: int,
    task_count: int,
) -> str:
    next_action = contract.get("next_action") if isinstance(contract.get("next_action"), dict) else {}
    action = str(next_action.get("type") or "none")
    auto = "auto" if bool(contract.get("auto_continue")) else "manual"
    return (
        f"Research contract applied: {source_count} source(s), {claim_count} claim(s), "
        f"{idea_count} idea(s), {task_count} task(s); next={action} ({auto})."
    )


def _require_project(store: Store, project_name: str):
    project = store.get_project(project_name)
    if project is None:
        raise ValueError(f"unknown project {project_name!r}; run `mycodex projects add` first")
    return project


def _ensure_concept_preflight_task(
    store: Store,
    research_id: int,
    *,
    question: str,
    auto_queue_search: bool,
) -> int | None:
    if _has_concept_preflight_record(store, research_id):
        return None
    for task in store.list_research_tasks(research_id, limit=200):
        if task["task_type"] == _CONCEPT_PREFLIGHT_KIND and task["status"] in {"queued", "running"}:
            return None
    return store.add_research_task(
        research_id,
        task_type=_CONCEPT_PREFLIGHT_KIND,
        title="Define concept map, query expansion, and anchor promotion criteria",
        payload={
            "question": question,
            "auto_queue_search": auto_queue_search,
            "auto_synthesize": True,
            "auto_deep_read": True,
            "auto_refine": True,
            "reason": "Every research thread must define mechanism axes and query families before search/ranking.",
        },
    )


def _run_concept_preflight_task(
    store: Store,
    research,
    task,
    payload: dict[str, Any],
) -> dict[str, Any]:
    preflight = _record_concept_preflight(
        store,
        research,
        _build_preflight_from_payload(research, task, payload),
        source="mycodex",
        message="Research concept preflight completed.",
    )
    queued_tasks: list[int] = []
    if bool(payload.get("auto_queue_search")):
        queued_tasks.append(
            store.add_research_task(
                research["id"],
                task_type="search",
                title="Search with concept-preflight query expansion",
                query="\n".join(preflight.get("search_queries") or []),
                payload={
                    "expand_queries": False,
                    "auto_synthesize": bool(payload.get("auto_synthesize", True)),
                    "auto_deep_read": bool(payload.get("auto_deep_read", True)),
                    "auto_refine": bool(payload.get("auto_refine", True)),
                    "concept_preflight_claim_id": preflight.get("claim_id"),
                },
            )
        )
    return {
        "profile": preflight.get("profile"),
        "label": preflight.get("label"),
        "queries": preflight.get("search_queries") or [],
        "query_families": [item.get("family") for item in preflight.get("query_families") or []],
        "anchor_promotion_criteria": len(preflight.get("anchor_promotion_criteria") or []),
        "claim_id": preflight.get("claim_id"),
        "queued_tasks": queued_tasks,
    }


def _ensure_concept_preflight_recorded(
    store: Store,
    research,
    payload: dict[str, Any],
    *,
    source: str,
    message: str,
) -> dict[str, Any] | None:
    existing = _latest_concept_preflight(store, research["id"])
    if existing:
        return None
    return _record_concept_preflight(
        store,
        research,
        build_concept_preflight(
            str(research["question"] or payload.get("question") or ""),
            _payload_seed_queries(payload),
            max_queries=int(payload.get("max_queries") or 12),
        ),
        source=source,
        message=message,
    )


def _record_concept_preflight(
    store: Store,
    research,
    preflight: dict[str, Any],
    *,
    source: str,
    message: str,
) -> dict[str, Any]:
    preflight = _normalize_concept_preflight(preflight, str(research["question"] or ""))
    existing = _latest_concept_preflight(store, research["id"])
    if existing:
        return existing
    claim_id = store.add_research_claim(
        research["id"],
        claim=(
            "Concept preflight: define the research problem by mechanism axes, "
            "query families, and anchor promotion criteria before ranking ideas."
        ),
        status="supported",
        support="research control preflight",
        confidence=5,
        notes=_concept_preflight_notes(preflight),
        metadata={
            "kind": _CONCEPT_PREFLIGHT_KIND,
            "concept_preflight": preflight,
        },
    )
    recorded = {**preflight, "claim_id": claim_id}
    store.add_research_event(
        research["id"],
        event_type=_CONCEPT_PREFLIGHT_KIND,
        level="info",
        message=message,
        payload={"claim_id": claim_id, "concept_preflight": preflight},
        source=source,
        status="running",
        phase="concept_preflight",
        phase_message=message,
    )
    return recorded


def _build_preflight_from_payload(research, task, payload: dict[str, Any]) -> dict[str, Any]:
    supplied = payload.get("concept_preflight")
    if isinstance(supplied, dict):
        return supplied
    return build_concept_preflight(
        str(payload.get("question") or research["question"] or ""),
        _task_queries(task, payload),
        max_queries=int(payload.get("max_queries") or 12),
    )


def _concept_preflight_from_contract(contract: dict[str, Any]) -> dict[str, Any] | None:
    direct = contract.get("concept_preflight")
    if isinstance(direct, dict):
        return direct
    concept_map = contract.get("concept_map")
    query_expansion = contract.get("query_expansion")
    anchor_criteria = contract.get("anchor_promotion_criteria")
    if not any(isinstance(item, (dict, list)) for item in (concept_map, query_expansion, anchor_criteria)):
        return None
    return {
        "concept_axes": concept_map if isinstance(concept_map, list) else [],
        "query_families": query_expansion if isinstance(query_expansion, list) else [],
        "anchor_promotion_criteria": anchor_criteria if isinstance(anchor_criteria, list) else [],
        "question": str(contract.get("research_question") or ""),
    }


def _normalize_concept_preflight(preflight: dict[str, Any], question: str) -> dict[str, Any]:
    if preflight.get("profile") and preflight.get("search_queries"):
        return preflight
    generated = build_concept_preflight(str(preflight.get("question") or question or ""))
    return {
        **generated,
        **preflight,
        "concept_axes": preflight.get("concept_axes") or generated["concept_axes"],
        "query_families": preflight.get("query_families") or generated["query_families"],
        "search_queries": preflight.get("search_queries") or generated["search_queries"],
        "anchor_promotion_criteria": preflight.get("anchor_promotion_criteria")
        or generated["anchor_promotion_criteria"],
        "ranking_gate": preflight.get("ranking_gate") or generated["ranking_gate"],
    }


def _latest_concept_preflight(store: Store, research_id: int) -> dict[str, Any] | None:
    for claim in store.list_research_claims(research_id, limit=120):
        metadata = _json_dict(claim["metadata_json"])
        if metadata.get("kind") == _CONCEPT_PREFLIGHT_KIND:
            preflight = metadata.get("concept_preflight")
            if isinstance(preflight, dict):
                return {**preflight, "claim_id": claim["id"]}
    return None


def _has_concept_preflight_record(store: Store, research_id: int) -> bool:
    return _latest_concept_preflight(store, research_id) is not None


def _task_requires_concept_preflight(task_type: str, payload: dict[str, Any]) -> bool:
    if payload.get("require_concept_preflight") is False:
        return False
    return task_type in {
        "search",
        "source_search",
        "literature_search",
        "triage",
        "deep_read_top",
        "read_top",
        "paper_read_top",
        "threat_search",
        "nearest_threat_search",
        "idea_critique",
        "critique_ideas",
        "synthesize",
        "local_synthesize",
        "rank_ideas",
        "local_idea_tournament",
        "idea_tournament",
        "design_experiment",
    }


def _payload_seed_queries(payload: dict[str, Any]) -> list[str]:
    queries: list[str] = []
    raw_queries = payload.get("queries")
    if isinstance(raw_queries, list):
        queries.extend(str(item).strip() for item in raw_queries if str(item).strip())
    elif isinstance(raw_queries, str) and raw_queries.strip():
        queries.append(raw_queries.strip())
    raw_query = payload.get("query")
    if isinstance(raw_query, str) and raw_query.strip():
        queries.append(raw_query.strip())
    return queries


def _concept_preflight_notes(preflight: dict[str, Any]) -> str:
    families = [
        str(item.get("family") or item.get("name") or "")
        for item in preflight.get("query_families", [])
        if isinstance(item, dict)
    ]
    criteria = [str(item) for item in preflight.get("anchor_promotion_criteria", [])]
    return "\n".join(
        [
            f"Profile: {preflight.get('profile') or '-'} ({preflight.get('label') or '-'})",
            "Query families: " + (", ".join(item for item in families if item) or "-"),
            "Anchor criteria:",
            *[f"- {item}" for item in criteria[:8]],
        ]
    )


def _compact_preflight_result(preflight: dict[str, Any]) -> dict[str, Any]:
    return {
        "profile": preflight.get("profile"),
        "claim_id": preflight.get("claim_id"),
        "queries": len(preflight.get("search_queries") or []),
        "query_families": len(preflight.get("query_families") or []),
    }


def _render_concept_preflight_markdown(preflight: dict[str, Any]) -> list[str]:
    lines = [
        f"- Profile: `{_md_escape(preflight.get('profile') or '-')}` ({_md_escape(preflight.get('label') or '-')})",
        f"- Ranking gate: {_md_escape(preflight.get('ranking_gate') or '-')}",
        "- Query families: "
        + _md_escape(
            ", ".join(
                str(item.get("family") or item.get("name") or "")
                for item in preflight.get("query_families", [])
                if isinstance(item, dict)
            )
            or "-"
        ),
        "- Anchor criteria:",
    ]
    for criterion in (preflight.get("anchor_promotion_criteria") or [])[:6]:
        lines.append(f"  - {_md_escape(criterion)}")
    lines.append("- Search queries:")
    for query in (preflight.get("search_queries") or [])[:8]:
        lines.append(f"  - `{_md_escape(query)}`")
    return lines


def _run_search_like_task(
    store: Store,
    research,
    task,
    payload: dict[str, Any],
    *,
    providers: list[str] | None,
    limit: int,
    timeout: int,
    searcher: Searcher,
) -> dict[str, Any]:
    queries = _task_queries(task, payload)
    if not queries:
        sources_to_read = _sources_to_read(payload)
        if sources_to_read:
            queued = _queue_deep_read_tasks(store, research["id"], sources_to_read)
            return {"queued_deep_reads": queued, "queries": []}
        raise RuntimeError("search/triage task has no query")
    question = str(research["question"] or payload.get("question") or "")
    if bool(payload.get("expand_queries", True)):
        queries = expand_queries(question, queries, max_queries=int(payload.get("max_queries") or 4))
    source_ids: list[int] = []
    rejected_ids: list[int] = []
    seen_source_ids: set[int] = set()
    scored_count = 0
    min_relevance = _int_or_zero(payload.get("min_relevance") if "min_relevance" in payload else 2)
    for query in queries:
        for result in searcher(query, providers=providers, limit=limit, timeout=timeout):
            kwargs = _source_kwargs_from_result(result)
            if not kwargs:
                continue
            quality = score_source(kwargs, question=question, query=query)
            metadata = kwargs.get("metadata") if isinstance(kwargs.get("metadata"), dict) else {}
            kwargs["metadata"] = {
                **metadata,
                "quality": quality,
                "matched_query": query,
            }
            kwargs["relevance"] = quality["relevance"]
            kwargs["evidence_quality"] = quality["evidence_quality"]
            kwargs["role"] = quality["role"]
            kwargs["status"] = quality["status"]
            source_id = store.upsert_research_source(research["id"], **kwargs)
            scored_count += 1
            if source_id in seen_source_ids:
                continue
            seen_source_ids.add(source_id)
            if quality["relevance"] >= min_relevance:
                source_ids.append(source_id)
            else:
                rejected_ids.append(source_id)
    all_sources = [_row_to_source_dict(row) for row in store.list_research_sources(research["id"], limit=200)]
    coverage = coverage_summary(question, all_sources)
    next_tasks: list[int] = []
    if bool(payload.get("auto_synthesize")):
        synth_payload = {"reason": "search completed", "source_task_id": task["id"]}
        if payload.get("auto_refine") is not None:
            synth_payload["auto_refine"] = bool(payload.get("auto_refine"))
        if payload.get("auto_deep_read") is not None:
            synth_payload["auto_deep_read"] = bool(payload.get("auto_deep_read"))
        next_tasks.append(
            store.add_research_task(
                research["id"],
                task_type="local_synthesize",
                title="Synthesize filtered source ledger into claims and ideas",
                payload=synth_payload,
            )
        )
    return {
        "queries": queries,
        "source_ids": source_ids,
        "rejected_source_ids": rejected_ids,
        "scored": scored_count,
        "sources": len(source_ids),
        "coverage": coverage,
        "queued_tasks": next_tasks,
        "providers": providers or ["zotero", "arxiv", "openalex", "semanticscholar"],
    }


def _run_deep_read_task(
    store: Store,
    research,
    task,
    payload: dict[str, Any],
    *,
    timeout: int,
    max_chars: int,
    ingester: Ingester,
) -> dict[str, Any]:
    source_id = _task_source_id(task, payload)
    source = store.get_research_source(research["id"], source_id) if source_id else None
    location = _read_location(source, payload, task)
    if not location and source is None:
        return _run_deep_read_sources_task(
            store,
            research,
            task,
            payload,
            timeout=timeout,
            max_chars=max_chars,
            ingester=ingester,
        )
    if not location:
        raise RuntimeError("deep_read task has no source URL/path")
    result = ingester(location, max_chars=max_chars, timeout=timeout)
    text = str(result.get("text") or "")
    existing_metadata = _json_dict(source["metadata_json"]) if source is not None else {}
    metadata = {
        **existing_metadata,
        "ingestion": {k: v for k, v in result.items() if k != "text"},
    }
    notes = _deep_read_notes(result, text)
    if source is not None:
        store.update_research_source(
            source["id"],
            status="kept",
            depth=str(payload.get("depth") or "skim"),
            notes=notes,
            metadata=metadata,
        )
    else:
        source_id = store.upsert_research_source(
            research["id"],
            title=str(payload.get("title") or location),
            url=location,
            source_type=str(payload.get("source_type") or "paper"),
            role=str(payload.get("role") or "candidate"),
            depth=str(payload.get("depth") or "skim"),
            status="kept",
            notes=notes,
            metadata=metadata,
        )
    return {
        "source_id": source_id,
        "location": location,
        "resolved_location": result.get("resolved_location"),
        "content_type": result.get("content_type"),
        "bytes": result.get("bytes"),
        "chars": len(text),
        "method": result.get("method"),
    }


def _run_deep_read_sources_task(
    store: Store,
    research,
    task,
    payload: dict[str, Any],
    *,
    timeout: int,
    max_chars: int,
    ingester: Ingester,
) -> dict[str, Any]:
    sources = _sources_for_deep_read_task(store, research["id"], task, payload)
    if not sources:
        raise RuntimeError("deep_read task has no source URL/path")
    ingested: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for source in sources:
        location = _read_location(source, {}, task)
        if not location:
            failures.append({"source_id": source["id"], "title": source["title"], "error": "no readable location"})
            continue
        try:
            result = ingester(location, max_chars=max_chars, timeout=timeout)
            text = str(result.get("text") or "")
            metadata = {
                **_json_dict(source["metadata_json"]),
                "ingestion": {key: value for key, value in result.items() if key != "text"},
            }
            store.update_research_source(
                source["id"],
                status="kept",
                depth="deep" if len(text) >= 2000 else "skim",
                notes=_deep_read_notes(result, text),
                metadata=metadata,
                evidence_quality=min(5, int(source["evidence_quality"] or 0) + 1),
            )
            ingested.append(
                {
                    "source_id": source["id"],
                    "title": source["title"],
                    "location": location,
                    "chars": len(text),
                    "method": result.get("method"),
                }
            )
        except Exception as exc:
            failures.append({"source_id": source["id"], "title": source["title"], "error": str(exc)})
    if not ingested:
        raise RuntimeError(
            "deep_read task could not ingest any matched source: "
            + "; ".join(str(item["error"]) for item in failures[:3])
        )
    return {"selected": len(sources), "ingested": ingested, "failures": failures}


def _run_deep_read_top_task(
    store: Store,
    research,
    task,
    payload: dict[str, Any],
    *,
    timeout: int,
    max_chars: int,
    ingester: Ingester,
) -> dict[str, Any]:
    sources = [_row_to_source_dict(row) for row in store.list_research_sources(research["id"], limit=200)]
    selected = select_sources_for_deep_read(
        question=str(research["question"] or ""),
        sources=sources,
        max_sources=int(payload.get("max_sources") or 3),
    )
    ingested: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for source in selected:
        source_id = int(source["id"])
        location = str(source["read_location"])
        try:
            result = ingester(location, max_chars=max_chars, timeout=timeout)
            text = str(result.get("text") or "")
            existing_metadata = source.get("metadata") if isinstance(source.get("metadata"), dict) else {}
            metadata = {
                **existing_metadata,
                "ingestion": {k: v for k, v in result.items() if k != "text"},
            }
            store.update_research_source(
                source_id,
                status="kept",
                depth="deep" if len(text) >= 2000 else "skim",
                notes=_deep_read_notes(result, text),
                metadata=metadata,
                evidence_quality=min(5, int(source.get("evidence_quality") or 0) + 1),
            )
            ingested.append(
                {
                    "source_id": source_id,
                    "title": source["title"],
                    "location": location,
                    "chars": len(text),
                    "method": result.get("method"),
                }
            )
        except Exception as exc:
            failures.append({"source_id": source_id, "title": source["title"], "error": str(exc)})
    return {
        "selected": len(selected),
        "ingested": ingested,
        "failures": failures,
    }


def _run_control_task(
    store: Store,
    project_name: str,
    research_name: str,
    task,
    *,
    timeout: int,
) -> dict[str, Any]:
    from .service import start_research_control_turn

    result = start_research_control_turn(
        store,
        project_name,
        research_name,
        timeout=timeout,
        sandbox="read-only",
        approval_policy="never",
    )
    return {"control": _compact_control_result(result), "task_id": task["id"]}


def _run_local_synthesis_task(
    store: Store,
    research,
    task,
    payload: dict[str, Any],
) -> dict[str, Any]:
    source_rows = store.list_research_sources(research["id"], limit=int(payload.get("source_limit") or 80))
    source_dicts = [_row_to_source_dict(row) for row in source_rows]
    synthesis = synthesize_ledgers(
        question=str(research["question"] or ""),
        sources=source_dicts,
        max_claims=int(payload.get("max_claims") or 4),
        max_ideas=int(payload.get("max_ideas") or 3),
    )
    claim_ids: list[int] = []
    for claim in synthesis["claims"]:
        claim_ids.append(
            store.add_research_claim(
                research["id"],
                claim=str(claim["claim"]),
                status=str(claim.get("status") or "uncertain"),
                support=str(claim.get("support") or ""),
                confidence=_int_or_zero(claim.get("confidence")),
                notes=str(claim.get("notes") or ""),
                metadata=claim.get("metadata") if isinstance(claim.get("metadata"), dict) else claim,
            )
        )
    idea_ids: list[int] = []
    for idea in synthesis["ideas"]:
        idea_ids.append(
            store.add_research_idea(
                research["id"],
                title=str(idea["title"]),
                hypothesis=str(idea.get("hypothesis") or ""),
                mechanism=str(idea.get("mechanism") or ""),
                status=str(idea.get("status") or "candidate"),
                score=idea.get("score") if isinstance(idea.get("score"), dict) else {},
                minimal_test=str(idea.get("minimal_test") or ""),
                kill_criteria=str(idea.get("kill_criteria") or ""),
                notes=str(idea.get("notes") or ""),
                metadata=idea.get("metadata") if isinstance(idea.get("metadata"), dict) else idea,
            )
        )
    queued_tasks: list[int] = []
    if bool(payload.get("auto_deep_read")):
        queued_tasks.append(
            store.add_research_task(
                research["id"],
                task_type="deep_read_top",
                title="Deep-read top retained sources",
                payload={"reason": "local synthesis requested deeper grounding"},
            )
        )
    if bool(payload.get("auto_refine")):
        queued_tasks.append(
            store.add_research_task(
                research["id"],
                task_type="threat_search",
                title="Search nearest threats for generated ideas",
                payload={"idea_ids": idea_ids, "reason": "local synthesis requested idea refinement"},
            )
        )
        queued_tasks.append(
            store.add_research_task(
                research["id"],
                task_type="idea_critique",
                title="Critique generated ideas against nearest threats",
                payload={"idea_ids": idea_ids, "reason": "local synthesis requested idea critique"},
            )
        )
    return {
        "profile": synthesis["profile"],
        "claims": len(claim_ids),
        "claim_ids": claim_ids,
        "ideas": len(idea_ids),
        "idea_ids": idea_ids,
        "queued_tasks": queued_tasks,
        "top_sources": synthesis["top_sources"],
        "coverage": synthesis["coverage"],
    }


def _run_threat_search_task(
    store: Store,
    research,
    task,
    payload: dict[str, Any],
    *,
    providers: list[str] | None,
    limit: int,
    timeout: int,
    searcher: Searcher,
) -> dict[str, Any]:
    ideas = _selected_ideas(store, research["id"], payload)
    source_ids: list[int] = []
    per_idea: dict[str, list[int]] = {}
    for idea in ideas:
        queries = threat_queries_for_idea(
            _row_to_idea_dict(idea),
            question=str(research["question"] or ""),
            max_queries=int(payload.get("max_queries") or 3),
        )
        threat_ids: list[int] = []
        for query in queries:
            for result in searcher(query, providers=providers, limit=limit, timeout=timeout):
                kwargs = _source_kwargs_from_result(result)
                if not kwargs:
                    continue
                quality = score_source(kwargs, question=str(research["question"] or ""), query=query)
                metadata = kwargs.get("metadata") if isinstance(kwargs.get("metadata"), dict) else {}
                threat_for = sorted(set([*metadata.get("threat_for_idea_ids", []), int(idea["id"])])) if isinstance(metadata.get("threat_for_idea_ids"), list) else [int(idea["id"])]
                kwargs["metadata"] = {
                    **metadata,
                    "quality": quality,
                    "matched_query": query,
                    "threat_for_idea_ids": threat_for,
                }
                kwargs["relevance"] = quality["relevance"]
                kwargs["evidence_quality"] = quality["evidence_quality"]
                kwargs["role"] = "counterevidence" if quality["relevance"] >= 2 else quality["role"]
                kwargs["status"] = "candidate" if quality["relevance"] >= 2 else "rejected"
                source_id = store.upsert_research_source(research["id"], **kwargs)
                if source_id not in threat_ids:
                    threat_ids.append(source_id)
                if source_id not in source_ids:
                    source_ids.append(source_id)
        metadata = _json_dict(idea["metadata_json"])
        existing_threats = metadata.get("threat_source_ids") if isinstance(metadata.get("threat_source_ids"), list) else []
        store.update_research_idea(
            int(idea["id"]),
            metadata={
                **metadata,
                "threat_source_ids": sorted(set([*existing_threats, *threat_ids])),
                "threat_search_queries": queries,
            },
        )
        per_idea[str(idea["id"])] = threat_ids
    return {
        "ideas": len(ideas),
        "source_ids": source_ids,
        "sources": len(source_ids),
        "per_idea": per_idea,
    }


def _run_idea_critique_task(
    store: Store,
    research,
    task,
    payload: dict[str, Any],
) -> dict[str, Any]:
    ideas = _selected_ideas(store, research["id"], payload)
    sources = [_row_to_source_dict(row) for row in store.list_research_sources(research["id"], limit=200)]
    critiques: list[dict[str, Any]] = []
    for idea in ideas:
        idea_dict = _row_to_idea_dict(idea)
        metadata = idea_dict["metadata"]
        threat_ids = metadata.get("threat_source_ids") if isinstance(metadata.get("threat_source_ids"), list) else []
        threat_sources = [source for source in sources if int(source["id"]) in {int(item) for item in threat_ids}]
        critique = critique_idea(
            idea_dict,
            question=str(research["question"] or ""),
            sources=sources,
            threat_sources=threat_sources,
        )
        refined = critique["refinement"]
        notes = "\n\n".join(part for part in [idea_dict.get("notes", ""), critique["notes"]] if part)
        store.update_research_idea(
            int(idea["id"]),
            status=str(critique["status"]),
            score=critique["score"],
            hypothesis=str(refined.get("hypothesis") or idea["hypothesis"]),
            minimal_test=str(refined.get("minimal_test") or idea["minimal_test"]),
            notes=notes,
            metadata={
                **metadata,
                "critique": critique,
            },
        )
        critiques.append(
            {
                "idea_id": int(idea["id"]),
                "title": idea["title"],
                "status": critique["status"],
                "score": critique["score"],
                "nearest_threats": critique["nearest_threats"],
            }
        )
    return {"ideas": len(ideas), "critiques": critiques}


def _run_generic_task(
    store: Store,
    research,
    task,
    payload: dict[str, Any],
    *,
    providers: list[str] | None,
    limit: int,
    timeout: int,
    searcher: Searcher,
    ingester: Ingester,
    max_chars: int,
) -> dict[str, Any]:
    if _task_queries(task, payload):
        return _run_search_like_task(
            store,
            research,
            task,
            payload,
            providers=providers,
            limit=limit,
            timeout=timeout,
            searcher=searcher,
        )
    if _read_location(None, payload, task):
        return _run_deep_read_task(
            store,
            research,
            task,
            payload,
            timeout=timeout,
            max_chars=max_chars,
            ingester=ingester,
        )
    raise RuntimeError(f"unsupported research task type {task['task_type']!r}")


def _task_queries(task, payload: dict[str, Any]) -> list[str]:
    queries: list[str] = []
    raw_queries = payload.get("queries")
    if isinstance(raw_queries, list):
        queries.extend(str(item).strip() for item in raw_queries if str(item).strip())
    elif isinstance(raw_queries, str) and raw_queries.strip():
        queries.append(raw_queries.strip())
    raw_query = payload.get("query")
    if isinstance(raw_query, str) and raw_query.strip():
        queries.append(raw_query.strip())
    if task["query"]:
        queries.extend(line.strip() for line in str(task["query"]).splitlines() if line.strip())
    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        if query not in seen:
            seen.add(query)
            deduped.append(query)
    return deduped


def _sources_to_read(payload: dict[str, Any]) -> list[Any]:
    value = payload.get("sources_to_read")
    return value if isinstance(value, list) else []


def _queue_deep_read_tasks(store: Store, research_id: int, sources: list[Any]) -> list[int]:
    task_ids: list[int] = []
    for source in sources:
        payload: dict[str, Any]
        title = ""
        query = ""
        source_id = None
        if isinstance(source, dict):
            payload = source
            title = str(source.get("title") or source.get("url") or "")
            query = str(source.get("url") or source.get("path") or "")
            source_id = _int_or_none(source.get("source_id"))
        else:
            payload = {"url": str(source)}
            title = str(source)
            query = str(source)
        task_ids.append(
            store.add_research_task(
                research_id,
                task_type="deep_read",
                title=title,
                query=query,
                source_id=source_id,
                payload=payload,
            )
        )
    return task_ids


def _task_source_id(task, payload: dict[str, Any]) -> int | None:
    if task["source_id"] is not None:
        return int(task["source_id"])
    return _int_or_none(payload.get("source_id"))


def _selected_ideas(store: Store, research_id: int, payload: dict[str, Any]) -> list[Any]:
    raw_ids = payload.get("idea_ids")
    idea_ids = []
    if isinstance(raw_ids, list):
        idea_ids = [_int_or_none(item) for item in raw_ids]
        idea_ids = [item for item in idea_ids if item is not None]
    elif payload.get("idea_id") is not None:
        idea_id = _int_or_none(payload.get("idea_id"))
        idea_ids = [idea_id] if idea_id is not None else []
    if idea_ids:
        rows = []
        for idea_id in idea_ids:
            row = store.get_research_idea(research_id, int(idea_id))
            if row is not None:
                rows.append(row)
        return rows
    return store.list_research_ideas(research_id, limit=int(payload.get("limit") or 20))


def _sources_for_deep_read_task(store: Store, research_id: int, task, payload: dict[str, Any]) -> list[Any]:
    sources_by_id = {int(row["id"]): row for row in store.list_research_sources(research_id, limit=300)}
    selected: list[Any] = []

    def add_source(row: Any | None) -> None:
        if row is None:
            return
        if any(int(existing["id"]) == int(row["id"]) for existing in selected):
            return
        selected.append(row)

    raw_ids = payload.get("source_ids")
    if isinstance(raw_ids, list):
        for item in raw_ids:
            source_id = _int_or_none(item)
            if source_id is not None:
                add_source(sources_by_id.get(source_id))
    elif payload.get("source_id") is not None:
        source_id = _int_or_none(payload.get("source_id"))
        if source_id is not None:
            add_source(sources_by_id.get(source_id))

    for target in _source_targets_from_payload(task, payload):
        exact = _find_source_by_target(sources_by_id.values(), target, require_readable=True)
        add_source(exact or _find_source_by_target(sources_by_id.values(), target, require_readable=False))
    return selected


def _source_targets_from_payload(task, payload: dict[str, Any]) -> list[str]:
    targets: list[str] = []
    for key in ("sources_to_read", "source_titles", "queries"):
        raw = payload.get(key)
        if isinstance(raw, list):
            targets.extend(str(item) for item in raw if str(item).strip())
    for key in ("url", "path", "location", "pdf_url"):
        value = payload.get(key)
        if value:
            targets.append(str(value))
    if task["query"]:
        targets.extend(line.strip().strip('"') for line in str(task["query"]).splitlines() if line.strip())
    deduped: list[str] = []
    for target in targets:
        target = target.strip()
        if target and target not in deduped:
            deduped.append(target)
    return deduped


def _triage_requires_control(task, payload: dict[str, Any]) -> bool:
    if payload.get("use_control") is True:
        return True
    if payload.get("use_control") is False:
        return False
    has_existing_targets = any(
        isinstance(payload.get(key), list) and bool(payload.get(key))
        for key in ("source_ids", "idea_ids", "sources_to_read")
    )
    has_queries = isinstance(payload.get("queries"), list) and bool(payload.get("queries"))
    return has_existing_targets and not has_queries


def _find_source_by_target(sources, target: str, *, require_readable: bool):
    target_norm = _normalize_match_text(target)
    best = None
    best_score = 0
    for source in sources:
        location = _read_location(source, {}, {"query": ""})
        if require_readable and not location:
            continue
        candidates = [
            str(source["title"] or ""),
            str(source["url"] or ""),
            str(_json_dict(source["metadata_json"]).get("pdf_url") or ""),
            str(_json_dict(source["metadata_json"]).get("external_url") or ""),
        ]
        score = 0
        for candidate in candidates:
            candidate_norm = _normalize_match_text(candidate)
            if not candidate_norm:
                continue
            if target_norm == candidate_norm:
                score = max(score, 100)
            elif target_norm in candidate_norm or candidate_norm in target_norm:
                score = max(score, min(len(target_norm), len(candidate_norm)))
        if score > best_score:
            best = source
            best_score = score
    return best


def _read_location(source, payload: dict[str, Any], task) -> str:
    for key in ("url", "path", "location", "pdf_url"):
        value = payload.get(key)
        if value:
            return str(value)
    if source is not None:
        metadata = _json_dict(source["metadata_json"])
        for key in ("pdf_url", "local_attachment_path", "url", "open_access_pdf"):
            value = metadata.get(key)
            if value:
                return str(value)
        if str(source["url"] or "").startswith(("http://", "https://", "file://", "/", "~")):
            return str(source["url"])
    if task["query"] and str(task["query"]).startswith(("http://", "https://", "/", "~")):
        return str(task["query"])
    return ""


def _source_kwargs_from_result(result: Any) -> dict[str, Any]:
    if hasattr(result, "as_source_kwargs"):
        return result.as_source_kwargs()
    if not isinstance(result, dict):
        return {}
    title = str(result.get("title") or result.get("name") or "").strip()
    url = str(result.get("url") or result.get("link") or "").strip()
    if not title and not url:
        return {}
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else dict(result)
    abstract = str(result.get("abstract") or result.get("summary") or "")
    return {
        "title": title or url,
        "url": url,
        "source_type": str(result.get("source_type") or result.get("type") or "paper"),
        "role": str(result.get("role") or "candidate"),
        "depth": str(result.get("depth") or ("abstract" if abstract else "title")),
        "status": str(result.get("status") or "candidate"),
        "relevance": _int_or_zero(result.get("relevance")),
        "evidence_quality": _int_or_zero(result.get("evidence_quality")),
        "notes": str(result.get("notes") or abstract),
        "metadata": metadata,
    }


def _row_to_source_dict(row) -> dict[str, Any]:
    metadata = _json_dict(row["metadata_json"])
    return {
        "id": row["id"],
        "title": row["title"],
        "url": row["url"],
        "source_type": row["source_type"],
        "role": row["role"],
        "depth": row["depth"],
        "status": row["status"],
        "relevance": row["relevance"],
        "evidence_quality": row["evidence_quality"],
        "notes": _truncate(row["notes"], 900),
        "metadata": metadata,
        "updated_at": row["updated_at"],
    }


def _row_to_claim_dict(row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "source_id": row["source_id"],
        "source_title": row["source_title"],
        "claim": row["claim"],
        "status": row["status"],
        "support": row["support"],
        "confidence": row["confidence"],
        "notes": _truncate(row["notes"], 700),
        "metadata": _json_dict(row["metadata_json"]),
        "updated_at": row["updated_at"],
    }


def _row_to_idea_dict(row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "title": row["title"],
        "hypothesis": row["hypothesis"],
        "mechanism": row["mechanism"],
        "status": row["status"],
        "score": _json_dict(row["score_json"]),
        "minimal_test": row["minimal_test"],
        "kill_criteria": row["kill_criteria"],
        "notes": _truncate(row["notes"], 1000),
        "metadata": _json_dict(row["metadata_json"]),
        "updated_at": row["updated_at"],
    }


def _row_to_task_dict(row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "task_type": row["task_type"],
        "status": row["status"],
        "title": row["title"],
        "query": _truncate(row["query"], 500),
        "source_id": row["source_id"],
        "source_title": row["source_title"],
        "idea_id": row["idea_id"],
        "idea_title": row["idea_title"],
        "payload": _json_dict(row["payload_json"]),
        "result": _json_dict(row["result_json"]),
        "updated_at": row["updated_at"],
        "completed_at": row["completed_at"],
    }


def _row_to_event_dict(row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "iteration": row["iteration"],
        "event_type": row["event_type"],
        "level": row["level"],
        "message": _truncate(row["message"], 700),
        "payload": _json_dict(row["payload_json"]),
        "source": row["source"],
        "run_id": row["run_id"],
        "created_at": row["created_at"],
    }


def _deep_read_notes(result: dict[str, Any], text: str) -> str:
    location = result.get("resolved_location") or result.get("location") or ""
    method = result.get("method") or "unknown"
    header = f"Ingested text excerpt via {method} from {location}."
    if not text.strip():
        return header + "\n\nNo extractable text was found."
    return header + "\n\n" + text.strip()


def _task_result_message(task_type: str, result: dict[str, Any]) -> str:
    if task_type == _CONCEPT_PREFLIGHT_KIND:
        return (
            "Research concept preflight completed; "
            f"{len(result.get('queries') or [])} search query candidate(s) prepared."
        )
    if "sources" in result:
        return f"Research task {task_type} completed; {result['sources']} source(s) recorded."
    if "chars" in result:
        return f"Research task {task_type} completed; {result['chars']} character(s) ingested."
    if "control" in result:
        control = result["control"]
        return f"Research task {task_type} completed through control run #{control.get('run_id')}."
    return f"Research task {task_type} completed."


def _phase_for_task(task_type: str) -> str:
    return {
        "concept_preflight": "concept_preflight",
        "search": "searching",
        "source_search": "searching",
        "literature_search": "searching",
        "triage": "triage",
        "deep_read": "deep_read",
        "deep_read_top": "deep_read",
        "read": "deep_read",
        "read_top": "deep_read",
        "paper_read": "deep_read",
        "paper_read_top": "deep_read",
        "ingest": "deep_read",
        "threat_search": "threat_search",
        "nearest_threat_search": "threat_search",
        "idea_critique": "idea_tournament",
        "critique_ideas": "idea_tournament",
        "synthesize": "synthesis",
        "local_synthesize": "synthesis",
        "rank_ideas": "idea_tournament",
        "local_idea_tournament": "idea_tournament",
        "idea_tournament": "idea_tournament",
        "design_experiment": "experiment_bridge",
    }.get(task_type, "running_task")


def _phase_after_task(task_type: str) -> str:
    return {
        "concept_preflight": "searching",
        "search": "triage",
        "source_search": "triage",
        "literature_search": "triage",
        "triage": "synthesis",
        "deep_read": "synthesis",
        "deep_read_top": "synthesis",
        "read": "synthesis",
        "read_top": "synthesis",
        "paper_read": "synthesis",
        "paper_read_top": "synthesis",
        "ingest": "synthesis",
        "threat_search": "idea_tournament",
        "nearest_threat_search": "idea_tournament",
        "idea_critique": "waiting_for_decision",
        "critique_ideas": "waiting_for_decision",
        "synthesize": "idea_tournament",
        "local_synthesize": "idea_tournament",
        "rank_ideas": "waiting_for_decision",
        "local_idea_tournament": "waiting_for_decision",
        "idea_tournament": "waiting_for_decision",
        "design_experiment": "experiment_bridge",
    }.get(task_type, "waiting_for_decision")


def _compact_control_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": result.get("run_id"),
        "thread_id": result.get("thread_id"),
        "turn_id": result.get("turn_id"),
        "status": result.get("status"),
        "applied": result.get("applied"),
    }


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _truncate(value: Any, limit: int) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _normalize_match_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _compact_for_control_context(
    value: Any,
    *,
    string_limit: int = 1200,
    list_limit: int = 16,
    dict_limit: int = 40,
) -> Any:
    if isinstance(value, str):
        return _truncate(value, string_limit)
    if isinstance(value, list):
        items = [
            _compact_for_control_context(
                item,
                string_limit=string_limit,
                list_limit=list_limit,
                dict_limit=dict_limit,
            )
            for item in value[:list_limit]
        ]
        if len(value) > list_limit:
            items.append({"truncated_items": len(value) - list_limit})
        return items
    if isinstance(value, dict):
        compact: dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= dict_limit:
                compact["truncated_keys"] = len(value) - dict_limit
                break
            compact[str(key)] = _compact_for_control_context(
                item,
                string_limit=string_limit,
                list_limit=list_limit,
                dict_limit=dict_limit,
            )
        return compact
    return value


def _try_write_research_markdown(store: Store, project_name: str, research_name: str) -> None:
    try:
        write_research_markdown(store, project_name, research_name)
    except Exception:
        # Markdown snapshots are auxiliary. SQLite remains the source of truth.
        pass


def _safe_filename(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip()).strip("-._")
    return safe or "research"


def _format_ts(value: Any) -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(value)))
    except (TypeError, ValueError, OSError):
        return "-"


def _md_escape(value: Any) -> str:
    return str(value or "").replace("\n", " ").replace("|", "\\|")


def _md_source_link(row: Any) -> str:
    title = _md_escape(_truncate(row["title"], 180))
    url = str(row["url"] or "")
    if url.startswith(("http://", "https://", "zotero://")):
        return f"[{title}]({url})"
    return title


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
