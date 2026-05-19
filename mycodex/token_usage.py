from __future__ import annotations

import bisect
import json
import os
import re
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .db import Store


DEFAULT_CODEX_LOGS_PATH = Path(
    os.environ.get("MYCODEX_CODEX_LOGS_DB", "~/.codex/logs_2.sqlite")
).expanduser()
SSE_PREFIX = "SSE event: "
USAGE_TARGET = "codex_api::sse::responses"
METADATA_TARGETS = (
    "codex_core::session::turn",
    "codex_core::stream_events_utils",
    "codex_otel.trace_safe",
    "codex_otel.log_only",
)
CHART_COLORS = (
    "#1769aa",
    "#16784f",
    "#b7791f",
    "#b42318",
    "#0f766e",
    "#4f46e5",
    "#64748b",
)
THREAD_RE = re.compile(r"(?:conversation\.id|thread(?:\.id|_id)?)=([A-Za-z0-9_.:-]+)")
CWD_RE = re.compile(r"cwd=([^,\s}]+)")


def resolve_codex_logs_path(path: str | Path | None = None) -> Path:
    if path is not None:
        return Path(path).expanduser().resolve()
    return DEFAULT_CODEX_LOGS_PATH.resolve()


def token_usage_snapshot(
    store: Store | None = None,
    *,
    logs_db_path: str | Path | None = None,
    days: int = 7,
    limit: int = 5000,
) -> dict[str, Any]:
    days = max(1, min(int(days), 30))
    limit = max(1, min(int(limit), 20000))
    now = int(time.time())
    start_ts = now - days * 86400
    source = resolve_codex_logs_path(logs_db_path)
    empty_hour_rows = _chart_bars(_filled_hours(24, now, {}))
    empty_day_rows = _chart_bars(_filled_days(days, now, {}))
    snapshot: dict[str, Any] = {
        "available": False,
        "error": "",
        "source": str(source),
        "generated_at": now,
        "window_days": days,
        "window_start": start_ts,
        "summary": _empty_metrics(),
        "by_project": [],
        "by_model": [],
        "by_day": empty_day_rows,
        "by_hour": empty_hour_rows,
        "day_axis": _chart_axis(empty_day_rows),
        "hour_axis": _chart_axis(empty_hour_rows),
        "project_pie": [],
        "project_pie_gradient": "#eef1f5 0% 100%",
        "recent": [],
        "unmapped_count": 0,
    }
    if not source.exists():
        snapshot["error"] = "Codex logs database was not found."
        return snapshot

    try:
        conn = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        snapshot["error"] = f"Could not open Codex logs database: {exc}"
        return snapshot

    try:
        live_events = _load_usage_events(conn, start_ts=start_ts, limit=limit)
        _attach_metadata(conn, live_events, start_ts=start_ts, now=now)
    except sqlite3.Error as exc:
        snapshot["error"] = f"Could not read Codex logs database: {exc}"
        return snapshot
    finally:
        conn.close()

    project_index = _project_index(store)
    for event in live_events:
        project = _match_project(event, project_index)
        event["project"] = project["name"] if project else "Unmapped"
        event["project_path"] = project["path"] if project else ""
        event["project_id"] = project["id"] if project else None

    if store is not None:
        try:
            event_keys = [event["event_key"] for event in live_events]
            known_keys = store.token_usage_event_keys(event_keys)
            new_events = [event for event in live_events if event["event_key"] not in known_keys]
            store.upsert_token_usage_events(new_events)
        except sqlite3.OperationalError as exc:
            store.conn.rollback()
            snapshot["error"] = f"Token usage cache update skipped: {exc}"

        try:
            events = _cached_usage_events(store, start_ts=start_ts, limit=limit)
        except sqlite3.OperationalError as exc:
            store.conn.rollback()
            snapshot["error"] = f"Token usage cache read skipped: {exc}"
            events = live_events

        if live_events:
            cached_keys = {event["event_key"] for event in events}
            events.extend(event for event in live_events if event["event_key"] not in cached_keys)
            events.sort(key=lambda item: (item["ts"], item["log_id"]), reverse=True)

        for event in events:
            if event.get("project") and event["project"] != "Unmapped":
                continue
            project = _match_project(event, project_index)
            event["project"] = project["name"] if project else event.get("project") or "Unmapped"
            event["project_path"] = project["path"] if project else event.get("project_path") or ""
            event["project_id"] = project["id"] if project else event.get("project_id")
    else:
        events = live_events

    summary = _empty_metrics()
    by_project: dict[str, dict[str, Any]] = {}
    by_model: dict[str, dict[str, Any]] = {}
    by_day: dict[str, dict[str, Any]] = {}
    by_hour: dict[str, dict[str, Any]] = {}
    unmapped_count = 0
    for event in events:
        _add_metrics(summary, event)
        _add_to_bucket(by_project, event["project"], event)
        _add_to_bucket(by_model, event["model"], event)
        day = datetime.fromtimestamp(event["ts"]).strftime("%Y-%m-%d")
        _add_to_bucket(by_day, day, event)
        hour = _hour_key(event["ts"])
        _add_to_bucket(by_hour, hour, event)
        if event["project"] == "Unmapped":
            unmapped_count += 1

    project_rows = _sorted_buckets(by_project)
    day_rows = _chart_bars(_filled_days(days, now, by_day))
    hour_rows = _chart_bars(_filled_hours(24, now, by_hour))
    project_pie = _project_pie(project_rows)

    snapshot.update(
        {
            "available": True,
            "summary": summary,
            "by_project": project_rows,
            "by_model": _sorted_buckets(by_model),
            "by_day": day_rows,
            "by_hour": hour_rows,
            "day_axis": _chart_axis(day_rows),
            "hour_axis": _chart_axis(hour_rows),
            "project_pie": project_pie,
            "project_pie_gradient": _pie_gradient(project_pie),
            "recent": events[:25],
            "unmapped_count": unmapped_count,
        }
    )
    return snapshot


def _load_usage_events(
    conn: sqlite3.Connection,
    *,
    start_ts: int,
    limit: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, ts, thread_id, process_uuid, feedback_log_body
        FROM logs
        WHERE ts >= ?
          AND target = ?
          AND feedback_log_body LIKE 'SSE event:%response.completed%'
          AND feedback_log_body LIKE '%usage%'
        ORDER BY ts DESC
        LIMIT ?
        """,
        (start_ts, USAGE_TARGET, limit),
    ).fetchall()
    by_response_id: dict[str, dict[str, Any]] = {}
    anonymous_events: list[dict[str, Any]] = []
    for row in rows:
        event = _parse_usage_row(row)
        if event is None:
            continue
        response_id = event.get("response_id") or ""
        if response_id:
            existing = by_response_id.get(response_id)
            if existing is None or event["log_id"] > existing["log_id"]:
                by_response_id[response_id] = event
        else:
            anonymous_events.append(event)
    events = [*by_response_id.values(), *anonymous_events]
    events.sort(key=lambda item: (item["ts"], item["log_id"]), reverse=True)
    return events


def _parse_usage_row(row: sqlite3.Row) -> dict[str, Any] | None:
    body = row["feedback_log_body"] or ""
    if not body.startswith(SSE_PREFIX):
        return None
    try:
        event = json.loads(body[len(SSE_PREFIX) :])
    except json.JSONDecodeError:
        return None
    if event.get("type") != "response.completed":
        return None
    response = event.get("response") or {}
    if not isinstance(response, dict):
        return None
    usage = response.get("usage") or {}
    if not isinstance(usage, dict):
        return None

    input_details = usage.get("input_tokens_details") or {}
    output_details = usage.get("output_tokens_details") or {}
    if not isinstance(input_details, dict):
        input_details = {}
    if not isinstance(output_details, dict):
        output_details = {}

    ts = int(response.get("completed_at") or row["ts"] or 0)
    input_tokens = _int_value(usage.get("input_tokens"))
    output_tokens = _int_value(usage.get("output_tokens"))
    total_tokens = _int_value(usage.get("total_tokens")) or input_tokens + output_tokens
    return {
        "event_key": response.get("id") or f"log:{int(row['id'])}",
        "log_id": int(row["id"]),
        "ts": ts,
        "thread_id": row["thread_id"] or "",
        "cwd": "",
        "process_uuid": row["process_uuid"] or "",
        "response_id": response.get("id") or "",
        "model": response.get("model") or "unknown",
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "cached_tokens": _int_value(input_details.get("cached_tokens")),
        "reasoning_tokens": _int_value(output_details.get("reasoning_tokens")),
    }


def _cached_usage_events(store: Store, *, start_ts: int, limit: int) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for row in store.list_token_usage_events(start_ts=start_ts, limit=limit):
        project_name = row["project_name"] or "Unmapped"
        events.append(
            {
                "event_key": row["event_key"],
                "log_id": row["log_id"],
                "ts": row["ts"],
                "thread_id": row["thread_id"],
                "cwd": row["cwd"],
                "process_uuid": row["process_uuid"],
                "response_id": row["response_id"],
                "model": row["model"],
                "input_tokens": row["input_tokens"],
                "output_tokens": row["output_tokens"],
                "total_tokens": row["total_tokens"],
                "cached_tokens": row["cached_tokens"],
                "reasoning_tokens": row["reasoning_tokens"],
                "project_id": row["project_id"],
                "project": project_name,
                "project_path": "",
            }
        )
    return events


def _attach_metadata(
    conn: sqlite3.Connection,
    events: list[dict[str, Any]],
    *,
    start_ts: int,
    now: int,
) -> None:
    process_uuids = sorted({event["process_uuid"] for event in events if event["process_uuid"]})
    if not process_uuids:
        return
    by_process: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for chunk in _chunks(process_uuids, 700):
        placeholders = ",".join("?" for _ in chunk)
        params: list[Any] = [*chunk, start_ts - 600, now + 600]
        rows = conn.execute(
            f"""
            SELECT id, ts, thread_id, process_uuid, target, feedback_log_body
            FROM logs
            WHERE process_uuid IN ({placeholders})
              AND ts BETWEEN ? AND ?
              AND (
                thread_id IS NOT NULL
                OR target IN ({",".join("?" for _ in METADATA_TARGETS)})
                OR feedback_log_body LIKE '%cwd=%'
                OR feedback_log_body LIKE '%conversation.id=%'
                OR feedback_log_body LIKE '%thread.id=%'
              )
            ORDER BY process_uuid, ts
            """,
            [*params, *METADATA_TARGETS],
        ).fetchall()
        for row in rows:
            metadata = _parse_metadata_row(row)
            if metadata:
                by_process[metadata["process_uuid"]].append(metadata)

    for rows in by_process.values():
        rows.sort(key=lambda item: item["log_id"])

    for event in events:
        metadata = _stable_metadata_for_event(
            by_process.get(event["process_uuid"], []),
            event_log_id=event["log_id"],
            event_ts=event["ts"],
        )
        if metadata is None:
            continue
        if not event["thread_id"]:
            event["thread_id"] = metadata.get("thread_id") or ""
        event["cwd"] = metadata.get("cwd") or event["cwd"]


def _parse_metadata_row(row: sqlite3.Row) -> dict[str, Any] | None:
    body = row["feedback_log_body"] or ""
    thread_id = row["thread_id"] or ""
    if not thread_id:
        match = THREAD_RE.search(body)
        if match:
            thread_id = match.group(1)
    cwd = ""
    match = CWD_RE.search(body)
    if match:
        cwd = match.group(1)
    if not thread_id and not cwd:
        return None
    return {
        "log_id": int(row["id"]),
        "ts": int(row["ts"] or 0),
        "process_uuid": row["process_uuid"] or "",
        "thread_id": thread_id,
        "cwd": cwd,
    }


def _stable_metadata_for_event(
    rows: list[dict[str, Any]],
    *,
    event_log_id: int,
    event_ts: int,
    backward_window: int = 6 * 3600,
    forward_window: int = 5,
) -> dict[str, Any] | None:
    if not rows:
        return None
    log_ids = [row["log_id"] for row in rows]
    index = bisect.bisect_right(log_ids, event_log_id)

    thread_id = ""
    cwd = ""
    best_ts = 0
    for candidate in reversed(rows[:index]):
        if event_ts - candidate["ts"] > backward_window:
            break
        thread_id = thread_id or candidate.get("thread_id") or ""
        cwd = cwd or candidate.get("cwd") or ""
        best_ts = max(best_ts, candidate["ts"])
        if thread_id and cwd:
            return {"thread_id": thread_id, "cwd": cwd, "ts": best_ts}

    # Some completion bookkeeping is logged a few rows after the SSE event.
    # Keep this window tiny so later turns cannot remap older usage rows.
    for candidate in rows[index : index + 32]:
        if candidate["ts"] - event_ts > forward_window:
            break
        thread_id = thread_id or candidate.get("thread_id") or ""
        cwd = cwd or candidate.get("cwd") or ""
        best_ts = max(best_ts, candidate["ts"])
        if thread_id and cwd:
            return {"thread_id": thread_id, "cwd": cwd, "ts": best_ts}

    if thread_id or cwd:
        return {"thread_id": thread_id, "cwd": cwd, "ts": best_ts}
    return None


def _project_index(store: Store | None) -> dict[str, Any]:
    if store is None:
        return {"threads": {}, "paths": []}
    threads: dict[str, dict[str, Any]] = {}
    paths: list[dict[str, Any]] = []
    for project_row in store.list_projects():
        project = {
            "id": project_row["id"],
            "name": project_row["name"],
            "path": project_row["path"],
        }
        paths.append(project)
        default_thread_id = project_row["default_thread_id"]
        if default_thread_id:
            threads[default_thread_id] = project
        for session in store.list_sessions(project_row["id"]):
            threads[session["thread_id"]] = project
    paths.sort(key=lambda item: len(item["path"]), reverse=True)
    return {"threads": threads, "paths": paths}


def _match_project(event: dict[str, Any], index: dict[str, Any]) -> dict[str, Any] | None:
    thread_id = event.get("thread_id") or ""
    if thread_id and thread_id in index["threads"]:
        return index["threads"][thread_id]
    cwd = event.get("cwd") or ""
    if not cwd:
        return None
    try:
        cwd_path = Path(cwd).expanduser().resolve(strict=False)
    except OSError:
        return None
    for project in index["paths"]:
        try:
            project_path = Path(project["path"]).expanduser().resolve(strict=False)
        except OSError:
            continue
        if cwd_path == project_path or _is_relative_to(cwd_path, project_path):
            return project
    return None


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _add_to_bucket(buckets: dict[str, dict[str, Any]], key: str, event: dict[str, Any]) -> None:
    bucket = buckets.setdefault(key or "unknown", {"name": key or "unknown", **_empty_metrics(), "latest_ts": 0})
    _add_metrics(bucket, event)
    bucket["latest_ts"] = max(bucket["latest_ts"], event["ts"])


def _add_metrics(metrics: dict[str, int], event: dict[str, Any]) -> None:
    metrics["events"] += 1
    metrics["input_tokens"] += event["input_tokens"]
    metrics["output_tokens"] += event["output_tokens"]
    metrics["total_tokens"] += event["total_tokens"]
    metrics["cached_tokens"] += event["cached_tokens"]
    metrics["reasoning_tokens"] += event["reasoning_tokens"]


def _merge_metrics(metrics: dict[str, int], row: dict[str, Any]) -> None:
    metrics["events"] += row["events"]
    metrics["input_tokens"] += row["input_tokens"]
    metrics["output_tokens"] += row["output_tokens"]
    metrics["total_tokens"] += row["total_tokens"]
    metrics["cached_tokens"] += row["cached_tokens"]
    metrics["reasoning_tokens"] += row["reasoning_tokens"]


def _sorted_buckets(buckets: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        buckets.values(),
        key=lambda item: (item["total_tokens"], item.get("latest_ts", 0), item["name"]),
        reverse=True,
    )


def _filled_days(
    days: int,
    now: int,
    buckets: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    today = datetime.fromtimestamp(now).date()
    rows: list[dict[str, Any]] = []
    for offset in range(days - 1, -1, -1):
        day = (today - timedelta(days=offset)).isoformat()
        bucket = buckets.get(day)
        if bucket:
            rows.append({**bucket, "label": day[5:]})
        else:
            rows.append({"name": day, "label": day[5:], **_empty_metrics(), "latest_ts": 0})
    return rows


def _filled_hours(
    hours: int,
    now: int,
    buckets: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    current_hour = datetime.fromtimestamp(now).replace(minute=0, second=0, microsecond=0)
    rows: list[dict[str, Any]] = []
    for offset in range(hours - 1, -1, -1):
        hour_dt = current_hour - timedelta(hours=offset)
        key = hour_dt.strftime("%Y-%m-%d %H:00")
        bucket = buckets.get(key)
        label = hour_dt.strftime("%H:%M")
        if bucket:
            rows.append({**bucket, "label": label})
        else:
            rows.append({"name": key, "label": label, **_empty_metrics(), "latest_ts": 0})
    return rows


def _hour_key(ts: int) -> str:
    return datetime.fromtimestamp(ts).replace(minute=0, second=0, microsecond=0).strftime(
        "%Y-%m-%d %H:00"
    )


def _chart_bars(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    max_total = max([row["total_tokens"] for row in rows] or [0])
    chart_rows: list[dict[str, Any]] = []
    for row in rows:
        height_pct = 0
        if max_total > 0 and row["total_tokens"] > 0:
            height_pct = max(3, round(row["total_tokens"] / max_total * 100))
        chart_rows.append({**row, "height_pct": height_pct})
    return chart_rows


def _chart_axis(rows: list[dict[str, Any]]) -> dict[str, Any]:
    max_total = max([row["total_tokens"] for row in rows] or [0])
    mid_total = round(max_total / 2)
    return {
        "max_tokens": max_total,
        "mid_tokens": mid_total,
        "max_label": _compact_metric(max_total),
        "mid_label": _compact_metric(mid_total),
    }


def _project_pie(rows: list[dict[str, Any]], max_slices: int = 6) -> list[dict[str, Any]]:
    nonzero = [row for row in rows if row["total_tokens"] > 0]
    total = sum(row["total_tokens"] for row in nonzero)
    if total <= 0:
        return []
    visible = nonzero[:max_slices]
    rest = nonzero[max_slices:]
    if rest:
        other = {"name": "Other", **_empty_metrics(), "latest_ts": 0}
        for row in rest:
            _merge_metrics(other, row)
            other["latest_ts"] = max(other["latest_ts"], row.get("latest_ts", 0))
        visible.append(other)

    slices: list[dict[str, Any]] = []
    for index, row in enumerate(visible):
        percent = row["total_tokens"] / total * 100
        slices.append(
            {
                **row,
                "color": CHART_COLORS[index % len(CHART_COLORS)],
                "percent": round(percent, 1),
            }
        )
    return slices


def _pie_gradient(slices: list[dict[str, Any]]) -> str:
    if not slices:
        return "#eef1f5 0% 100%"
    cursor = 0.0
    parts: list[str] = []
    for index, item in enumerate(slices):
        percent = float(item.get("percent") or 0)
        end = 100.0 if index == len(slices) - 1 else min(100.0, cursor + percent)
        parts.append(f"{item['color']} {cursor:.2f}% {end:.2f}%")
        cursor = end
    return ", ".join(parts)


def _empty_metrics() -> dict[str, int]:
    return {
        "events": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cached_tokens": 0,
        "reasoning_tokens": 0,
    }


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _compact_metric(value: int) -> str:
    if abs(value) >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs(value) >= 10_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:,}"


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]
