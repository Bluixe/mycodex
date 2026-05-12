from __future__ import annotations

import time
from datetime import datetime, timedelta

from .db import Store
from .service import run_daily_summary, send_prompt


def run_daemon(
    store: Store,
    *,
    interval_seconds: int = 300,
    once: bool = False,
    timeout: int = 1800,
) -> None:
    while True:
        run_due_rules(store, timeout=timeout)
        if once:
            return
        time.sleep(interval_seconds)


def run_due_rules(store: Store, *, timeout: int = 1800) -> None:
    for project in store.list_projects():
        if project["daily_summary_enabled"]:
            _maybe_run_project_daily_summary(store, project, timeout=timeout)
        for rule in store.list_rules(project["id"]):
            if not rule["enabled"]:
                continue
            if _rule_due(rule):
                if rule["kind"] == "daily_summary":
                    run_daily_summary(store, project["name"], timeout=timeout)
                else:
                    send_prompt(
                        store,
                        project["name"],
                        rule["prompt"],
                        kind=f"rule:{rule['kind']}",
                        timeout=timeout,
                    )
                store.update_rule_run(rule["id"])


def _maybe_run_project_daily_summary(store: Store, project, *, timeout: int) -> None:
    now = datetime.now()
    scheduled = _today_at(project["daily_summary_time"])
    if now < scheduled:
        return
    day_start = int(datetime(now.year, now.month, now.day).timestamp())
    if store.has_run_since(project["id"], kind="daily_summary", since_ts=day_start):
        return
    run_daily_summary(store, project["name"], timeout=timeout)


def _rule_due(rule) -> bool:
    now = datetime.now()
    last_run_at = rule["last_run_at"]
    if rule["interval_seconds"]:
        if not last_run_at:
            return True
        return datetime.fromtimestamp(last_run_at) + timedelta(seconds=rule["interval_seconds"]) <= now
    if rule["time_of_day"]:
        scheduled = _today_at(rule["time_of_day"])
        if now < scheduled:
            return False
        day_start = int(datetime(now.year, now.month, now.day).timestamp())
        return not last_run_at or last_run_at < day_start
    return False


def _today_at(value: str):
    hour, minute = [int(part) for part in value.split(":", 1)]
    now = datetime.now()
    return datetime(now.year, now.month, now.day, hour, minute)
