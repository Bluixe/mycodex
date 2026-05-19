from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta
from typing import Any

from .db import Store
from .service import run_daily_summary, send_prompt
from .telegram import notify_project_bindings


def run_daemon(
    store: Store,
    *,
    interval_seconds: int = 300,
    once: bool = False,
    timeout: int = 1800,
    notify_telegram: bool = True,
    telegram_token_file: str | None = None,
    telegram_token_env: str = "MYCODEX_TELEGRAM_BOT_TOKEN",
    telegram_proxy_url: str | None = None,
) -> None:
    while True:
        run_due_rules(
            store,
            timeout=timeout,
            notify_telegram=notify_telegram,
            telegram_token_file=telegram_token_file,
            telegram_token_env=telegram_token_env,
            telegram_proxy_url=telegram_proxy_url,
        )
        if once:
            return
        time.sleep(interval_seconds)


def run_due_rules(
    store: Store,
    *,
    timeout: int = 1800,
    notify_telegram: bool = True,
    telegram_token_file: str | None = None,
    telegram_token_env: str = "MYCODEX_TELEGRAM_BOT_TOKEN",
    telegram_proxy_url: str | None = None,
) -> None:
    for project in store.list_projects():
        if project["daily_summary_enabled"]:
            try:
                _maybe_run_project_daily_summary(
                    store,
                    project,
                    timeout=timeout,
                    notify_telegram=notify_telegram,
                    telegram_token_file=telegram_token_file,
                    telegram_token_env=telegram_token_env,
                    telegram_proxy_url=telegram_proxy_url,
                )
            except Exception as exc:
                _log(f"daily_summary scheduler error project={project['name']} error={exc}")
        for rule in store.list_rules(project["id"]):
            if not rule["enabled"]:
                continue
            if _rule_due(rule):
                try:
                    _run_rule(
                        store,
                        project,
                        rule,
                        timeout=timeout,
                        notify_telegram=notify_telegram,
                        telegram_token_file=telegram_token_file,
                        telegram_token_env=telegram_token_env,
                        telegram_proxy_url=telegram_proxy_url,
                    )
                except Exception as exc:
                    _log(
                        f"rule failed project={project['name']} rule={rule['name']} "
                        f"kind={rule['kind']} error={exc}"
                    )


def _maybe_run_project_daily_summary(
    store: Store,
    project,
    *,
    timeout: int,
    notify_telegram: bool,
    telegram_token_file: str | None,
    telegram_token_env: str,
    telegram_proxy_url: str | None,
) -> None:
    now = datetime.now()
    scheduled = _today_at(project["daily_summary_time"])
    if now < scheduled:
        return
    day_start = int(datetime(now.year, now.month, now.day).timestamp())
    if store.has_run_since(project["id"], kind="daily_summary", since_ts=day_start):
        return
    _run_daily_summary_and_notify(
        store,
        project,
        timeout=timeout,
        reason="project_daily_summary",
        notify_telegram=notify_telegram,
        telegram_token_file=telegram_token_file,
        telegram_token_env=telegram_token_env,
        telegram_proxy_url=telegram_proxy_url,
    )


def _run_rule(
    store: Store,
    project,
    rule,
    *,
    timeout: int,
    notify_telegram: bool,
    telegram_token_file: str | None,
    telegram_token_env: str,
    telegram_proxy_url: str | None,
) -> None:
    _log(f"rule due project={project['name']} rule={rule['name']} kind={rule['kind']}")
    if rule["kind"] == "daily_summary":
        _run_daily_summary_and_notify(
            store,
            project,
            timeout=timeout,
            reason=f"rule:{rule['name']}",
            notify_telegram=notify_telegram,
            telegram_token_file=telegram_token_file,
            telegram_token_env=telegram_token_env,
            telegram_proxy_url=telegram_proxy_url,
        )
    else:
        result = send_prompt(
            store,
            project["name"],
            rule["prompt"],
            kind=f"rule:{rule['kind']}",
            timeout=timeout,
        )
        _log(
            f"rule completed project={project['name']} rule={rule['name']} "
            f"run=#{result['run_id']} status={result['status']} thread={result['thread_id']}"
        )
    store.update_rule_run(rule["id"])


def _run_daily_summary_and_notify(
    store: Store,
    project,
    *,
    timeout: int,
    reason: str,
    notify_telegram: bool,
    telegram_token_file: str | None,
    telegram_token_env: str,
    telegram_proxy_url: str | None,
) -> dict[str, Any]:
    _log(f"daily_summary due project={project['name']} reason={reason}")
    try:
        result = run_daily_summary(store, project["name"], timeout=timeout)
    except Exception as exc:
        _log(f"daily_summary failed project={project['name']} reason={reason} error={exc}")
        if notify_telegram:
            _notify_daily_summary_failure(
                store,
                project,
                exc,
                telegram_token_file=telegram_token_file,
                telegram_token_env=telegram_token_env,
                telegram_proxy_url=telegram_proxy_url,
            )
        raise
    _log(
        f"daily_summary completed project={project['name']} reason={reason} "
        f"run=#{result['run_id']} status={result['status']} thread={result['thread_id']}"
    )
    if notify_telegram:
        _notify_daily_summary_result(
            store,
            project,
            result,
            telegram_token_file=telegram_token_file,
            telegram_token_env=telegram_token_env,
            telegram_proxy_url=telegram_proxy_url,
        )
    return result


def _notify_daily_summary_result(
    store: Store,
    project,
    result: dict[str, Any],
    *,
    telegram_token_file: str | None,
    telegram_token_env: str,
    telegram_proxy_url: str | None,
) -> None:
    output = result.get("output_text") or "(no output)"
    text = (
        f"Daily summary for {project['name']} completed.\n"
        f"Run #{result.get('run_id')} {result.get('status')}\n"
        f"Thread: {result.get('thread_id') or '-'}\n\n"
        f"{output}"
    )
    _log_telegram_notification(
        project["name"],
        notify_project_bindings(
            store,
            project["id"],
            text,
            token_file=telegram_token_file,
            token_env=telegram_token_env,
            proxy_url=telegram_proxy_url,
        ),
    )


def _notify_daily_summary_failure(
    store: Store,
    project,
    exc: Exception,
    *,
    telegram_token_file: str | None,
    telegram_token_env: str,
    telegram_proxy_url: str | None,
) -> None:
    text = (
        f"Daily summary for {project['name']} failed.\n"
        f"Error: {exc}"
    )
    _log_telegram_notification(
        project["name"],
        notify_project_bindings(
            store,
            project["id"],
            text,
            token_file=telegram_token_file,
            token_env=telegram_token_env,
            proxy_url=telegram_proxy_url,
        ),
    )


def _log_telegram_notification(project_name: str, result: dict[str, Any]) -> None:
    error_text = "; ".join(result.get("errors") or [])
    suffix = f" errors={error_text}" if error_text else ""
    _log(
        f"telegram notify project={project_name} attempted={result.get('attempted', 0)} "
        f"sent={result.get('sent', 0)} skipped={result.get('skipped', 0)}{suffix}"
    )


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


def _log(message: str) -> None:
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {message}", file=sys.stderr, flush=True)
