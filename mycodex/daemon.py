from __future__ import annotations

import json
import sys
import time
import traceback
from datetime import datetime, timedelta
from typing import Any

from .db import Store
from .experiment_ops import recover_stale_experiment_tasks, run_experiment_watchdog
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
        try:
            run_due_rules(
                store,
                timeout=timeout,
                notify_telegram=notify_telegram,
                telegram_token_file=telegram_token_file,
                telegram_token_env=telegram_token_env,
                telegram_proxy_url=telegram_proxy_url,
            )
        except Exception as exc:
            _log(f"daemon cycle failed error={exc}")
            traceback.print_exc(file=sys.stderr)
        if once:
            return
        time.sleep(interval_seconds)


def run_single_rule_watch(
    store: Store,
    project_name: str,
    rule_name: str,
    *,
    interval_seconds: int = 60,
    once: bool = False,
    timeout: int = 1800,
    notify_telegram: bool = True,
    telegram_token_file: str | None = None,
    telegram_token_env: str = "MYCODEX_TELEGRAM_BOT_TOKEN",
    telegram_proxy_url: str | None = None,
) -> None:
    while True:
        try:
            run_single_rule_check(
                store,
                project_name,
                rule_name,
                timeout=timeout,
                notify_telegram=notify_telegram,
                telegram_token_file=telegram_token_file,
                telegram_token_env=telegram_token_env,
                telegram_proxy_url=telegram_proxy_url,
            )
        except Exception as exc:
            _log(
                f"single rule watch failed project={project_name} "
                f"rule={rule_name} error={exc}"
            )
            traceback.print_exc(file=sys.stderr)
        if once:
            return
        time.sleep(interval_seconds)


def run_single_rule_check(
    store: Store,
    project_name: str,
    rule_name: str,
    *,
    timeout: int = 1800,
    notify_telegram: bool = True,
    telegram_token_file: str | None = None,
    telegram_token_env: str = "MYCODEX_TELEGRAM_BOT_TOKEN",
    telegram_proxy_url: str | None = None,
) -> bool:
    project = store.get_project(project_name)
    if project is None:
        raise ValueError(f"unknown project: {project_name}")
    rule = next(
        (row for row in store.list_rules(project["id"]) if row["name"] == rule_name),
        None,
    )
    if rule is None:
        raise ValueError(f"unknown rule for {project_name}: {rule_name}")
    if not rule["enabled"]:
        _log(f"single rule watch skipped disabled project={project_name} rule={rule_name}")
        return False
    if not _rule_due(rule):
        next_in = _rule_next_due_seconds(rule)
        suffix = f" next_in={next_in}s" if next_in is not None else ""
        _log(f"single rule watch check project={project_name} rule={rule_name} due=no{suffix}")
        return False
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
    return True


def run_due_rules(
    store: Store,
    *,
    timeout: int = 1800,
    notify_telegram: bool = True,
    telegram_token_file: str | None = None,
    telegram_token_env: str = "MYCODEX_TELEGRAM_BOT_TOKEN",
    telegram_proxy_url: str | None = None,
) -> None:
    recovery = recover_stale_experiment_tasks(store, source="mycodex-daemon")
    if recovery["recovered_tasks"]:
        _log(f"recovered stale experiment tasks count={recovery['recovered_tasks']}")
    projects = list(store.list_projects())
    for project in projects:
        try:
            _run_due_experiment_watchdogs(store, project)
        except Exception as exc:
            _log(f"experiment watchdog scheduler error project={project['name']} error={exc}")

    for project in projects:
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
            thread_id=rule["thread_id"],
            timeout=timeout,
        )
        _log(
            f"rule completed project={project['name']} rule={rule['name']} "
            f"run=#{result['run_id']} status={result['status']} thread={result['thread_id']}"
        )
    store.update_rule_run(rule["id"])


def _run_due_experiment_watchdogs(store: Store, project) -> None:
    for experiment in store.list_experiments(project["id"]):
        if experiment["status"] != "running":
            continue
        config = _experiment_watchdog_config(experiment)
        if not config.get("enabled"):
            continue
        interval = max(1, _int_config(config, "interval_seconds", _int_config(config, "interval", 300)))
        now = int(time.time())
        last_at = _latest_experiment_watchdog_daemon_ts(store, experiment["id"])
        if last_at is not None and now - last_at < interval:
            continue
        control_timeout = _int_config(config, "control_timeout", 1800)
        if _recent_experiment_control_run_is_active(
            store,
            project,
            experiment,
            timeout_seconds=control_timeout,
            now=now,
        ):
            continue

        payload = {"watchdog": _redacted_watchdog_config(config), "interval_seconds": interval}
        store.add_experiment_event(
            experiment["id"],
            event_type="experiment_watchdog_daemon_started",
            level="info",
            message="Daemon started configured experiment watchdog cycle.",
            payload=payload,
            source="mycodex-daemon",
            status="running",
            phase=experiment["phase"],
            phase_message=experiment["phase_message"],
        )
        try:
            result = run_experiment_watchdog(
                store,
                project["name"],
                experiment_name=experiment["name"],
                run_path=config.get("run_path") or None,
                metrics=_list_config(config.get("metrics") or config.get("metric")),
                stop_conditions=_list_config(config.get("stop_conditions") or config.get("stop_condition")),
                fail_on_nonfinite=_bool_config(config, "fail_on_nonfinite", True),
                complete_on_finish=_bool_config(config, "complete_on_finish", False),
                remote_ref=config.get("remote_ref") or None,
                ssh_key=config.get("ssh_key") or None,
                ssh_password_file=config.get("ssh_password_file") or None,
                remote_state_root=config.get("remote_state_root") or None,
                remote_state_path=config.get("remote_state_path") or None,
                remote_events_path=config.get("remote_events_path") or None,
                remote_base=config.get("remote_base") or None,
                remote_python=config.get("remote_python") or None,
                remote_sync=_bool_config(config, "remote_sync", True),
                remote_event_tail=_int_config(config, "remote_event_tail", 200),
                remote_log_tail_lines=_int_config(config, "remote_log_tail_lines", 120),
                remote_sync_timeout=_int_config(config, "remote_sync_timeout", 90),
                remote_anomaly_control=_bool_config(config, "remote_anomaly_control", True),
                interval=interval,
                max_cycles=1,
                stale_seconds=_int_config(config, "stale_seconds", 900),
                supervisor_interval=_int_config(config, "supervisor_interval", 300),
                trigger_next_control=_bool_config(config, "trigger_next_control", True),
                auto_execute=_bool_config(config, "auto_execute", True),
                allow_full_train=_bool_config(config, "allow_full_train", False),
                monitor_only=_bool_config(
                    config,
                    "monitor_only",
                    _bool_config(config, "monitor_only_after_restart_protection", False),
                ),
                control_timeout=control_timeout,
                control_sandbox=config.get("control_sandbox") or config.get("sandbox") or None,
                control_approval_policy=config.get("control_approval_policy") or config.get("approval_policy") or None,
                control_model=config.get("control_model") or config.get("model") or None,
                control_reasoning_effort=config.get("control_reasoning_effort")
                or config.get("reasoning_effort")
                or None,
                peer_review_bypass_timeout_seconds=_int_config(
                    config,
                    "peer_review_bypass_timeout_seconds",
                    1800,
                ),
                peer_review_bypass_on=config.get("peer_review_bypass_on"),
                peer_review_bypass_allowed_classes=config.get("peer_review_bypass_classes")
                or config.get("peer_review_bypass_allowed_classes"),
            )
        except Exception as exc:
            store.add_experiment_event(
                experiment["id"],
                event_type="experiment_watchdog_daemon_failed",
                level="error",
                message=f"Daemon experiment watchdog cycle failed: {exc}",
                payload={**payload, "error": str(exc)},
                source="mycodex-daemon",
                phase="waiting_for_decision",
                phase_message="Configured daemon watchdog failed; inspect event payload.",
            )
            _log(
                f"experiment watchdog failed project={project['name']} "
                f"experiment={experiment['name']} error={exc}"
            )
            continue

        store.add_experiment_event(
            experiment["id"],
            event_type="experiment_watchdog_daemon_completed",
            level="info",
            message="Daemon configured experiment watchdog cycle completed.",
            payload={**payload, "result": result},
            source="mycodex-daemon",
            status="running",
            phase=experiment["phase"],
            phase_message=experiment["phase_message"],
        )
        _log(
            f"experiment watchdog completed project={project['name']} "
            f"experiment={experiment['name']}"
        )


def _recent_experiment_control_run_is_active(
    store: Store,
    project,
    experiment,
    *,
    timeout_seconds: int,
    now: int,
) -> bool:
    active_runs = store.active_runs_for_project(
        project["id"],
        kind_prefix=f"experiment:{experiment['id']}:",
    )
    if not active_runs:
        return False

    stale_runs: list[dict[str, Any]] = []
    for active_run in active_runs:
        age_seconds = max(0, now - int(active_run["started_at"]))
        stale_after = _experiment_codex_run_stale_after(
            str(active_run["kind"]),
            control_timeout_seconds=timeout_seconds,
        )
        payload = {
            "run_id": active_run["id"],
            "kind": active_run["kind"],
            "thread_id": active_run["thread_id"],
            "age_seconds": age_seconds,
            "stale_after_seconds": stale_after,
        }
        if age_seconds < stale_after:
            store.add_experiment_event(
                experiment["id"],
                event_type="experiment_watchdog_daemon_skipped_active_control",
                level="info",
                message="Daemon skipped watchdog cycle because an experiment Codex run is still active.",
                payload={**payload, "stale_runs_cleared": stale_runs},
                source="mycodex-daemon",
                status="running",
                phase=experiment["phase"],
                phase_message=experiment["phase_message"],
            )
            _log(
                f"experiment watchdog skipped active run project={project['name']} "
                f"experiment={experiment['name']} run=#{active_run['id']} age={age_seconds}s"
            )
            return True

        error = (
            "stale running experiment Codex run exceeded "
            f"{stale_after}s; daemon will retry watchdog cycle"
        )
        store.finish_run(int(active_run["id"]), status="failed", error=error)
        stale_payload = {**payload, "error": error}
        stale_runs.append(stale_payload)
        store.add_experiment_event(
            experiment["id"],
            event_type="experiment_control_run_marked_stale",
            level="warning",
            message="Daemon marked a stale running experiment Codex run as failed.",
            payload=stale_payload,
            source="mycodex-daemon",
            status="running",
            phase="analyzing",
            phase_message="Stale experiment Codex run was cleared; watchdog will retry.",
        )
        _log(
            f"experiment watchdog cleared stale active run project={project['name']} "
            f"experiment={experiment['name']} run=#{active_run['id']} age={age_seconds}s"
        )
    return False


def _experiment_codex_run_stale_after(kind: str, *, control_timeout_seconds: int) -> int:
    if kind.endswith(":control"):
        return max(60, control_timeout_seconds + 60)
    return max(60, min(control_timeout_seconds, 900) + 60)


def _experiment_watchdog_config(experiment) -> dict[str, Any]:
    try:
        config = json.loads(experiment["config_json"] or "{}")
    except json.JSONDecodeError:
        return {}
    if not isinstance(config, dict):
        return {}
    watchdog = config.get("watchdog") or config.get("experiment_watchdog")
    return watchdog if isinstance(watchdog, dict) else {}


def _latest_experiment_watchdog_daemon_ts(store: Store, experiment_id: int) -> int | None:
    latest: int | None = None
    for event in store.list_experiment_events(experiment_id, limit=200):
        if event["event_type"] not in {
            "experiment_watchdog_daemon_started",
            "experiment_watchdog_daemon_completed",
            "experiment_watchdog_daemon_failed",
            "watchdog_heartbeat",
            "watchdog_supervise_started",
            "remote_anomaly_control_started",
            "experiment_watchdog_daemon_skipped_active_control",
            "experiment_control_run_marked_stale",
        }:
            continue
        created_at = int(event["created_at"])
        latest = created_at if latest is None else max(latest, created_at)
    return latest


def _redacted_watchdog_config(config: dict[str, Any]) -> dict[str, Any]:
    redacted = dict(config)
    for key in ("ssh_password", "password", "token", "api_key"):
        if key in redacted:
            redacted[key] = "<redacted>"
    return redacted


def _list_config(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item for item in value.replace(",", " ").split() if item]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item)]
    return [str(value)]


def _int_config(config: dict[str, Any], key: str, default: int) -> int:
    try:
        return int(config.get(key, default))
    except (TypeError, ValueError):
        return default


def _bool_config(config: dict[str, Any], key: str, default: bool) -> bool:
    if key not in config:
        return default
    value = config.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off", ""}
    return bool(value)


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


def _rule_next_due_seconds(rule) -> int | None:
    now = datetime.now()
    last_run_at = rule["last_run_at"]
    if rule["interval_seconds"]:
        if not last_run_at:
            return 0
        due_at = datetime.fromtimestamp(last_run_at) + timedelta(seconds=rule["interval_seconds"])
        return max(0, int((due_at - now).total_seconds()))
    if rule["time_of_day"]:
        scheduled = _today_at(rule["time_of_day"])
        if now < scheduled:
            return max(0, int((scheduled - now).total_seconds()))
        day_start = int(datetime(now.year, now.month, now.day).timestamp())
        if not last_run_at or last_run_at < day_start:
            return 0
        tomorrow = scheduled + timedelta(days=1)
        return max(0, int((tomorrow - now).total_seconds()))
    return None


def _today_at(value: str):
    hour, minute = [int(part) for part in value.split(":", 1)]
    now = datetime.now()
    return datetime(now.year, now.month, now.day, hour, minute)


def _log(message: str) -> None:
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {message}", file=sys.stderr, flush=True)
