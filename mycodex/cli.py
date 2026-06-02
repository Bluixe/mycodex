from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from .daemon import run_daemon, run_single_rule_watch
from .db import Store
from .codex_state import discover_codex_projects
from .experiment_ops import (
    extract_execution_contract,
    git_commit_push,
    launch_remote_job,
    poll_wandb_run,
    recover_stale_experiment_tasks,
    record_execution_contract_task,
    remote_job_status,
    run_ready_experiment_tasks,
    run_experiment_watchdog,
    supervise_experiment,
    sync_remote_experiment_state,
)
from .launch_agent import (
    daemon_launch_agent_status,
    default_daemon_launch_agent_label,
    install_daemon_launch_agent,
    uninstall_daemon_launch_agent,
)
from .research_ops import (
    ensure_research_concept_preflight_task,
    find_research_threads,
    run_research_task,
    supervise_research,
    update_research_from_query,
    write_research_markdown,
)
from .service import (
    ensure_valid_approval_policy,
    ensure_valid_sandbox,
    format_ts,
    git_snapshot,
    run_daily_summary,
    send_prompt,
    start_experiment_control_turn,
    start_research_control_turn,
    sync_sessions,
)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "handler"):
        parser.print_help()
        return 2
    try:
        with Store(args.db) as store:
            args.handler(store, args)
        return 0
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mycodex")
    parser.add_argument("--db", default=None, help="SQLite DB path (default: data/my_codex.sqlite)")
    sub = parser.add_subparsers(dest="command")

    projects = sub.add_parser("projects", help="Manage projects")
    projects_sub = projects.add_subparsers(dest="projects_command", required=True)

    p_add = projects_sub.add_parser("add", help="Add or update a project")
    p_add.add_argument("name")
    p_add.add_argument("path")
    p_add.add_argument("--description", default="")
    p_add.add_argument("--sandbox", default="read-only", choices=sorted(["read-only", "workspace-write", "danger-full-access"]))
    p_add.add_argument("--approval-policy", default="never", choices=sorted(["untrusted", "on-failure", "on-request", "never"]))
    p_add.add_argument("--model")
    p_add.add_argument("--effort", dest="reasoning_effort")
    p_add.add_argument("--enable-daily-summary", action="store_true")
    p_add.add_argument("--daily-summary-time", default="18:00")
    p_add.set_defaults(handler=cmd_projects_add)

    p_connect = projects_sub.add_parser("connect", help="Add/update a project and sync its Codex sessions")
    p_connect.add_argument("name")
    p_connect.add_argument("path")
    p_connect.add_argument("--description", default="")
    p_connect.add_argument("--sandbox", default="read-only", choices=sorted(["read-only", "workspace-write", "danger-full-access"]))
    p_connect.add_argument("--approval-policy", default="never", choices=sorted(["untrusted", "on-failure", "on-request", "never"]))
    p_connect.add_argument("--model")
    p_connect.add_argument("--effort", dest="reasoning_effort")
    p_connect.add_argument("--enable-daily-summary", action="store_true")
    p_connect.add_argument("--daily-summary-time", default="18:00")
    p_connect.add_argument("--limit", type=int, default=100)
    p_connect.set_defaults(handler=cmd_projects_connect)

    p_list = projects_sub.add_parser("list", help="List projects")
    p_list.set_defaults(handler=cmd_projects_list)

    p_discover = projects_sub.add_parser("discover", help="Discover Codex projects from local session history")
    p_discover.add_argument("--limit", type=int, default=50)
    p_discover.add_argument("--search")
    p_discover.add_argument("--include-archived", action="store_true")
    p_discover.set_defaults(handler=cmd_projects_discover)

    p_status = projects_sub.add_parser("status", help="Show project status")
    p_status.add_argument("name")
    p_status.set_defaults(handler=cmd_projects_status)

    p_remove = projects_sub.add_parser("remove", help="Remove a project from mycodex")
    p_remove.add_argument("name")
    p_remove.add_argument("--yes", action="store_true", help="Do not ask for confirmation")
    p_remove.set_defaults(handler=cmd_projects_remove)

    sessions = sub.add_parser("sessions", help="Manage Codex sessions")
    sessions_sub = sessions.add_subparsers(dest="sessions_command", required=True)

    s_sync = sessions_sub.add_parser("sync", help="Sync Codex sessions for a project")
    s_sync.add_argument("--project", required=True)
    s_sync.add_argument("--limit", type=int, default=100)
    s_sync.add_argument("--scan-rollouts", action="store_true", help="Ask Codex to repair metadata by scanning rollout JSONL")
    s_sync.set_defaults(handler=cmd_sessions_sync)

    s_list = sessions_sub.add_parser("list", help="List stored sessions")
    s_list.add_argument("--project", required=True)
    s_list.set_defaults(handler=cmd_sessions_list)

    s_default = sessions_sub.add_parser("set-default", help="Set the default session thread")
    s_default.add_argument("--project", required=True)
    s_default.add_argument("--thread-id", required=True)
    s_default.set_defaults(handler=cmd_sessions_set_default)

    send = sub.add_parser("send", help="Send a prompt to a project Codex session")
    send.add_argument("--project", required=True)
    send.add_argument("prompt", nargs="+")
    send.add_argument("--new-thread", action="store_true")
    send.add_argument("--timeout", type=int, default=1800)
    send.add_argument("--no-sync", action="store_true")
    send.add_argument("--sandbox", choices=sorted(["read-only", "workspace-write", "danger-full-access"]))
    send.add_argument("--approval-policy", choices=sorted(["untrusted", "on-failure", "on-request", "never"]))
    send.add_argument("--model")
    send.add_argument("--effort", dest="reasoning_effort")
    send.set_defaults(handler=cmd_send)

    daily = sub.add_parser("daily-summary", help="Generate a daily summary for a project")
    daily.add_argument("--project", required=True)
    daily.add_argument("--timeout", type=int, default=1800)
    daily.add_argument("--no-sync", action="store_true")
    daily.set_defaults(handler=cmd_daily_summary)

    daemon = sub.add_parser("daemon", help="Run local scheduled rules")
    daemon.add_argument("--interval", type=int, default=300)
    daemon.add_argument("--once", action="store_true")
    daemon.add_argument("--timeout", type=int, default=1800)
    daemon.add_argument("--allow-full-train", action="store_true")
    daemon.add_argument("--no-telegram-notify", action="store_true")
    daemon.add_argument("--telegram-token-file")
    daemon.add_argument("--telegram-token-env", default="MYCODEX_TELEGRAM_BOT_TOKEN")
    daemon.add_argument("--telegram-proxy-url")
    daemon.add_argument("--install-launch-agent", action="store_true", help="Install and bootstrap a macOS LaunchAgent for this daemon")
    daemon.add_argument("--uninstall-launch-agent", action="store_true", help="Unload and remove the macOS LaunchAgent for this daemon")
    daemon.add_argument("--launch-agent-status", action="store_true", help="Show macOS LaunchAgent install/load status")
    daemon.add_argument("--launch-agent-label", help="Override the LaunchAgent label")
    daemon.add_argument("--launch-agent-log-dir", help="Directory for LaunchAgent stdout/stderr logs")
    daemon.add_argument("--no-bootstrap", action="store_true", help="Write the LaunchAgent plist without loading it")
    daemon.set_defaults(handler=cmd_daemon)

    web = sub.add_parser("web", help="Run the local web dashboard")
    web.add_argument("--host", default="127.0.0.1")
    web.add_argument("--port", type=int, default=8765)
    web.add_argument("--reload", action="store_true")
    web.set_defaults(handler=cmd_web)

    telegram = sub.add_parser("telegram", help="Run and manage the Telegram adapter")
    telegram_sub = telegram.add_subparsers(dest="telegram_command", required=True)

    tg_run = telegram_sub.add_parser("run", help="Run the Telegram long-polling bot")
    tg_run.add_argument("--token")
    tg_run.add_argument("--token-file")
    tg_run.add_argument("--token-env", default="MYCODEX_TELEGRAM_BOT_TOKEN")
    tg_run.add_argument("--allow-user", action="append", default=[])
    tg_run.add_argument("--poll-timeout", type=int, default=30)
    tg_run.add_argument("--codex-timeout", type=int, default=1800)
    tg_run.add_argument("--proxy-url")
    tg_run.set_defaults(handler=cmd_telegram_run)

    tg_allow = telegram_sub.add_parser("allow", help="Allow a Telegram user id")
    tg_allow.add_argument("user_id")
    tg_allow.add_argument("--username", default="")
    tg_allow.set_defaults(handler=cmd_telegram_allow)

    tg_deny = telegram_sub.add_parser("deny", help="Deny a Telegram user id")
    tg_deny.add_argument("user_id")
    tg_deny.set_defaults(handler=cmd_telegram_deny)

    tg_users = telegram_sub.add_parser("users", help="List Telegram users")
    tg_users.set_defaults(handler=cmd_telegram_users)

    tg_bindings = telegram_sub.add_parser("bindings", help="List Telegram bindings")
    tg_bindings.set_defaults(handler=cmd_telegram_bindings)

    tg_bind = telegram_sub.add_parser("bind", help="Bind a Telegram chat/topic to a project")
    tg_bind.add_argument("chat_id")
    tg_bind.add_argument("project")
    tg_bind.add_argument("--message-thread-id")
    tg_bind.add_argument("--thread-id")
    tg_bind.set_defaults(handler=cmd_telegram_bind)

    tg_unbind = telegram_sub.add_parser("unbind", help="Remove a Telegram chat/topic binding")
    tg_unbind.add_argument("chat_id")
    tg_unbind.add_argument("--message-thread-id")
    tg_unbind.set_defaults(handler=cmd_telegram_unbind)

    rules = sub.add_parser("rules", help="Manage local scheduled rules")
    rules_sub = rules.add_subparsers(dest="rules_command", required=True)
    r_list = rules_sub.add_parser("list", help="List rules")
    r_list.add_argument("--project")
    r_list.set_defaults(handler=cmd_rules_list)
    r_add = rules_sub.add_parser("add", help="Add or update a custom prompt rule")
    r_add.add_argument("--project", required=True)
    r_add.add_argument("--name", required=True)
    r_add.add_argument("--kind", default="custom")
    r_add.add_argument("--thread-id")
    r_add.add_argument("--time-of-day")
    r_add.add_argument("--interval-seconds", type=int)
    r_add.add_argument("--prompt", required=True)
    r_add.add_argument("--disabled", action="store_true")
    r_add.set_defaults(handler=cmd_rules_add)
    r_watch = rules_sub.add_parser("watch", help="Run a scheduler loop for one rule")
    r_watch.add_argument("--project", required=True)
    r_watch.add_argument("--name", required=True)
    r_watch.add_argument("--interval", type=int, default=60)
    r_watch.add_argument("--once", action="store_true")
    r_watch.add_argument("--timeout", type=int, default=1800)
    r_watch.add_argument("--no-telegram-notify", action="store_true")
    r_watch.add_argument("--telegram-token-file")
    r_watch.add_argument("--telegram-token-env", default="MYCODEX_TELEGRAM_BOT_TOKEN")
    r_watch.add_argument("--telegram-proxy-url")
    r_watch.set_defaults(handler=cmd_rules_watch)

    runs = sub.add_parser("runs", help="Inspect run history")
    runs_sub = runs.add_subparsers(dest="runs_command", required=True)
    runs_list = runs_sub.add_parser("list", help="List project runs")
    runs_list.add_argument("--project", required=True)
    runs_list.add_argument("--limit", type=int, default=10)
    runs_list.set_defaults(handler=cmd_runs_list)

    experiments = sub.add_parser("experiments", help="Manage self-iterating experiments")
    experiments_sub = experiments.add_subparsers(dest="experiments_command", required=True)

    exp_create = experiments_sub.add_parser("create", help="Create or update an experiment")
    exp_create.add_argument("--project", required=True)
    exp_create.add_argument("--name", required=True)
    exp_create.add_argument("--thread-id", required=True)
    exp_create.add_argument("--objective", default="")
    exp_create.add_argument("--max-iterations", type=int)
    exp_create.add_argument("--remote-ref", default="")
    exp_create.add_argument("--config-json", default="{}")
    exp_create.add_argument(
        "--status",
        default="planned",
        choices=sorted(["planned", "running", "paused", "completed", "failed", "archived"]),
    )
    exp_create.set_defaults(handler=cmd_experiments_create)

    exp_list = experiments_sub.add_parser("list", help="List project experiments")
    exp_list.add_argument("--project", required=True)
    exp_list.add_argument("--include-archived", action="store_true")
    exp_list.set_defaults(handler=cmd_experiments_list)

    exp_status = experiments_sub.add_parser("status", help="Show one experiment and recent events")
    exp_status.add_argument("--project", required=True)
    exp_status.add_argument("--name", required=True)
    exp_status.add_argument("--limit", type=int, default=20)
    exp_status.set_defaults(handler=cmd_experiments_status)

    exp_watch = experiments_sub.add_parser("watch", help="Continuously show experiment phase and event stream")
    exp_watch.add_argument("--project", required=True)
    exp_watch.add_argument("--name", required=True)
    exp_watch.add_argument("--limit", type=int, default=25)
    exp_watch.add_argument("--interval", type=float, default=10.0)
    exp_watch.add_argument("--once", action="store_true")
    exp_watch.add_argument("--include-deltas", action="store_true", help="Include streamed agent message chunks")
    exp_watch.set_defaults(handler=cmd_experiments_watch)

    exp_event = experiments_sub.add_parser("event", help="Record an experiment event")
    exp_event.add_argument("--project", required=True)
    exp_event.add_argument("--name", required=True)
    exp_event.add_argument("--type", dest="event_type", required=True)
    exp_event.add_argument("--iteration", type=int)
    exp_event.add_argument(
        "--level",
        default="info",
        choices=sorted(["debug", "info", "warning", "error"]),
    )
    exp_event.add_argument("--message", default="")
    exp_event.add_argument("--payload-json", default="{}")
    exp_event.add_argument("--source", default="manual")
    exp_event.add_argument("--run-id", type=int)
    exp_event.add_argument("--phase")
    exp_event.add_argument("--phase-message")
    exp_event.add_argument(
        "--status",
        choices=sorted(["planned", "running", "paused", "completed", "failed", "archived"]),
    )
    exp_event.set_defaults(handler=cmd_experiments_event)

    exp_set_status = experiments_sub.add_parser("set-status", help="Update experiment status")
    exp_set_status.add_argument("--project", required=True)
    exp_set_status.add_argument("--name", required=True)
    exp_set_status.add_argument(
        "status",
        choices=sorted(["planned", "running", "paused", "completed", "failed", "archived"]),
    )
    exp_set_status.set_defaults(handler=cmd_experiments_set_status)

    exp_start = experiments_sub.add_parser("start", help="Start a Codex control turn")
    exp_start.add_argument("--project", required=True)
    exp_start.add_argument("--name", required=True)
    exp_start.add_argument("--timeout", type=int, default=1800)
    exp_start.add_argument("--sandbox", choices=sorted(["read-only", "workspace-write", "danger-full-access"]))
    exp_start.add_argument("--approval-policy", choices=sorted(["untrusted", "on-failure", "on-request", "never"]))
    exp_start.add_argument("--model")
    exp_start.add_argument("--effort", dest="reasoning_effort")
    exp_start.set_defaults(handler=cmd_experiments_start)

    exp_task_list = experiments_sub.add_parser("task-list", help="List durable experiment protocol tasks")
    exp_task_list.add_argument("--project", required=True)
    exp_task_list.add_argument("--name", required=True)
    exp_task_list.add_argument("--status", action="append", default=[])
    exp_task_list.add_argument("--limit", type=int, default=20)
    exp_task_list.set_defaults(handler=cmd_experiments_task_list)

    exp_task_approve = experiments_sub.add_parser("task-approve", help="Mark a proposed experiment task ready")
    exp_task_approve.add_argument("--project", required=True)
    exp_task_approve.add_argument("--name", required=True)
    exp_task_approve.add_argument("--task-id", type=int, required=True)
    exp_task_approve.set_defaults(handler=cmd_experiments_task_approve)

    exp_task_run = experiments_sub.add_parser("task-run", help="Run ready durable experiment protocol tasks")
    exp_task_run.add_argument("--project", required=True)
    exp_task_run.add_argument("--name", required=True)
    exp_task_run.add_argument("--owner", default="mycodex-cli")
    exp_task_run.add_argument("--lease-seconds", type=int, default=900)
    exp_task_run.add_argument("--max-tasks", type=int, default=1)
    exp_task_run.add_argument("--remote-ref")
    exp_task_run.add_argument("--ssh-key")
    exp_task_run.add_argument("--ssh-password-file")
    exp_task_run.add_argument("--remote-python")
    exp_task_run.add_argument("--allow-full-train", action="store_true")
    exp_task_run.set_defaults(handler=cmd_experiments_task_run)

    exp_task_recover = experiments_sub.add_parser("task-recover", help="Recover stale leased/running experiment protocol tasks")
    exp_task_recover.add_argument("--project")
    exp_task_recover.add_argument("--name")
    exp_task_recover.add_argument("--running-timeout-seconds", type=int, default=3600)
    exp_task_recover.set_defaults(handler=cmd_experiments_task_recover)

    exp_git_push = experiments_sub.add_parser("git-push", help="Commit selected local changes and push")
    exp_git_push.add_argument("--project", required=True)
    exp_git_push.add_argument("--name", required=True)
    exp_git_push.add_argument("--message", dest="commit_message")
    exp_git_push.add_argument("--all", dest="all_changes", action="store_true")
    exp_git_push.add_argument("--path", action="append", default=[])
    exp_git_push.add_argument("--remote", default="origin")
    exp_git_push.add_argument("--branch")
    exp_git_push.add_argument("--dry-run", action="store_true")
    exp_git_push.set_defaults(handler=cmd_experiments_git_push)

    exp_launch = experiments_sub.add_parser("launch-remote", help="Sync remote repo and start a background job")
    exp_launch.add_argument("--project", required=True)
    exp_launch.add_argument("--name", required=True)
    exp_launch.add_argument("--command", required=True)
    exp_launch.add_argument("--remote-ref")
    exp_launch.add_argument("--ssh-key")
    exp_launch.add_argument("--ssh-password-file")
    exp_launch.add_argument("--commit")
    exp_launch.add_argument("--branch")
    exp_launch.add_argument("--allow-dirty", action="store_true")
    exp_launch.add_argument("--no-fetch", action="store_true")
    exp_launch.add_argument("--log-path")
    exp_launch.add_argument("--pid-path")
    exp_launch.add_argument("--env-json", default="{}")
    exp_launch.add_argument("--wandb-entity")
    exp_launch.add_argument("--wandb-project")
    exp_launch.add_argument("--wandb-run-id")
    exp_launch.add_argument("--wandb-run-name")
    exp_launch.add_argument("--timeout", type=int, default=120)
    exp_launch.add_argument("--post-launch-check-delay", type=float, default=2.0)
    exp_launch.add_argument("--post-launch-check-lines", type=int, default=80)
    exp_launch.add_argument("--no-post-launch-check", action="store_true")
    exp_launch.add_argument("--dry-run", action="store_true")
    exp_launch.set_defaults(handler=cmd_experiments_launch_remote)

    exp_remote_status = experiments_sub.add_parser("remote-status", help="Check remote PID and recent log tail")
    exp_remote_status.add_argument("--project", required=True)
    exp_remote_status.add_argument("--name", required=True)
    exp_remote_status.add_argument("--remote-ref")
    exp_remote_status.add_argument("--ssh-key")
    exp_remote_status.add_argument("--ssh-password-file")
    exp_remote_status.add_argument("--pid")
    exp_remote_status.add_argument("--log-path")
    exp_remote_status.add_argument("--lines", type=int, default=40)
    exp_remote_status.add_argument("--timeout", type=int, default=30)
    exp_remote_status.set_defaults(handler=cmd_experiments_remote_status)

    exp_sync_remote = experiments_sub.add_parser("sync-remote", help="Sync structured remote experiment state into local events")
    exp_sync_remote.add_argument("--project", required=True)
    exp_sync_remote.add_argument("--name", required=True)
    exp_sync_remote.add_argument("--remote-ref")
    exp_sync_remote.add_argument("--ssh-key")
    exp_sync_remote.add_argument("--ssh-password-file")
    exp_sync_remote.add_argument("--remote-state-root")
    exp_sync_remote.add_argument("--remote-state-path")
    exp_sync_remote.add_argument("--remote-events-path")
    exp_sync_remote.add_argument("--remote-base")
    exp_sync_remote.add_argument("--remote-python")
    exp_sync_remote.add_argument("--event-tail", type=int, default=200)
    exp_sync_remote.add_argument("--log-tail-lines", type=int, default=120)
    exp_sync_remote.add_argument("--stale-seconds", type=int, default=900)
    exp_sync_remote.add_argument("--timeout", type=int, default=90)
    exp_sync_remote.set_defaults(handler=cmd_experiments_sync_remote)

    exp_poll = experiments_sub.add_parser("poll-wandb", help="Poll W&B and record metric/decision events")
    exp_poll.add_argument("--project", required=True)
    exp_poll.add_argument("--name", required=True)
    exp_poll.add_argument("--run-path", help="W&B run path: entity/project/run_id")
    exp_poll.add_argument("--wandb-entity")
    exp_poll.add_argument("--wandb-project")
    exp_poll.add_argument("--wandb-run-id")
    exp_poll.add_argument("--metric", action="append", default=[])
    exp_poll.add_argument("--history-limit", type=int, default=5)
    exp_poll.add_argument("--stop-condition", action="append", default=[])
    exp_poll.add_argument("--fail-on-nonfinite", action="store_true")
    exp_poll.add_argument("--complete-on-finish", action="store_true")
    exp_poll.add_argument("--interval", type=int, default=0)
    exp_poll.add_argument("--max-polls", type=int, default=1)
    exp_poll.add_argument("--trigger-next-control", action="store_true")
    exp_poll.add_argument("--control-timeout", type=int, default=1800)
    exp_poll.add_argument("--sandbox", choices=sorted(["read-only", "workspace-write", "danger-full-access"]))
    exp_poll.add_argument("--approval-policy", choices=sorted(["untrusted", "on-failure", "on-request", "never"]))
    exp_poll.set_defaults(handler=cmd_experiments_poll_wandb)

    exp_supervise = experiments_sub.add_parser("supervise", help="Run an experiment supervisor loop")
    exp_supervise.add_argument("--project", required=True)
    exp_supervise.add_argument("--name", required=True)
    exp_supervise.add_argument("--run-path", help="W&B run path: entity/project/run_id")
    exp_supervise.add_argument("--metric", action="append", default=[])
    exp_supervise.add_argument("--stop-condition", action="append", default=[])
    exp_supervise.add_argument("--no-fail-on-nonfinite", action="store_true")
    exp_supervise.add_argument("--complete-on-finish", action="store_true")
    exp_supervise.add_argument("--remote-ref")
    exp_supervise.add_argument("--ssh-key")
    exp_supervise.add_argument("--ssh-password-file")
    exp_supervise.add_argument("--remote-python")
    exp_supervise.add_argument("--pid", help="Remote process PID to monitor when the job was attached manually")
    exp_supervise.add_argument("--log-path", help="Remote log path to tail when the job was attached manually")
    exp_supervise.add_argument("--interval", type=int, default=300)
    exp_supervise.add_argument("--max-cycles", type=int, default=1)
    exp_supervise.add_argument("--no-trigger-next-control", action="store_true")
    exp_supervise.add_argument("--no-auto-execute", action="store_true")
    exp_supervise.add_argument("--allow-full-train", action="store_true")
    exp_supervise.add_argument("--control-timeout", type=int, default=1800)
    exp_supervise.add_argument("--peer-review-bypass-timeout-seconds", type=int, default=1800)
    exp_supervise.add_argument(
        "--peer-review-bypass-on",
        default="revise,timeout",
        help="Comma-separated peer-review outcomes eligible for timed bypass; use 'none' to disable",
    )
    exp_supervise.add_argument(
        "--peer-review-bypass-classes",
        default="read-only-preflight,diagnostic-patch,smoke-train,monitor-only,full-train",
        help="Comma-separated execution classes eligible for timed peer-review bypass",
    )
    exp_supervise.add_argument("--sandbox", choices=sorted(["read-only", "workspace-write", "danger-full-access"]))
    exp_supervise.add_argument("--approval-policy", choices=sorted(["untrusted", "on-failure", "on-request", "never"]))
    exp_supervise.add_argument("--model")
    exp_supervise.add_argument("--reasoning-effort")
    exp_supervise.set_defaults(handler=cmd_experiments_supervise)

    exp_watchdog = experiments_sub.add_parser("watchdog", help="Restart one-cycle supervision when experiment activity is stale")
    exp_watchdog.add_argument("--project", required=True)
    exp_watchdog.add_argument("--name", help="Experiment name. Defaults to all running experiments in the project.")
    exp_watchdog.add_argument("--run-path", help="W&B run path: entity/project/run_id")
    exp_watchdog.add_argument("--metric", action="append", default=[])
    exp_watchdog.add_argument("--stop-condition", action="append", default=[])
    exp_watchdog.add_argument("--no-fail-on-nonfinite", action="store_true")
    exp_watchdog.add_argument("--complete-on-finish", action="store_true")
    exp_watchdog.add_argument("--remote-ref")
    exp_watchdog.add_argument("--ssh-key")
    exp_watchdog.add_argument("--ssh-password-file")
    exp_watchdog.add_argument("--remote-state-root")
    exp_watchdog.add_argument("--remote-state-path")
    exp_watchdog.add_argument("--remote-events-path")
    exp_watchdog.add_argument("--remote-base")
    exp_watchdog.add_argument("--remote-python")
    exp_watchdog.add_argument("--no-remote-sync", action="store_true")
    exp_watchdog.add_argument("--remote-event-tail", type=int, default=200)
    exp_watchdog.add_argument("--remote-log-tail-lines", type=int, default=120)
    exp_watchdog.add_argument("--remote-sync-timeout", type=int, default=90)
    exp_watchdog.add_argument("--no-remote-anomaly-control", action="store_true")
    exp_watchdog.add_argument("--interval", type=int, default=300, help="Seconds between watchdog checks")
    exp_watchdog.add_argument("--max-cycles", type=int, default=1, help="Watchdog check cycles")
    exp_watchdog.add_argument("--stale-seconds", type=int, default=900, help="Run supervision when activity is older than this")
    exp_watchdog.add_argument("--supervisor-interval", type=int, default=300, help="Interval passed to the one-cycle supervisor")
    exp_watchdog.add_argument("--no-trigger-next-control", action="store_true")
    exp_watchdog.add_argument("--no-auto-execute", action="store_true")
    exp_watchdog.add_argument("--allow-full-train", action="store_true")
    exp_watchdog.add_argument("--monitor-only", action="store_true", help="Only record remote/local watchdog observations; do not execute tasks or run supervisor")
    exp_watchdog.add_argument("--control-timeout", type=int, default=1800)
    exp_watchdog.add_argument("--peer-review-bypass-timeout-seconds", type=int, default=1800)
    exp_watchdog.add_argument(
        "--peer-review-bypass-on",
        default="revise,timeout",
        help="Comma-separated peer-review outcomes eligible for timed bypass; use 'none' to disable",
    )
    exp_watchdog.add_argument(
        "--peer-review-bypass-classes",
        default="read-only-preflight,diagnostic-patch,smoke-train,monitor-only,full-train",
        help="Comma-separated execution classes eligible for timed peer-review bypass",
    )
    exp_watchdog.add_argument("--sandbox", choices=sorted(["read-only", "workspace-write", "danger-full-access"]))
    exp_watchdog.add_argument("--approval-policy", choices=sorted(["untrusted", "on-failure", "on-request", "never"]))
    exp_watchdog.add_argument("--model")
    exp_watchdog.add_argument("--reasoning-effort")
    exp_watchdog.add_argument("--force", action="store_true", help="Run one supervisor cycle even if recent activity is fresh")
    exp_watchdog.set_defaults(handler=cmd_experiments_watchdog)

    research = sub.add_parser("research", help="Manage iterative research threads")
    research_sub = research.add_subparsers(dest="research_command", required=True)

    research_create = research_sub.add_parser("create", help="Create or update a research thread")
    research_create.add_argument("--project", required=True)
    research_create.add_argument("--name", required=True)
    research_create.add_argument("--thread-id", required=True)
    research_create.add_argument("--question", default="")
    research_create.add_argument("--max-iterations", type=int)
    research_create.add_argument("--config-json", default="{}")
    research_create.add_argument(
        "--status",
        default="planned",
        choices=sorted(["planned", "running", "paused", "completed", "failed", "archived"]),
    )
    research_create.set_defaults(handler=cmd_research_create)

    research_list = research_sub.add_parser("list", help="List project research threads")
    research_list.add_argument("--project", required=True)
    research_list.add_argument("--include-archived", action="store_true")
    research_list.set_defaults(handler=cmd_research_list)

    research_find = research_sub.add_parser("find", help="Find prior research by topic")
    research_find.add_argument("--project", required=True)
    research_find.add_argument("--query", required=True)
    research_find.add_argument("--limit", type=int, default=5)
    research_find.add_argument("--include-archived", action="store_true")
    research_find.set_defaults(handler=cmd_research_find)

    research_status = research_sub.add_parser("status", help="Show a research thread and recent ledger")
    research_status.add_argument("--project", required=True)
    research_status.add_argument("--name", required=True)
    research_status.add_argument("--limit", type=int, default=10)
    research_status.set_defaults(handler=cmd_research_status)

    research_event = research_sub.add_parser("event", help="Record a research event")
    research_event.add_argument("--project", required=True)
    research_event.add_argument("--name", required=True)
    research_event.add_argument("--type", dest="event_type", required=True)
    research_event.add_argument("--iteration", type=int)
    research_event.add_argument("--level", default="info", choices=sorted(["debug", "info", "warning", "error"]))
    research_event.add_argument("--message", default="")
    research_event.add_argument("--payload-json", default="{}")
    research_event.add_argument("--source", default="manual")
    research_event.add_argument("--run-id", type=int)
    research_event.add_argument("--phase")
    research_event.add_argument("--phase-message")
    research_event.add_argument(
        "--status",
        choices=sorted(["planned", "running", "paused", "completed", "failed", "archived"]),
    )
    research_event.set_defaults(handler=cmd_research_event)

    research_start = research_sub.add_parser("start", help="Start a Codex research control turn")
    research_start.add_argument("--project", required=True)
    research_start.add_argument("--name", required=True)
    research_start.add_argument("--timeout", type=int, default=1800)
    research_start.add_argument("--sandbox", choices=sorted(["read-only", "workspace-write", "danger-full-access"]))
    research_start.add_argument("--approval-policy", choices=sorted(["untrusted", "on-failure", "on-request", "never"]))
    research_start.add_argument("--model")
    research_start.add_argument("--effort", dest="reasoning_effort")
    research_start.set_defaults(handler=cmd_research_start)

    research_run_task = research_sub.add_parser("run-task", help="Run one queued research task")
    research_run_task.add_argument("--project", required=True)
    research_run_task.add_argument("--name", required=True)
    research_run_task.add_argument("--task-id", type=int)
    research_run_task.add_argument("--provider", action="append", default=[])
    research_run_task.add_argument("--limit", type=int, default=5)
    research_run_task.add_argument("--timeout", type=int, default=60)
    research_run_task.add_argument("--max-chars", type=int, default=12000)
    research_run_task.add_argument("--no-control", action="store_true")
    research_run_task.set_defaults(handler=cmd_research_run_task)

    research_supervise = research_sub.add_parser("supervise", help="Run a research supervisor loop")
    research_supervise.add_argument("--project", required=True)
    research_supervise.add_argument("--name", required=True)
    research_supervise.add_argument("--max-cycles", type=int, default=1)
    research_supervise.add_argument("--interval", type=int, default=300)
    research_supervise.add_argument("--no-trigger-control", action="store_true")
    research_supervise.add_argument("--control-timeout", type=int, default=1800)
    research_supervise.add_argument("--sandbox", choices=sorted(["read-only", "workspace-write", "danger-full-access"]))
    research_supervise.add_argument("--approval-policy", choices=sorted(["untrusted", "on-failure", "on-request", "never"]))
    research_supervise.add_argument("--model")
    research_supervise.add_argument("--effort", dest="reasoning_effort")
    research_supervise.add_argument("--provider", action="append", default=[])
    research_supervise.add_argument("--limit", type=int, default=5)
    research_supervise.add_argument("--task-timeout", type=int, default=60)
    research_supervise.add_argument("--max-chars", type=int, default=12000)
    research_supervise.set_defaults(handler=cmd_research_supervise)

    research_update = research_sub.add_parser("update", help="Find a prior research thread and run an incremental update")
    research_update.add_argument("--project", required=True)
    research_update.add_argument("--query", required=True)
    research_update.add_argument("--name", help="Use an exact research name instead of topic search")
    research_update.add_argument("--match-limit", type=int, default=5)
    research_update.add_argument("--max-cycles", type=int, default=5)
    research_update.add_argument("--interval", type=int, default=300)
    research_update.add_argument("--no-trigger-control", action="store_true")
    research_update.add_argument("--control-timeout", type=int, default=1800)
    research_update.add_argument("--sandbox", choices=sorted(["read-only", "workspace-write", "danger-full-access"]))
    research_update.add_argument("--approval-policy", choices=sorted(["untrusted", "on-failure", "on-request", "never"]))
    research_update.add_argument("--model")
    research_update.add_argument("--effort", dest="reasoning_effort")
    research_update.add_argument("--provider", action="append", default=[])
    research_update.add_argument("--limit", type=int, default=5)
    research_update.add_argument("--task-timeout", type=int, default=60)
    research_update.add_argument("--max-chars", type=int, default=12000)
    research_update.add_argument("--markdown-dir")
    research_update.set_defaults(handler=cmd_research_update)

    research_source_add = research_sub.add_parser("source-add", help="Add or update a research source")
    research_source_add.add_argument("--project", required=True)
    research_source_add.add_argument("--name", required=True)
    research_source_add.add_argument("--title", required=True)
    research_source_add.add_argument("--url", default="")
    research_source_add.add_argument("--source-type", default="")
    research_source_add.add_argument("--role", default="")
    research_source_add.add_argument("--depth", default="title")
    research_source_add.add_argument("--status", default="candidate", choices=sorted(["candidate", "kept", "rejected", "stale"]))
    research_source_add.add_argument("--relevance", type=int, default=0)
    research_source_add.add_argument("--evidence-quality", type=int, default=0)
    research_source_add.add_argument("--notes", default="")
    research_source_add.add_argument("--metadata-json", default="{}")
    research_source_add.set_defaults(handler=cmd_research_source_add)

    research_claim_add = research_sub.add_parser("claim-add", help="Add a research claim")
    research_claim_add.add_argument("--project", required=True)
    research_claim_add.add_argument("--name", required=True)
    research_claim_add.add_argument("--claim", required=True)
    research_claim_add.add_argument("--status", default="uncertain", choices=sorted(["supported", "contradicted", "uncertain"]))
    research_claim_add.add_argument("--support", default="")
    research_claim_add.add_argument("--confidence", type=int, default=0)
    research_claim_add.add_argument("--notes", default="")
    research_claim_add.add_argument("--metadata-json", default="{}")
    research_claim_add.set_defaults(handler=cmd_research_claim_add)

    research_idea_add = research_sub.add_parser("idea-add", help="Add a research idea")
    research_idea_add.add_argument("--project", required=True)
    research_idea_add.add_argument("--name", required=True)
    research_idea_add.add_argument("--title", required=True)
    research_idea_add.add_argument("--hypothesis", default="")
    research_idea_add.add_argument("--mechanism", default="")
    research_idea_add.add_argument("--status", default="candidate", choices=sorted(["candidate", "active", "selected", "rejected", "experiment_ready"]))
    research_idea_add.add_argument("--score-json", default="{}")
    research_idea_add.add_argument("--minimal-test", default="")
    research_idea_add.add_argument("--kill-criteria", default="")
    research_idea_add.add_argument("--notes", default="")
    research_idea_add.add_argument("--metadata-json", default="{}")
    research_idea_add.set_defaults(handler=cmd_research_idea_add)

    research_task_add = research_sub.add_parser("task-add", help="Queue a research task")
    research_task_add.add_argument("--project", required=True)
    research_task_add.add_argument("--name", required=True)
    research_task_add.add_argument("--task-type", required=True)
    research_task_add.add_argument("--title", default="")
    research_task_add.add_argument("--query", default="")
    research_task_add.add_argument("--source-id", type=int)
    research_task_add.add_argument("--idea-id", type=int)
    research_task_add.add_argument("--status", default="queued", choices=sorted(["queued", "running", "done", "blocked", "failed", "archived"]))
    research_task_add.add_argument("--payload-json", default="{}")
    research_task_add.add_argument("--result-json", default="{}")
    research_task_add.set_defaults(handler=cmd_research_task_add)

    return parser


def cmd_projects_add(store: Store, args) -> None:
    project = _save_project_from_args(store, args)
    print(f"Project {project['name']} -> {project['path']}")
    print(f"DB: {store.path}")


def cmd_projects_connect(store: Store, args) -> None:
    project = _save_project_from_args(store, args)
    print(f"Project {project['name']} -> {project['path']}")
    threads = sync_sessions(store, project["name"], limit=args.limit)
    print(f"Synced {len(threads)} session(s).")
    default_thread = store.get_project(project["name"])["default_thread_id"]
    print(f"Default thread: {default_thread or '-'}")


def _save_project_from_args(store: Store, args):
    ensure_valid_sandbox(args.sandbox)
    ensure_valid_approval_policy(args.approval_policy)
    return store.add_project(
        args.name,
        args.path,
        description=args.description,
        sandbox=args.sandbox,
        approval_policy=args.approval_policy,
        model=args.model,
        reasoning_effort=args.reasoning_effort,
        daily_summary_enabled=args.enable_daily_summary,
        daily_summary_time=args.daily_summary_time,
    )


def cmd_projects_list(store: Store, _args) -> None:
    rows = store.list_projects()
    if not rows:
        print("No projects. Add one with: mycodex projects add NAME PATH")
        return
    for row in rows:
        daily = f"daily@{row['daily_summary_time']}" if row["daily_summary_enabled"] else "daily:off"
        print(f"{row['name']}\t{row['path']}\t{daily}\tdefault={row['default_thread_id'] or '-'}")


def cmd_projects_discover(store: Store, args) -> None:
    projects = discover_codex_projects(
        limit=args.limit,
        include_archived=args.include_archived,
        search=args.search,
    )
    if not projects:
        print("No Codex projects found in local session history.")
        return
    for project in projects:
        connected = store.get_project_by_path(project["cwd"])
        marker = "*" if connected else " "
        name = connected["name"] if connected else "-"
        title = project["latest_title"] or project["latest_preview"] or ""
        title = title.replace("\n", " ")
        if len(title) > 90:
            title = title[:87] + "..."
        print(
            f"{marker} {project['cwd']}\t"
            f"sessions={project['session_count']}\t"
            f"updated={format_ts(project['latest_updated_at'])}\t"
            f"mycodex={name}\t"
            f"latest={project['latest_thread_id'] or '-'}\t"
            f"{title}"
        )


def cmd_projects_status(store: Store, args) -> None:
    project = _require_project(store, args.name)
    sessions = store.list_sessions(project["id"])
    runs = store.list_runs(project["id"], limit=5)
    experiments = store.list_experiments(project["id"])
    research_threads = store.list_research_threads(project["id"])
    git = git_snapshot(project["path"])
    print(f"Project: {project['name']}")
    print(f"Path: {project['path']}")
    print(f"Default thread: {project['default_thread_id'] or '-'}")
    print(f"Policy: sandbox={project['sandbox']} approval={project['approval_policy']}")
    if git["state"] == "git":
        print(f"Git: {git['branch']} {git['sha']} dirty={git['dirty_count']}")
        print(f"Last commit: {git['last_commit']}")
    else:
        print(f"Git: {git['summary']}")
    print(f"Sessions: {len(sessions)}")
    if sessions:
        latest = sessions[0]
        print(
            "Latest session: "
            f"{latest['thread_id']} status={latest['status_type']} "
            f"updated={format_ts(latest['updated_at'])}"
        )
    if runs:
        print("Recent runs:")
        for run in runs:
            print(
                f"  #{run['id']} {run['kind']} {run['status']} "
                f"started={format_ts(run['started_at'])} thread={run['thread_id'] or '-'}"
            )
    if experiments:
        print("Experiments:")
        for experiment in experiments[:5]:
            limit = experiment["max_iterations"] if experiment["max_iterations"] is not None else "-"
            print(
                f"  #{experiment['id']} {experiment['name']} {experiment['status']} "
                f"iter={experiment['current_iteration']}/{limit} "
                f"thread={experiment['thread_id']}"
            )
    if research_threads:
        print("Research:")
        for research in research_threads[:5]:
            limit = research["max_iterations"] if research["max_iterations"] is not None else "-"
            print(
                f"  #{research['id']} {research['name']} {research['status']} "
                f"phase={research['phase']} iter={research['current_iteration']}/{limit} "
                f"thread={research['thread_id']}"
            )


def cmd_projects_remove(store: Store, args) -> None:
    project = _require_project(store, args.name)
    if not args.yes:
        if not sys.stdin.isatty():
            raise ValueError("refusing to remove without --yes in non-interactive mode")
        answer = input(
            f"Remove project {project['name']} and its mycodex sessions/runs/rules? [y/N] "
        ).strip().lower()
        if answer not in {"y", "yes"}:
            print("Cancelled.")
            return
    removed = store.remove_project(args.name)
    if removed:
        print(f"Removed project {args.name}.")
    else:
        print(f"Project {args.name} was not found.")


def cmd_sessions_sync(store: Store, args) -> None:
    threads = sync_sessions(
        store,
        args.project,
        limit=args.limit,
        use_state_db_only=not args.scan_rollouts,
    )
    print(f"Synced {len(threads)} session(s) for {args.project}.")


def cmd_sessions_list(store: Store, args) -> None:
    project = _require_project(store, args.project)
    rows = store.list_sessions(project["id"])
    if not rows:
        print("No sessions stored. Run: mycodex sessions sync --project NAME")
        return
    for row in rows:
        title = (row["name"] or row["preview"] or "").replace("\n", " ")
        if len(title) > 100:
            title = title[:97] + "..."
        default = "*" if row["thread_id"] == project["default_thread_id"] else " "
        print(
            f"{default} {row['thread_id']}\t{row['status_type']}\t"
            f"{format_ts(row['updated_at'])}\t{title}"
        )


def cmd_sessions_set_default(store: Store, args) -> None:
    project = _require_project(store, args.project)
    sessions = store.list_sessions(project["id"])
    known = {row["thread_id"] for row in sessions}
    if args.thread_id not in known:
        raise ValueError(
            "thread is not stored for this project; run `mycodex sessions sync --project "
            f"{args.project}` first"
        )
    store.update_default_thread(project["id"], args.thread_id)
    print(f"Default thread for {args.project}: {args.thread_id}")


def cmd_send(store: Store, args) -> None:
    prompt = " ".join(args.prompt)
    result = send_prompt(
        store,
        args.project,
        prompt,
        new_thread=args.new_thread,
        timeout=args.timeout,
        sync_first=not args.no_sync,
        sandbox=args.sandbox,
        approval_policy=args.approval_policy,
        model=args.model,
        reasoning_effort=args.reasoning_effort,
    )
    print(f"Run #{result['run_id']} {result['status']} thread={result['thread_id']}")
    if result["output_text"]:
        print()
        print(result["output_text"])


def cmd_daily_summary(store: Store, args) -> None:
    result = run_daily_summary(
        store,
        args.project,
        timeout=args.timeout,
        sync_first=not args.no_sync,
    )
    print(f"Run #{result['run_id']} {result['status']} thread={result['thread_id']}")
    if result["output_text"]:
        print()
        print(result["output_text"])


def cmd_daemon(store: Store, args) -> None:
    label = args.launch_agent_label or default_daemon_launch_agent_label(store.path)
    if args.install_launch_agent:
        result = install_daemon_launch_agent(
            db_path=store.path,
            cwd=Path.cwd(),
            label=label,
            interval=args.interval,
            timeout=args.timeout,
            allow_full_train=args.allow_full_train,
            notify_telegram=not args.no_telegram_notify,
            telegram_token_file=args.telegram_token_file,
            telegram_token_env=args.telegram_token_env,
            telegram_proxy_url=args.telegram_proxy_url,
            log_dir=args.launch_agent_log_dir,
            bootstrap=not args.no_bootstrap,
        )
        print(f"Installed LaunchAgent {result['label']}: {result['path']}")
        for command in result["commands"]:
            print(
                "  "
                + " ".join(command["args"])
                + f" -> {command['returncode']}"
                + (f" stderr={command['stderr'].strip()}" if command.get("stderr") else "")
            )
        return
    if args.uninstall_launch_agent:
        result = uninstall_daemon_launch_agent(db_path=store.path, label=label)
        print(f"Uninstalled LaunchAgent {result['label']}: removed={result['removed']} path={result['path']}")
        for command in result["commands"]:
            print(
                "  "
                + " ".join(command["args"])
                + f" -> {command['returncode']}"
                + (f" stderr={command['stderr'].strip()}" if command.get("stderr") else "")
            )
        return
    if args.launch_agent_status:
        result = daemon_launch_agent_status(db_path=store.path, label=label)
        print(
            f"LaunchAgent {result['label']}: installed={result['installed']} "
            f"loaded={result['loaded']} path={result['path']}"
        )
        launchctl = result["launchctl"]
        if launchctl.get("stderr"):
            print(f"launchctl stderr: {launchctl['stderr'].strip()}")
        return
    print(f"mycodex daemon started; db={store.path}; interval={args.interval}s")
    run_daemon(
        store,
        interval_seconds=args.interval,
        once=args.once,
        timeout=args.timeout,
        notify_telegram=not args.no_telegram_notify,
        telegram_token_file=args.telegram_token_file,
        telegram_token_env=args.telegram_token_env,
        telegram_proxy_url=args.telegram_proxy_url,
    )


def cmd_web(store: Store, args) -> None:
    try:
        from .web import run_web
    except ImportError as exc:
        raise RuntimeError(
            "web dashboard dependencies are missing; install with `pip install -e .`"
        ) from exc
    print(f"mycodex web dashboard: http://{args.host}:{args.port}")
    run_web(db_path=store.path, host=args.host, port=args.port, reload=args.reload)


def cmd_telegram_run(store: Store, args) -> None:
    from .telegram import TelegramBot, token_from_sources

    for user_id in args.allow_user:
        store.allow_telegram_user(user_id)
        print(f"Allowed Telegram user {user_id}.")
    token = token_from_sources(
        token=args.token,
        token_file=args.token_file,
        token_env=args.token_env,
        db_path=store.path,
    )
    bot = TelegramBot(
        db_path=store.path,
        token=token,
        poll_timeout=args.poll_timeout,
        codex_timeout=args.codex_timeout,
        proxy_url=args.proxy_url,
    )
    bot.run()


def cmd_telegram_allow(store: Store, args) -> None:
    store.allow_telegram_user(args.user_id, args.username)
    print(f"Allowed Telegram user {args.user_id}.")


def cmd_telegram_deny(store: Store, args) -> None:
    store.deny_telegram_user(args.user_id)
    print(f"Denied Telegram user {args.user_id}.")


def cmd_telegram_users(store: Store, _args) -> None:
    users = store.list_telegram_users()
    if not users:
        print("No Telegram users.")
        return
    for user in users:
        state = "allowed" if user["allowed"] else "denied"
        username = f"@{user['username']}" if user["username"] else "-"
        print(f"{user['user_id']}\t{state}\t{username}")


def cmd_telegram_bindings(store: Store, _args) -> None:
    bindings = store.list_telegram_bindings()
    if not bindings:
        print("No Telegram bindings.")
        return
    for binding in bindings:
        topic = binding["message_thread_id"] or "-"
        thread = binding["thread_id"] or "-"
        print(
            f"chat={binding['chat_id']}\ttopic={topic}\t"
            f"project={binding['project_name']}\tthread={thread}"
        )


def cmd_telegram_bind(store: Store, args) -> None:
    project = _require_project(store, args.project)
    thread_id = args.thread_id or project["default_thread_id"]
    if thread_id is None:
        latest = store.latest_session(project["id"])
        thread_id = latest["thread_id"] if latest else None
    store.set_telegram_binding(
        chat_id=args.chat_id,
        message_thread_id=args.message_thread_id,
        project_id=project["id"],
        thread_id=thread_id,
    )
    print(
        f"Bound chat={args.chat_id} topic={args.message_thread_id or '-'} "
        f"to {project['name']} thread={thread_id or '-'}."
    )


def cmd_telegram_unbind(store: Store, args) -> None:
    removed = store.remove_telegram_binding(
        chat_id=args.chat_id,
        message_thread_id=args.message_thread_id,
    )
    if removed:
        print(f"Removed binding for chat={args.chat_id} topic={args.message_thread_id or '-'}.")
    else:
        print("No matching binding.")


def cmd_rules_list(store: Store, args) -> None:
    project_id = None
    if args.project:
        project = _require_project(store, args.project)
        project_id = project["id"]
    rows = store.list_rules(project_id)
    if not rows:
        print("No rules.")
        return
    for row in rows:
        schedule = row["time_of_day"] or f"every {row['interval_seconds']}s"
        enabled = "on" if row["enabled"] else "off"
        thread = row["thread_id"] or "-"
        print(f"{row['project_name']}\t{row['name']}\t{row['kind']}\t{enabled}\t{schedule}\tthread={thread}")


def cmd_rules_add(store: Store, args) -> None:
    if not args.time_of_day and not args.interval_seconds:
        raise ValueError("provide --time-of-day or --interval-seconds")
    project = _require_project(store, args.project)
    store.add_rule(
        project["id"],
        name=args.name,
        kind=args.kind,
        thread_id=args.thread_id,
        time_of_day=args.time_of_day,
        interval_seconds=args.interval_seconds,
        prompt=args.prompt,
        enabled=not args.disabled,
    )
    print(f"Rule {args.name} saved for {args.project}.")


def cmd_rules_watch(store: Store, args) -> None:
    print(
        f"mycodex rule watch started; db={store.path}; "
        f"project={args.project}; rule={args.name}; interval={args.interval}s",
        flush=True,
    )
    run_single_rule_watch(
        store,
        args.project,
        args.name,
        interval_seconds=args.interval,
        once=args.once,
        timeout=args.timeout,
        notify_telegram=not args.no_telegram_notify,
        telegram_token_file=args.telegram_token_file,
        telegram_token_env=args.telegram_token_env,
        telegram_proxy_url=args.telegram_proxy_url,
    )


def cmd_runs_list(store: Store, args) -> None:
    project = _require_project(store, args.project)
    rows = store.list_runs(project["id"], limit=args.limit)
    if not rows:
        print("No runs.")
        return
    for row in rows:
        print(
            f"#{row['id']}\t{row['kind']}\t{row['status']}\t"
            f"{format_ts(row['started_at'])}\tthread={row['thread_id'] or '-'}"
        )
        if row["error"]:
            print(f"  error: {row['error']}")
        elif row["output_text"]:
            first = row["output_text"].strip().splitlines()[0]
            print(f"  {first[:160]}")


def cmd_experiments_create(store: Store, args) -> None:
    project = _require_project(store, args.project)
    known_threads = {row["thread_id"] for row in store.list_sessions(project["id"])}
    if known_threads and args.thread_id not in known_threads:
        print(
            f"warning: thread {args.thread_id} is not currently stored for {args.project}; "
            "run `mycodex sessions sync` if this is unexpected.",
            file=sys.stderr,
        )
    experiment = store.add_experiment(
        project["id"],
        name=args.name,
        thread_id=args.thread_id,
        objective=args.objective,
        max_iterations=args.max_iterations,
        remote_ref=args.remote_ref,
        config=_parse_json_object(args.config_json),
        status=args.status,
    )
    store.add_experiment_event(
        experiment["id"],
        event_type="created",
        level="info",
        message="Experiment record created or updated.",
        source="mycodex",
        status=args.status,
    )
    print(
        f"Experiment #{experiment['id']} {experiment['name']} "
        f"{experiment['status']} thread={experiment['thread_id']}"
    )


def cmd_experiments_list(store: Store, args) -> None:
    project = _require_project(store, args.project)
    rows = store.list_experiments(project["id"], include_archived=args.include_archived)
    if not rows:
        print("No experiments.")
        return
    for row in rows:
        limit = row["max_iterations"] if row["max_iterations"] is not None else "-"
        print(
            f"#{row['id']}\t{row['name']}\t{row['status']}\t"
            f"phase={row['phase']}\t"
            f"iter={row['current_iteration']}/{limit}\t"
            f"{format_ts(row['updated_at'])}\tthread={row['thread_id']}"
        )


def cmd_experiments_status(store: Store, args) -> None:
    project = _require_project(store, args.project)
    experiment = _require_experiment(store, project["id"], args.name)
    events = store.list_experiment_events(experiment["id"], limit=args.limit)
    _print_experiment_status(experiment, events)


def cmd_experiments_watch(store: Store, args) -> None:
    project = _require_project(store, args.project)
    try:
        while True:
            experiment = _require_experiment(store, project["id"], args.name)
            event_limit = args.limit if args.include_deltas else max(args.limit * 5, 50)
            events = store.list_experiment_events(experiment["id"], limit=event_limit)
            if not args.include_deltas:
                events = [
                    event
                    for event in events
                    if event["event_type"] != "agent_message_delta"
                ][: args.limit]
            if not args.once:
                print("\033[2J\033[H", end="")
            print(f"Updated: {format_ts(int(time.time()))}")
            _print_experiment_status(experiment, events)
            if args.once:
                break
            sys.stdout.flush()
            time.sleep(max(1.0, args.interval))
    except KeyboardInterrupt:
        print("\nStopped.")


def _print_experiment_status(experiment, events) -> None:
    print(f"Experiment: #{experiment['id']} {experiment['name']}")
    print(f"Project: {experiment['project_name']}")
    print(f"Status: {experiment['status']}")
    print(f"Phase: {experiment['phase']}")
    if experiment["phase_message"]:
        print(f"Phase message: {experiment['phase_message']}")
    if experiment["latest_decision"]:
        print(f"Latest decision: {experiment['latest_decision']}")
    if experiment["artifact_ref"]:
        print(f"Artifact: {experiment['artifact_ref']}")
    print(f"Thread: {experiment['thread_id']}")
    print(f"Iteration: {experiment['current_iteration']}/{experiment['max_iterations'] or '-'}")
    print(f"Remote: {experiment['remote_ref'] or '-'}")
    if experiment["objective"]:
        print(f"Objective: {experiment['objective']}")
    if not events:
        print("No events.")
        return
    print("Recent events:")
    for event in events:
        iteration = "-" if event["iteration"] is None else event["iteration"]
        message = event["message"] or "-"
        print(
            f"  #{event['id']} iter={iteration} {event['level']} "
            f"{event['event_type']} {format_ts(event['created_at'])} {message}"
        )


def cmd_experiments_event(store: Store, args) -> None:
    project = _require_project(store, args.project)
    experiment = _require_experiment(store, project["id"], args.name)
    event_id = store.add_experiment_event(
        experiment["id"],
        event_type=args.event_type,
        iteration=args.iteration,
        level=args.level,
        message=args.message,
        payload=_parse_json_object(args.payload_json),
        source=args.source,
        run_id=args.run_id,
        status=args.status,
        phase=args.phase,
        phase_message=args.phase_message,
    )
    print(f"Experiment event #{event_id} recorded for {experiment['name']}.")


def cmd_experiments_set_status(store: Store, args) -> None:
    project = _require_project(store, args.project)
    experiment = _require_experiment(store, project["id"], args.name)
    store.update_experiment_status(experiment["id"], args.status)
    store.add_experiment_event(
        experiment["id"],
        event_type="status_changed",
        level="info",
        message=f"Status -> {args.status}.",
        source="mycodex",
        status=args.status,
    )
    print(f"Experiment {experiment['name']} -> {args.status}.")


def cmd_experiments_start(store: Store, args) -> None:
    result = start_experiment_control_turn(
        store,
        args.project,
        args.name,
        timeout=args.timeout,
        sandbox=args.sandbox,
        approval_policy=args.approval_policy,
        model=args.model,
        reasoning_effort=args.reasoning_effort,
    )
    print(f"Run #{result['run_id']} {result['status']} thread={result['thread_id']}")
    contract = extract_execution_contract(result.get("output_text") or "")
    if contract is not None:
        task_record = record_execution_contract_task(
            store,
            args.project,
            args.name,
            contract,
            trigger="manual_start",
            auto_execute_policy=True,
            run_id=result.get("run_id") if isinstance(result.get("run_id"), int) else None,
            context_key=str(result.get("turn_id") or result.get("run_id") or ""),
            source="mycodex-cli",
        )
        print(
            f"Protocol task #{task_record['task']['id']} "
            f"{task_record['task']['status']} "
            f"class={task_record['task']['execution_class'] or '-'}"
        )
    if result["output_text"]:
        print()
        print(result["output_text"])


def cmd_experiments_task_list(store: Store, args) -> None:
    project = _require_project(store, args.project)
    experiment = _require_experiment(store, project["id"], args.name)
    tasks = store.list_experiment_tasks(
        experiment["id"],
        statuses=args.status or None,
        limit=args.limit,
    )
    if not tasks:
        print("No experiment protocol tasks.")
        return
    for task in tasks:
        print(
            f"#{task['id']}\t{task['status']}\t{task['task_type']}\t"
            f"class={task['execution_class'] or '-'}\tattempts={task['attempts']}/{task['max_attempts']}\t"
            f"lease={task['lease_owner'] or '-'}\tupdated={format_ts(task['updated_at'])}"
        )


def cmd_experiments_task_approve(store: Store, args) -> None:
    project = _require_project(store, args.project)
    experiment = _require_experiment(store, project["id"], args.name)
    task = store.get_experiment_task(experiment["id"], args.task_id)
    if task is None:
        raise ValueError(f"unknown experiment task #{args.task_id}")
    if task["status"] not in {"proposed", "blocked"}:
        raise ValueError(f"task #{args.task_id} is {task['status']}, not proposed/blocked")
    updated = store.update_experiment_task_status(experiment["id"], args.task_id, "ready")
    store.add_experiment_event(
        experiment["id"],
        event_type="experiment_task_approved",
        level="info",
        message=f"Experiment task #{args.task_id} approved for execution.",
        payload={"task_id": args.task_id, "previous_status": task["status"]},
        source="manual",
        status="running",
        phase="checking_git",
        phase_message="A protocol task was approved and is ready for worker execution.",
    )
    print(f"Experiment task #{updated['id']} -> {updated['status']}.")


def cmd_experiments_task_run(store: Store, args) -> None:
    result = run_ready_experiment_tasks(
        store,
        args.project,
        args.name,
        owner=args.owner,
        lease_seconds=args.lease_seconds,
        max_tasks=args.max_tasks,
        ssh_key=args.ssh_key,
        ssh_password_file=args.ssh_password_file,
        remote_ref=args.remote_ref,
        remote_python=args.remote_python,
        allow_full_train=args.allow_full_train,
    )
    print(f"Executed protocol tasks: {result['executed_tasks']}")
    for item in result["tasks"]:
        task = item["task"]
        execution = item.get("execution") or {}
        print(
            f"  #{task['id']} {task['status']} reason={execution.get('reason') or '-'} "
            f"launch={bool(execution.get('launch'))}"
        )


def cmd_experiments_task_recover(store: Store, args) -> None:
    result = recover_stale_experiment_tasks(
        store,
        project_name=args.project,
        experiment_name=args.name,
        running_timeout_seconds=args.running_timeout_seconds,
        source="mycodex-cli",
    )
    print(f"Recovered protocol tasks: {result['recovered_tasks']}")
    for item in result["tasks"]:
        print(
            f"  {item['project']}/{item['experiment']} "
            f"#{item['task_id']} {item['previous_status']} -> {item['status']} "
            f"reason={item['reason']}"
        )


def cmd_experiments_git_push(store: Store, args) -> None:
    result = git_commit_push(
        store,
        args.project,
        args.name,
        commit_message=args.commit_message,
        all_changes=args.all_changes,
        paths=args.path,
        remote=args.remote,
        branch=args.branch,
        dry_run=args.dry_run,
    )
    action = "Would push" if result["dry_run"] else "Pushed"
    print(f"{action} {result['commit']} to {result['remote']}/{result['branch']}.")


def cmd_experiments_launch_remote(store: Store, args) -> None:
    result = launch_remote_job(
        store,
        args.project,
        args.name,
        command=args.command,
        remote_ref=args.remote_ref,
        ssh_key=args.ssh_key,
        ssh_password_file=args.ssh_password_file,
        commit=args.commit,
        branch=args.branch,
        require_clean=not args.allow_dirty,
        fetch=not args.no_fetch,
        log_path=args.log_path,
        pid_path=args.pid_path,
        env=_parse_json_object(args.env_json),
        wandb_entity=args.wandb_entity,
        wandb_project=args.wandb_project,
        wandb_run_id=args.wandb_run_id,
        wandb_run_name=args.wandb_run_name,
        timeout=args.timeout,
        dry_run=args.dry_run,
        post_launch_check_delay=-1 if args.no_post_launch_check else args.post_launch_check_delay,
        post_launch_check_lines=args.post_launch_check_lines,
    )
    if result.get("dry_run"):
        print("Remote launch dry run.")
        print(result["script"])
        return
    print(
        f"Remote job started pid={result['remote_pid']} "
        f"commit={result['remote_commit'] or '-'} log={result['log_path']}"
    )


def cmd_experiments_remote_status(store: Store, args) -> None:
    result = remote_job_status(
        store,
        args.project,
        args.name,
        remote_ref=args.remote_ref,
        ssh_key=args.ssh_key,
        ssh_password_file=args.ssh_password_file,
        pid=args.pid,
        log_path=args.log_path,
        lines=args.lines,
        timeout=args.timeout,
    )
    print(f"Remote job status: {result['status']}")
    if result["stdout"]:
        print(result["stdout"])


def cmd_experiments_sync_remote(store: Store, args) -> None:
    result = sync_remote_experiment_state(
        store,
        args.project,
        args.name,
        remote_ref=args.remote_ref,
        ssh_key=args.ssh_key,
        ssh_password_file=args.ssh_password_file,
        remote_state_root=args.remote_state_root,
        remote_state_path=args.remote_state_path,
        remote_events_path=args.remote_events_path,
        remote_base=args.remote_base,
        remote_python=args.remote_python,
        event_tail=args.event_tail,
        log_tail_lines=args.log_tail_lines,
        stale_seconds=args.stale_seconds,
        timeout=args.timeout,
    )
    print(
        f"Remote sync status={result.get('sync_status')} "
        f"events={result.get('remote_events_imported', 0)} "
        f"anomalies={len(result.get('control_anomalies') or [])}"
    )
    if result.get("batch_fingerprint"):
        print(f"Anomaly fingerprint: {result['batch_fingerprint']}")


def cmd_experiments_poll_wandb(store: Store, args) -> None:
    result = poll_wandb_run(
        store,
        args.project,
        args.name,
        run_path=args.run_path,
        wandb_entity=args.wandb_entity,
        wandb_project=args.wandb_project,
        wandb_run_id=args.wandb_run_id,
        metrics=args.metric,
        history_limit=args.history_limit,
        stop_conditions=args.stop_condition,
        fail_on_nonfinite=args.fail_on_nonfinite,
        complete_on_finish=args.complete_on_finish,
        interval=args.interval,
        max_polls=args.max_polls,
        trigger_next_control=args.trigger_next_control,
        control_timeout=args.control_timeout,
        control_sandbox=args.sandbox,
        control_approval_policy=args.approval_policy,
    )
    print(
        f"W&B {result['run_path']} state={result['state']} "
        f"outcome={result['outcome']}"
    )


def cmd_experiments_supervise(store: Store, args) -> None:
    result = supervise_experiment(
        store,
        args.project,
        args.name,
        run_path=args.run_path,
        metrics=args.metric,
        stop_conditions=args.stop_condition,
        fail_on_nonfinite=not args.no_fail_on_nonfinite,
        complete_on_finish=args.complete_on_finish,
        remote_ref=args.remote_ref,
        ssh_key=args.ssh_key,
        ssh_password_file=args.ssh_password_file,
        remote_python=args.remote_python,
        remote_pid=args.pid,
        remote_log_path=args.log_path,
        interval=args.interval,
        max_cycles=args.max_cycles,
        trigger_next_control=not args.no_trigger_next_control,
        auto_execute=not args.no_auto_execute,
        allow_full_train=args.allow_full_train,
        control_timeout=args.control_timeout,
        control_sandbox=args.sandbox,
        control_approval_policy=args.approval_policy,
        control_model=args.model,
        control_reasoning_effort=args.reasoning_effort,
        peer_review_bypass_timeout_seconds=args.peer_review_bypass_timeout_seconds,
        peer_review_bypass_on=args.peer_review_bypass_on,
        peer_review_bypass_allowed_classes=args.peer_review_bypass_classes,
    )
    print(
        f"Supervisor cycle={result.get('cycle')} "
        f"terminal={result.get('terminal_reason') or '-'} "
        f"run={result.get('run_path') or '-'}"
    )
    if result.get("matched_stop_conditions"):
        print("Matched stop conditions: " + ", ".join(result["matched_stop_conditions"]))


def cmd_experiments_watchdog(store: Store, args) -> None:
    result = run_experiment_watchdog(
        store,
        args.project,
        experiment_name=args.name,
        run_path=args.run_path,
        metrics=args.metric,
        stop_conditions=args.stop_condition,
        fail_on_nonfinite=not args.no_fail_on_nonfinite,
        complete_on_finish=args.complete_on_finish,
        remote_ref=args.remote_ref,
        ssh_key=args.ssh_key,
        ssh_password_file=args.ssh_password_file,
        remote_state_root=args.remote_state_root,
        remote_state_path=args.remote_state_path,
        remote_events_path=args.remote_events_path,
        remote_base=args.remote_base,
        remote_python=args.remote_python,
        remote_sync=not args.no_remote_sync,
        remote_event_tail=args.remote_event_tail,
        remote_log_tail_lines=args.remote_log_tail_lines,
        remote_sync_timeout=args.remote_sync_timeout,
        remote_anomaly_control=not args.no_remote_anomaly_control,
        interval=args.interval,
        max_cycles=args.max_cycles,
        stale_seconds=args.stale_seconds,
        supervisor_interval=args.supervisor_interval,
        trigger_next_control=not args.no_trigger_next_control,
        auto_execute=not args.no_auto_execute,
        allow_full_train=args.allow_full_train,
        monitor_only=args.monitor_only,
        control_timeout=args.control_timeout,
        control_sandbox=args.sandbox,
        control_approval_policy=args.approval_policy,
        control_model=args.model,
        control_reasoning_effort=args.reasoning_effort,
        peer_review_bypass_timeout_seconds=args.peer_review_bypass_timeout_seconds,
        peer_review_bypass_on=args.peer_review_bypass_on,
        peer_review_bypass_allowed_classes=args.peer_review_bypass_classes,
        force=args.force,
    )
    print(f"Watchdog cycle={result.get('cycle')} experiments={len(result.get('experiments') or [])}")
    for item in result.get("experiments") or []:
        print(
            f"  {item.get('experiment')} action={item.get('action')} "
            f"reason={item.get('reason') or '-'} activity={item.get('activity_event') or '-'} "
            f"age={item.get('activity_age_seconds')}"
        )


def cmd_research_create(store: Store, args) -> None:
    project = _require_project(store, args.project)
    known_threads = {row["thread_id"] for row in store.list_sessions(project["id"])}
    if known_threads and args.thread_id not in known_threads:
        print(
            f"warning: thread {args.thread_id} is not currently stored for {args.project}; "
            "run `mycodex sessions sync` if this is unexpected.",
            file=sys.stderr,
        )
    config = _parse_json_object(args.config_json)
    research = store.add_research_thread(
        project["id"],
        name=args.name,
        thread_id=args.thread_id,
        question=args.question,
        max_iterations=args.max_iterations,
        config=config,
        status=args.status,
    )
    store.add_research_event(
        research["id"],
        event_type="created",
        level="info",
        message="Research thread record created or updated.",
        source="mycodex",
        status=args.status,
    )
    preflight_task_id = None
    if config.get("concept_preflight") is not False:
        preflight_task_id = ensure_research_concept_preflight_task(
            store,
            args.project,
            args.name,
            auto_queue_search=bool(config.get("auto_queue_initial_search", False)),
        )
        if preflight_task_id:
            store.add_research_event(
                research["id"],
                event_type="task_queued",
                message=f"Task #{preflight_task_id}: concept_preflight",
                payload={"task_id": preflight_task_id, "auto_queue_initial_search": bool(config.get("auto_queue_initial_search", False))},
                source="mycodex",
                phase="concept_preflight",
            )
    print(
        f"Research #{research['id']} {research['name']} "
        f"{research['status']} thread={research['thread_id']}"
    )
    if preflight_task_id:
        print(f"Queued concept preflight task #{preflight_task_id}.")
    path = write_research_markdown(store, args.project, args.name)
    print(f"Markdown: {path}")


def cmd_research_list(store: Store, args) -> None:
    project = _require_project(store, args.project)
    rows = store.list_research_threads(project["id"], include_archived=args.include_archived)
    if not rows:
        print("No research threads.")
        return
    for row in rows:
        limit = row["max_iterations"] if row["max_iterations"] is not None else "-"
        print(
            f"#{row['id']}\t{row['name']}\t{row['status']}\t"
            f"phase={row['phase']}\t"
            f"iter={row['current_iteration']}/{limit}\t"
            f"{format_ts(row['updated_at'])}\tthread={row['thread_id']}"
        )


def cmd_research_find(store: Store, args) -> None:
    matches = find_research_threads(
        store,
        args.project,
        args.query,
        limit=args.limit,
        include_archived=args.include_archived,
    )
    if not matches:
        print("No matching research threads.")
        return
    for item in matches:
        row = item["research"]
        fields = ",".join(item["matched_fields"]) or "-"
        print(
            f"#{row['id']}\t{row['name']}\tscore={item['score']}\t"
            f"fields={fields}\tphase={row['phase']}\tupdated={format_ts(row['updated_at'])}"
        )
        for snippet in item["snippets"][:3]:
            print(f"  - {snippet}")


def cmd_research_status(store: Store, args) -> None:
    project = _require_project(store, args.project)
    research = _require_research(store, project["id"], args.name)
    print(f"Research: #{research['id']} {research['name']}")
    print(f"Project: {research['project_name']}")
    print(f"Status: {research['status']}")
    print(f"Phase: {research['phase']}")
    if research["phase_message"]:
        print(f"Phase message: {research['phase_message']}")
    if research["latest_decision"]:
        print(f"Latest decision: {research['latest_decision']}")
    print(f"Thread: {research['thread_id']}")
    print(f"Iteration: {research['current_iteration']}/{research['max_iterations'] or '-'}")
    if research["question"]:
        print(f"Question: {research['question']}")

    sources = store.list_research_sources(research["id"], limit=args.limit)
    claims = store.list_research_claims(research["id"], limit=args.limit)
    ideas = store.list_research_ideas(research["id"], limit=args.limit)
    tasks = store.list_research_tasks(research["id"], limit=args.limit)
    events = store.list_research_events(research["id"], limit=args.limit)

    _print_research_rows("Sources", sources, lambda row: f"#{row['id']} {row['status']} {row['role']} {row['title']} {row['url']}")
    _print_research_rows("Claims", claims, lambda row: f"#{row['id']} {row['status']} c={row['confidence']} {row['claim']}")
    _print_research_rows("Ideas", ideas, lambda row: f"#{row['id']} {row['status']} {row['title']} :: {row['minimal_test']}")
    _print_research_rows("Tasks", tasks, lambda row: f"#{row['id']} {row['status']} {row['task_type']} {row['title'] or row['query']}")
    _print_research_rows(
        "Recent events",
        events,
        lambda row: (
            f"#{row['id']} iter={'-' if row['iteration'] is None else row['iteration']} "
            f"{row['level']} {row['event_type']} {format_ts(row['created_at'])} {row['message'] or '-'}"
        ),
    )


def cmd_research_event(store: Store, args) -> None:
    project = _require_project(store, args.project)
    research = _require_research(store, project["id"], args.name)
    event_id = store.add_research_event(
        research["id"],
        event_type=args.event_type,
        iteration=args.iteration,
        level=args.level,
        message=args.message,
        payload=_parse_json_object(args.payload_json),
        source=args.source,
        run_id=args.run_id,
        status=args.status,
        phase=args.phase,
        phase_message=args.phase_message,
    )
    path = write_research_markdown(store, args.project, args.name)
    print(f"Research event #{event_id} recorded for {research['name']}.")
    print(f"Markdown: {path}")


def cmd_research_start(store: Store, args) -> None:
    result = start_research_control_turn(
        store,
        args.project,
        args.name,
        timeout=args.timeout,
        sandbox=args.sandbox,
        approval_policy=args.approval_policy,
        model=args.model,
        reasoning_effort=args.reasoning_effort,
    )
    print(f"Run #{result['run_id']} {result['status']} thread={result['thread_id']}")
    applied = result.get("applied")
    if applied:
        print(
            f"Applied contract: sources={applied['sources']} claims={applied['claims']} "
            f"ideas={applied['ideas']} tasks={applied['tasks']} auto={applied['auto_continue']}"
        )
    elif result.get("contract") is None:
        print("No parseable research contract found.")
    if result["output_text"]:
        print()
        print(result["output_text"])
    path = write_research_markdown(store, args.project, args.name)
    print(f"Markdown: {path}")


def cmd_research_run_task(store: Store, args) -> None:
    result = run_research_task(
        store,
        args.project,
        args.name,
        task_id=args.task_id,
        providers=args.provider or None,
        limit=args.limit,
        timeout=args.timeout,
        max_chars=args.max_chars,
        allow_control=not args.no_control,
    )
    print(
        f"Research task {result.get('task_id') or '-'} {result.get('status')} "
        f"type={result.get('task_type') or '-'}"
    )
    if result.get("sources") is not None:
        print(f"Sources recorded: {result['sources']}")
    if result.get("chars") is not None:
        print(f"Characters ingested: {result['chars']}")
    if result.get("control"):
        control = result["control"]
        print(f"Control run: #{control.get('run_id')} {control.get('status')}")
    path = write_research_markdown(store, args.project, args.name)
    print(f"Markdown: {path}")


def cmd_research_supervise(store: Store, args) -> None:
    result = supervise_research(
        store,
        args.project,
        args.name,
        max_cycles=args.max_cycles,
        interval=args.interval,
        trigger_control=not args.no_trigger_control,
        control_timeout=args.control_timeout,
        sandbox=args.sandbox,
        approval_policy=args.approval_policy,
        model=args.model,
        reasoning_effort=args.reasoning_effort,
        providers=args.provider or None,
        limit=args.limit,
        task_timeout=args.task_timeout,
        max_chars=args.max_chars,
    )
    print(
        f"Research supervisor cycles={len(result['cycles'])} "
        f"terminal={result.get('terminal_reason') or '-'}"
    )
    for cycle in result["cycles"]:
        action = cycle.get("action")
        status = (cycle.get("result") or {}).get("status") or "-"
        print(f"  cycle={cycle.get('cycle')} action={action} status={status}")
    path = write_research_markdown(store, args.project, args.name)
    print(f"Markdown: {path}")


def cmd_research_update(store: Store, args) -> None:
    result = update_research_from_query(
        store,
        args.project,
        args.query,
        research_name=args.name,
        match_limit=args.match_limit,
        max_cycles=args.max_cycles,
        interval=args.interval,
        trigger_control=not args.no_trigger_control,
        control_timeout=args.control_timeout,
        sandbox=args.sandbox,
        approval_policy=args.approval_policy,
        model=args.model,
        reasoning_effort=args.reasoning_effort,
        providers=args.provider or None,
        limit=args.limit,
        task_timeout=args.task_timeout,
        max_chars=args.max_chars,
        markdown_dir=args.markdown_dir,
    )
    print(
        f"Research update queued for {result['project']}/{result['name']} "
        f"task=#{result['task_id']}"
    )
    if result.get("preflight_task_id"):
        print(f"Queued concept preflight task #{result['preflight_task_id']}.")
    matches = result.get("matches") or []
    if matches:
        match = matches[0]
        print(f"Matched score={match['score']} fields={','.join(match['matched_fields']) or '-'}")
    supervisor = result.get("supervisor")
    if supervisor:
        print(
            f"Supervisor cycles={len(supervisor['cycles'])} "
            f"terminal={supervisor.get('terminal_reason') or '-'}"
        )
    print(f"Markdown: {result['markdown_path']}")


def cmd_research_source_add(store: Store, args) -> None:
    project = _require_project(store, args.project)
    research = _require_research(store, project["id"], args.name)
    source_id = store.upsert_research_source(
        research["id"],
        title=args.title,
        url=args.url,
        source_type=args.source_type,
        role=args.role,
        depth=args.depth,
        status=args.status,
        relevance=args.relevance,
        evidence_quality=args.evidence_quality,
        notes=args.notes,
        metadata=_parse_json_object(args.metadata_json),
    )
    store.add_research_event(
        research["id"],
        event_type="source_added",
        message=f"Source #{source_id}: {args.title}",
        payload={"source_id": source_id},
        source="mycodex",
    )
    path = write_research_markdown(store, args.project, args.name)
    print(f"Research source #{source_id} saved.")
    print(f"Markdown: {path}")


def cmd_research_claim_add(store: Store, args) -> None:
    project = _require_project(store, args.project)
    research = _require_research(store, project["id"], args.name)
    claim_id = store.add_research_claim(
        research["id"],
        claim=args.claim,
        status=args.status,
        support=args.support,
        confidence=args.confidence,
        notes=args.notes,
        metadata=_parse_json_object(args.metadata_json),
    )
    store.add_research_event(
        research["id"],
        event_type="claim_added",
        message=f"Claim #{claim_id}: {args.claim}",
        payload={"claim_id": claim_id},
        source="mycodex",
    )
    path = write_research_markdown(store, args.project, args.name)
    print(f"Research claim #{claim_id} saved.")
    print(f"Markdown: {path}")


def cmd_research_idea_add(store: Store, args) -> None:
    project = _require_project(store, args.project)
    research = _require_research(store, project["id"], args.name)
    idea_id = store.add_research_idea(
        research["id"],
        title=args.title,
        hypothesis=args.hypothesis,
        mechanism=args.mechanism,
        status=args.status,
        score=_parse_json_object(args.score_json),
        minimal_test=args.minimal_test,
        kill_criteria=args.kill_criteria,
        notes=args.notes,
        metadata=_parse_json_object(args.metadata_json),
    )
    store.add_research_event(
        research["id"],
        event_type="idea_added",
        message=f"Idea #{idea_id}: {args.title}",
        payload={"idea_id": idea_id},
        source="mycodex",
    )
    path = write_research_markdown(store, args.project, args.name)
    print(f"Research idea #{idea_id} saved.")
    print(f"Markdown: {path}")


def cmd_research_task_add(store: Store, args) -> None:
    project = _require_project(store, args.project)
    research = _require_research(store, project["id"], args.name)
    task_id = store.add_research_task(
        research["id"],
        task_type=args.task_type,
        title=args.title,
        query=args.query,
        status=args.status,
        source_id=args.source_id,
        idea_id=args.idea_id,
        payload=_parse_json_object(args.payload_json),
        result=_parse_json_object(args.result_json),
    )
    store.add_research_event(
        research["id"],
        event_type="task_queued",
        message=f"Task #{task_id}: {args.task_type}",
        payload={"task_id": task_id},
        source="mycodex",
    )
    path = write_research_markdown(store, args.project, args.name)
    print(f"Research task #{task_id} saved.")
    print(f"Markdown: {path}")


def _require_project(store: Store, name: str):
    project = store.get_project(name)
    if project is None:
        raise ValueError(f"unknown project {name!r}; run `mycodex projects add {name} PATH`")
    return project


def _require_experiment(store: Store, project_id: int, name: str):
    experiment = store.get_experiment(project_id, name)
    if experiment is None:
        raise ValueError(f"unknown experiment {name!r}; run `mycodex experiments create` first")
    return experiment


def _require_research(store: Store, project_id: int, name: str):
    research = store.get_research_thread(project_id, name)
    if research is None:
        raise ValueError(f"unknown research {name!r}; run `mycodex research create` first")
    return research


def _print_research_rows(title: str, rows, formatter) -> None:
    if not rows:
        print(f"{title}: none")
        return
    print(f"{title}:")
    for row in rows:
        print("  " + formatter(row))


def _parse_json_object(value: str) -> dict:
    try:
        parsed = json.loads(value or "{}")
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON object: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("JSON value must be an object")
    return parsed
