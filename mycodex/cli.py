from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .daemon import run_daemon
from .db import Store
from .codex_state import discover_codex_projects
from .service import (
    ensure_valid_approval_policy,
    ensure_valid_sandbox,
    format_ts,
    git_snapshot,
    run_daily_summary,
    send_prompt,
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
    r_add.add_argument("--time-of-day")
    r_add.add_argument("--interval-seconds", type=int)
    r_add.add_argument("--prompt", required=True)
    r_add.add_argument("--disabled", action="store_true")
    r_add.set_defaults(handler=cmd_rules_add)

    runs = sub.add_parser("runs", help="Inspect run history")
    runs_sub = runs.add_subparsers(dest="runs_command", required=True)
    runs_list = runs_sub.add_parser("list", help="List project runs")
    runs_list.add_argument("--project", required=True)
    runs_list.add_argument("--limit", type=int, default=10)
    runs_list.set_defaults(handler=cmd_runs_list)

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
    print(f"mycodex daemon started; db={store.path}; interval={args.interval}s")
    run_daemon(store, interval_seconds=args.interval, once=args.once, timeout=args.timeout)


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
    from .telegram import TelegramBot, token_from_env

    for user_id in args.allow_user:
        store.allow_telegram_user(user_id)
        print(f"Allowed Telegram user {user_id}.")
    token = args.token or token_from_env(args.token_env)
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
        print(f"{row['project_name']}\t{row['name']}\t{row['kind']}\t{enabled}\t{schedule}")


def cmd_rules_add(store: Store, args) -> None:
    if not args.time_of_day and not args.interval_seconds:
        raise ValueError("provide --time-of-day or --interval-seconds")
    project = _require_project(store, args.project)
    store.add_rule(
        project["id"],
        name=args.name,
        kind=args.kind,
        time_of_day=args.time_of_day,
        interval_seconds=args.interval_seconds,
        prompt=args.prompt,
        enabled=not args.disabled,
    )
    print(f"Rule {args.name} saved for {args.project}.")


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


def _require_project(store: Store, name: str):
    project = store.get_project(name)
    if project is None:
        raise ValueError(f"unknown project {name!r}; run `mycodex projects add {name} PATH`")
    return project
