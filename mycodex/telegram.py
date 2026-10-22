from __future__ import annotations

import json
import os
import shlex
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .db import Store
from .prompts import daily_summary_prompt
from .service import format_ts, send_prompt, sync_sessions
from .status import project_snapshot, snapshot_to_jsonable


TELEGRAM_MAX_MESSAGE = 4096
DEFAULT_TOKEN_FILE = Path("data/secrets/telegram_bot_token")
HELP_TEXT = """mycodex Telegram commands:
/projects - list mycodex projects
/bind PROJECT [THREAD_ID] - bind this chat/topic to a project and optional Codex thread
/bind_thread PROJECT THREAD_ID - bind explicitly to a Codex thread
/binding - show current binding
/unbind - remove current binding
/status - show bound project status
/sessions - list stored Codex sessions for bound project
/sync - sync Codex sessions for bound project
/runs - list recent runs for bound project
/summary - run a read-only daily summary on the bound thread
/send PROMPT - send a prompt to the bound Codex thread
/new PROMPT - start a new Codex thread and rebind this chat/topic to it
/todo list|add|open|doing|blocked|done|archived ... - manage project todos

Plain text messages are treated as /send when this chat/topic is bound.
"""


@dataclass(frozen=True)
class TelegramContext:
    chat_id: str
    message_thread_id: str | None
    user_id: str
    username: str
    chat_type: str
    text: str
    update_id: int | None = None


class TelegramApiError(RuntimeError):
    pass


class TelegramClient:
    def __init__(
        self,
        token: str,
        *,
        api_base: str = "https://api.telegram.org",
        proxy_url: str | None = None,
    ) -> None:
        self.token = token
        self.api_base = api_base.rstrip("/")
        self.proxy_url = proxy_url
        if proxy_url:
            self.opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
            )
        else:
            self.opener = urllib.request.build_opener()

    def get_updates(
        self,
        *,
        offset: int | None,
        timeout: int,
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "timeout": timeout,
            "allowed_updates": ["message"],
        }
        if offset is not None:
            payload["offset"] = offset
        result = self._request("getUpdates", payload, timeout=timeout + 10)
        return list(result)

    def send_message(
        self,
        chat_id: str,
        text: str,
        *,
        message_thread_id: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if message_thread_id:
            payload["message_thread_id"] = int(message_thread_id)
        self._request("sendMessage", payload, timeout=30)

    def send_chunks(
        self,
        chat_id: str,
        text: str,
        *,
        message_thread_id: str | None = None,
    ) -> None:
        text = text or "(empty)"
        while text:
            chunk = text[:3900]
            text = text[3900:]
            self.send_message(chat_id, chunk, message_thread_id=message_thread_id)

    def _request(self, method: str, payload: dict[str, Any], *, timeout: int) -> Any:
        url = f"{self.api_base}/bot{self.token}/{method}"
        data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=data,
            headers={"content-type": "application/json"},
            method="POST",
        )
        try:
            with self.opener.open(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            raise TelegramApiError(f"Telegram API HTTP {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            raise TelegramApiError(f"Telegram API request failed: {exc}") from exc
        parsed = json.loads(raw)
        if not parsed.get("ok"):
            raise TelegramApiError(parsed.get("description") or raw)
        return parsed.get("result")


class TelegramBot:
    def __init__(
        self,
        *,
        db_path: str | Path | None,
        token: str,
        poll_timeout: int = 30,
        codex_timeout: int = 1800,
        proxy_url: str | None = None,
    ) -> None:
        self.db_path = db_path
        self.poll_timeout = poll_timeout
        self.codex_timeout = codex_timeout
        self.client = TelegramClient(token, proxy_url=proxy_url)
        self._active_lock = threading.Lock()
        self._active_bindings: set[tuple[str, str]] = set()

    def run(self) -> None:
        offset = None
        print("mycodex telegram bot started", file=sys.stderr)
        while True:
            try:
                updates = self.client.get_updates(offset=offset, timeout=self.poll_timeout)
                for update in updates:
                    offset = int(update["update_id"]) + 1
                    self.handle_update(update)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"telegram polling error: {exc}", file=sys.stderr)
                time.sleep(5)

    def handle_update(self, update: dict[str, Any]) -> None:
        context = _context_from_update(update)
        if context is None:
            return
        with Store(self.db_path) as store:
            if not store.telegram_user_allowed(context.user_id):
                self.client.send_message(
                    context.chat_id,
                    (
                        "Access denied.\n"
                        f"Telegram user id: {context.user_id}\n"
                        f"Allow locally with: mycodex telegram allow {context.user_id}"
                    ),
                    message_thread_id=context.message_thread_id,
                )
                return
            user = store.get_telegram_user(context.user_id)
            if user and context.username and context.username != user["username"]:
                store.allow_telegram_user(context.user_id, context.username)

            command = parse_command(context.text)
            if command is None:
                binding = store.get_telegram_binding(
                    chat_id=context.chat_id,
                    message_thread_id=context.message_thread_id,
                )
                if binding is None:
                    self.client.send_message(
                        context.chat_id,
                        "This chat/topic is not bound. Use /bind PROJECT first.",
                        message_thread_id=context.message_thread_id,
                    )
                    return
                self._start_prompt_task(context, binding, context.text, kind="telegram")
                return

            name, args = command
            handler = getattr(self, f"_cmd_{name}", None)
            if handler is None:
                self.client.send_message(
                    context.chat_id,
                    "Unknown command. Use /help.",
                    message_thread_id=context.message_thread_id,
                )
                return
            handler(store, context, args)

    def _cmd_start(self, _store: Store, context: TelegramContext, _args: str) -> None:
        self.client.send_message(
            context.chat_id,
            f"{HELP_TEXT}\nChat id: {context.chat_id}\nTopic id: {context.message_thread_id or '-'}",
            message_thread_id=context.message_thread_id,
        )

    def _cmd_help(self, store: Store, context: TelegramContext, args: str) -> None:
        self._cmd_start(store, context, args)

    def _cmd_projects(self, store: Store, context: TelegramContext, _args: str) -> None:
        projects = store.list_projects()
        if not projects:
            text = "No mycodex projects. Add one locally with `mycodex projects connect NAME PATH`."
        else:
            text = "\n".join(
                f"- {project['name']} ({project['path']})" for project in projects
            )
        self.client.send_chunks(context.chat_id, text, message_thread_id=context.message_thread_id)

    def _cmd_bind(self, store: Store, context: TelegramContext, args: str) -> None:
        parts = args.split()
        if not parts:
            self.client.send_message(
                context.chat_id,
                "Usage: /bind PROJECT [THREAD_ID]",
                message_thread_id=context.message_thread_id,
            )
            return
        project_name = parts[0]
        thread_id = parts[1] if len(parts) > 1 else None
        self._bind(store, context, project_name, thread_id)

    def _cmd_bind_thread(self, store: Store, context: TelegramContext, args: str) -> None:
        parts = args.split()
        if len(parts) != 2:
            self.client.send_message(
                context.chat_id,
                "Usage: /bind_thread PROJECT THREAD_ID",
                message_thread_id=context.message_thread_id,
            )
            return
        self._bind(store, context, parts[0], parts[1])

    def _bind(
        self,
        store: Store,
        context: TelegramContext,
        project_name: str,
        thread_id: str | None,
    ) -> None:
        project = store.get_project(project_name)
        if project is None:
            self.client.send_message(
                context.chat_id,
                f"Unknown project {project_name!r}. Use /projects.",
                message_thread_id=context.message_thread_id,
            )
            return
        if thread_id is None:
            thread_id = project["default_thread_id"]
            if thread_id is None:
                latest = store.latest_session(project["id"])
                thread_id = latest["thread_id"] if latest else None
        store.set_telegram_binding(
            chat_id=context.chat_id,
            message_thread_id=context.message_thread_id,
            project_id=project["id"],
            thread_id=thread_id,
        )
        resume = _resume_command(project["path"], thread_id) if thread_id else "(next prompt will create a thread)"
        self.client.send_message(
            context.chat_id,
            (
                f"Bound to {project['name']}.\n"
                f"Thread: {thread_id or '-'}\n"
                f"Local resume: {resume}"
            ),
            message_thread_id=context.message_thread_id,
        )

    def _cmd_binding(self, store: Store, context: TelegramContext, _args: str) -> None:
        binding = self._require_binding(store, context)
        if binding is None:
            return
        self.client.send_message(
            context.chat_id,
            (
                f"Project: {binding['project_name']}\n"
                f"Path: {binding['project_path']}\n"
                f"Thread: {binding['thread_id'] or '-'}\n"
                f"Local resume: {_resume_command(binding['project_path'], binding['thread_id']) if binding['thread_id'] else '-'}"
            ),
            message_thread_id=context.message_thread_id,
        )

    def _cmd_unbind(self, store: Store, context: TelegramContext, _args: str) -> None:
        removed = store.remove_telegram_binding(
            chat_id=context.chat_id,
            message_thread_id=context.message_thread_id,
        )
        self.client.send_message(
            context.chat_id,
            "Binding removed." if removed else "No binding was set.",
            message_thread_id=context.message_thread_id,
        )

    def _cmd_status(self, store: Store, context: TelegramContext, _args: str) -> None:
        binding = self._require_binding(store, context)
        if binding is None:
            return
        snapshot = snapshot_to_jsonable(project_snapshot(store, binding["project_name"], include_detail=False))
        latest_run = snapshot["latest_run"]
        text = [
            f"Project: {binding['project_name']}",
            f"Health: {snapshot['health']['label']} ({snapshot['health']['severity']})",
            f"Sessions: {snapshot['session_count']}",
            f"Open todos: {snapshot['open_todo_count']}",
            f"Thread: {binding['thread_id'] or '-'}",
        ]
        if snapshot["git"]["state"] == "git":
            text.append(
                f"Git: {snapshot['git']['branch']} {snapshot['git']['sha']} dirty={snapshot['git']['dirty_count']}"
            )
        else:
            text.append(f"Git: {snapshot['git']['summary']}")
        if latest_run:
            text.append(f"Latest run: #{latest_run['id']} {latest_run['kind']} {latest_run['status']}")
        text.extend(f"- {reason}" for reason in snapshot["health"]["reasons"])
        self.client.send_chunks(context.chat_id, "\n".join(text), message_thread_id=context.message_thread_id)

    def _cmd_sessions(self, store: Store, context: TelegramContext, _args: str) -> None:
        binding = self._require_binding(store, context)
        if binding is None:
            return
        project = store.get_project(binding["project_name"])
        sessions = store.list_sessions(project["id"]) if project else []
        if not sessions:
            text = "No stored sessions. Use /sync."
        else:
            lines = []
            for session in sessions[:10]:
                marker = "*" if session["thread_id"] == binding["thread_id"] else " "
                title = (session["name"] or session["preview"] or "").replace("\n", " ")
                lines.append(
                    f"{marker} {session['thread_id']} {session['status_type']} {format_ts(session['updated_at'])} {title[:80]}"
                )
            text = "\n".join(lines)
        self.client.send_chunks(context.chat_id, text, message_thread_id=context.message_thread_id)

    def _cmd_sync(self, store: Store, context: TelegramContext, _args: str) -> None:
        binding = self._require_binding(store, context)
        if binding is None:
            return
        try:
            threads = sync_sessions(store, binding["project_name"])
        except Exception as exc:
            self.client.send_message(
                context.chat_id,
                f"Sync failed: {exc}",
                message_thread_id=context.message_thread_id,
            )
            return
        if not binding["thread_id"]:
            project = store.get_project(binding["project_name"])
            thread_id = project["default_thread_id"] if project else None
            if thread_id:
                store.set_telegram_binding(
                    chat_id=context.chat_id,
                    message_thread_id=context.message_thread_id,
                    project_id=project["id"],
                    thread_id=thread_id,
                )
        self.client.send_message(
            context.chat_id,
            f"Synced {len(threads)} session(s).",
            message_thread_id=context.message_thread_id,
        )

    def _cmd_runs(self, store: Store, context: TelegramContext, _args: str) -> None:
        binding = self._require_binding(store, context)
        if binding is None:
            return
        project = store.get_project(binding["project_name"])
        runs = store.list_runs(project["id"], limit=8) if project else []
        if not runs:
            text = "No runs."
        else:
            text = "\n".join(
                f"#{run['id']} {run['kind']} {run['status']} {format_ts(run['started_at'])}"
                for run in runs
            )
        self.client.send_chunks(context.chat_id, text, message_thread_id=context.message_thread_id)

    def _cmd_summary(self, store: Store, context: TelegramContext, _args: str) -> None:
        binding = self._require_binding(store, context)
        if binding is not None:
            self._start_prompt_task(
                context,
                binding,
                daily_summary_prompt(binding["project_name"]),
                kind="daily_summary",
                sandbox="read-only",
                approval_policy="never",
            )

    def _cmd_send(self, store: Store, context: TelegramContext, args: str) -> None:
        binding = self._require_binding(store, context)
        if binding is None:
            return
        prompt = args.strip()
        if not prompt:
            self.client.send_message(
                context.chat_id,
                "Usage: /send PROMPT",
                message_thread_id=context.message_thread_id,
            )
            return
        self._start_prompt_task(context, binding, prompt, kind="telegram")

    def _cmd_new(self, store: Store, context: TelegramContext, args: str) -> None:
        binding = self._require_binding(store, context)
        if binding is None:
            return
        prompt = args.strip() or "请基于当前项目开始一个新的 Codex session，并总结项目状态。"
        self._start_prompt_task(context, binding, prompt, kind="telegram:new_thread", new_thread=True)

    def _cmd_todo(self, store: Store, context: TelegramContext, args: str) -> None:
        binding = self._require_binding(store, context)
        if binding is None:
            return
        project = store.get_project(binding["project_name"])
        if project is None:
            return
        parts = args.split(maxsplit=1)
        action = parts[0] if parts else "list"
        rest = parts[1] if len(parts) > 1 else ""
        if action == "list":
            todos = store.list_todos(project["id"], limit=12)
            if not todos:
                text = "No todos."
            else:
                text = "\n".join(
                    f"#{todo['id']} [{todo['status']}] p{todo['priority']} {todo['title']}"
                    for todo in todos
                )
            self.client.send_chunks(context.chat_id, text, message_thread_id=context.message_thread_id)
            return
        if action == "add":
            if not rest.strip():
                text = "Usage: /todo add TITLE"
            else:
                todo_id = store.add_todo(project["id"], title=rest.strip())
                text = f"Todo #{todo_id} added."
            self.client.send_message(context.chat_id, text, message_thread_id=context.message_thread_id)
            return
        if action in {"open", "doing", "blocked", "done", "archived"}:
            try:
                todo_id = int(rest.strip())
            except ValueError:
                self.client.send_message(
                    context.chat_id,
                    f"Usage: /todo {action} TODO_ID",
                    message_thread_id=context.message_thread_id,
                )
                return
            todo = store.get_todo(todo_id)
            if todo is None or todo["project_id"] != project["id"]:
                text = f"Todo #{todo_id} was not found in {project['name']}."
            else:
                store.update_todo_status(todo_id, action)
                text = f"Todo #{todo_id} -> {action}."
            self.client.send_message(context.chat_id, text, message_thread_id=context.message_thread_id)
            return
        self.client.send_message(
            context.chat_id,
            "Usage: /todo list | /todo add TITLE | /todo done TODO_ID",
            message_thread_id=context.message_thread_id,
        )

    def _require_binding(self, store: Store, context: TelegramContext):
        binding = store.get_telegram_binding(
            chat_id=context.chat_id,
            message_thread_id=context.message_thread_id,
        )
        if binding is None:
            self.client.send_message(
                context.chat_id,
                "This chat/topic is not bound. Use /bind PROJECT first.",
                message_thread_id=context.message_thread_id,
            )
        return binding

    def _start_prompt_task(
        self, context: TelegramContext, binding, prompt: str, *, kind: str,
        new_thread: bool = False, sandbox: str | None = None, approval_policy: str | None = None,
    ) -> None:
        if context.update_id is not None:
            with Store(self.db_path) as store:
                prior = store.conn.execute("SELECT id,status FROM jobs WHERE source='telegram' AND idempotency_key=? LIMIT 1",
                    (f"telegram:{context.chat_id}:{context.update_id}",)).fetchone()
            if prior is not None:
                self.client.send_message(context.chat_id, f"Task #{prior['id']} is already saved ({prior['status']}).",
                    message_thread_id=context.message_thread_id)
                return
        try:
            self._start_prompt_task_impl(context, binding, prompt, kind=kind,
                new_thread=new_thread, sandbox=sandbox, approval_policy=approval_policy)
        except BaseException:
            with self._active_lock:
                self._active_bindings.discard((context.chat_id, context.message_thread_id or ""))
            raise

    def _start_prompt_task_impl(
        self,
        context: TelegramContext,
        binding,
        prompt: str,
        *,
        kind: str,
        new_thread: bool = False,
        sandbox: str | None = None,
        approval_policy: str | None = None,
    ) -> None:
        binding_key = (context.chat_id, context.message_thread_id or "")
        with self._active_lock:
            if binding_key in self._active_bindings:
                self.client.send_message(
                    context.chat_id,
                    "This chat/topic already has a Telegram task running. Wait for the final result before sending another prompt.",
                    message_thread_id=context.message_thread_id,
                )
                return
            self._active_bindings.add(binding_key)

        active = None
        with Store(self.db_path) as store:
            active = store.active_run_for_project(binding["project_id"], kind_prefix="telegram")
            if active is None and binding["thread_id"] and not new_thread:
                active = store.active_run_for_thread(binding["thread_id"])
        if active is not None:
            with self._active_lock:
                self._active_bindings.discard(binding_key)
            self.client.send_message(
                context.chat_id,
                f"Project/thread is busy with run #{active['id']} ({active['kind']}). Wait for it to finish before sending another prompt.",
                message_thread_id=context.message_thread_id,
            )
            return
        from .jobs import JobQueue
        payload = {"prompt": prompt, "kind": kind, "new_thread": new_thread,
                   "timeout": self.codex_timeout, "update_default_thread": False}
        if binding["thread_id"] and not new_thread:
            payload["thread_id"] = binding["thread_id"]
        if sandbox:
            payload["sandbox"] = sandbox
        if approval_policy:
            payload["approval_policy"] = approval_policy
        with Store(self.db_path) as store:
            job = JobQueue(store).submit(binding["project_name"], "prompt", payload, source="telegram",
                idempotency_key=f"telegram:{context.chat_id}:{context.update_id}" if context.update_id is not None else None)
        self.client.send_message(
            context.chat_id,
            f"Saved task #{job['id']} for {binding['project_name']}...",
            message_thread_id=context.message_thread_id,
        )
        thread = threading.Thread(
            target=self._prompt_worker,
            args=(context, {key: binding[key] for key in binding.keys()}, prompt),
            kwargs={
                "kind": kind,
                "new_thread": new_thread,
                "sandbox": sandbox,
                "approval_policy": approval_policy,
                "binding_key": binding_key,
                "job_id": job["id"],
            },
            daemon=True,
        )
        thread.start()

    def _prompt_worker(
        self,
        context: TelegramContext,
        binding: dict[str, Any],
        prompt: str,
        *,
        kind: str,
        new_thread: bool,
        sandbox: str | None,
        approval_policy: str | None,
        binding_key: tuple[str, str],
        job_id: int,
    ) -> None:
        try:
            with Store(self.db_path) as store:
                from .jobs import JobQueue, run_worker
                run_worker(store, once=True, job_id=job_id)
                job = JobQueue(store).get(job_id)
                result = job["result"]
                if "thread_id" not in result:
                    self.client.send_chunks(context.chat_id,
                        f"Task #{job_id}: {job['status']}. {job['error'] or 'A local worker can continue queued work.'}",
                        message_thread_id=context.message_thread_id)
                    return
                project = store.get_project(binding["project_name"])
                if project is not None and (new_thread or not binding["thread_id"]):
                    store.set_telegram_binding(
                        chat_id=context.chat_id,
                        message_thread_id=context.message_thread_id,
                        project_id=project["id"],
                        thread_id=result["thread_id"],
                    )
                resume = _resume_command(binding["project_path"], result["thread_id"])
                header = (
                    f"Run #{result['run_id']} {result['status']}\n"
                    f"Thread: {result['thread_id']}\n"
                    f"Local resume: {resume}\n\n"
                )
                output = result["output_text"] or "(no output)"
            self.client.send_chunks(
                context.chat_id,
                header + output,
                message_thread_id=context.message_thread_id,
            )
        except Exception as exc:
            self.client.send_chunks(
                context.chat_id,
                f"Run failed: {exc}",
                message_thread_id=context.message_thread_id,
            )
        finally:
            with self._active_lock:
                self._active_bindings.discard(binding_key)


def notify_project_bindings(
    store: Store,
    project_id: int,
    text: str,
    *,
    token: str | None = None,
    token_file: str | Path | None = None,
    token_env: str = "MYCODEX_TELEGRAM_BOT_TOKEN",
    proxy_url: str | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    bindings = [
        {key: binding[key] for key in binding.keys()}
        for binding in store.list_telegram_bindings()
        if binding["project_id"] == project_id
    ]
    result: dict[str, Any] = {
        "attempted": len(bindings),
        "sent": 0,
        "skipped": 0,
        "errors": [],
    }
    if not bindings:
        return result
    if client is None:
        resolved_token = token_from_sources(
            token=token,
            token_file=token_file,
            token_env=token_env,
            db_path=store.path,
            require=False,
        )
        if not resolved_token:
            result["skipped"] = len(bindings)
            result["errors"].append(
                f"missing Telegram bot token; set {token_env} or create "
                f"{default_token_file(store.path)}"
            )
            return result
        client = TelegramClient(resolved_token, proxy_url=proxy_url)
    for binding in bindings:
        try:
            client.send_chunks(
                binding["chat_id"],
                text,
                message_thread_id=binding["message_thread_id"] or None,
            )
            result["sent"] += 1
        except Exception as exc:
            result["errors"].append(
                f"chat={binding['chat_id']} topic={binding['message_thread_id'] or '-'}: {exc}"
            )
    return result


def parse_command(text: str) -> tuple[str, str] | None:
    text = text.strip()
    if not text.startswith("/"):
        return None
    first, _, rest = text.partition(" ")
    name = first[1:].split("@", 1)[0].replace("-", "_").lower()
    return name, rest.strip()


def _context_from_update(update: dict[str, Any]) -> TelegramContext | None:
    message = update.get("message")
    if not message:
        return None
    text = message.get("text")
    if not text:
        return None
    chat = message.get("chat") or {}
    user = message.get("from") or {}
    if "id" not in chat or "id" not in user:
        return None
    username = user.get("username") or ""
    return TelegramContext(
        chat_id=str(chat["id"]),
        message_thread_id=str(message["message_thread_id"]) if message.get("message_thread_id") else None,
        user_id=str(user["id"]),
        username=username,
        chat_type=chat.get("type") or "",
        text=text,
        update_id=update.get("update_id"),
    )


def _resume_command(project_path: str, thread_id: str | None) -> str:
    if not thread_id:
        return "-"
    return f"codex resume {shlex.quote(thread_id)} -C {shlex.quote(project_path)}"


def token_from_env(env_name: str) -> str:
    token = os.environ.get(env_name)
    if not token:
        raise ValueError(f"missing Telegram bot token; set {env_name}")
    return token


def token_from_file(path: str | Path) -> str:
    token_path = Path(path).expanduser()
    try:
        token = token_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError as exc:
        raise ValueError(f"Telegram bot token file does not exist: {token_path}") from exc
    if not token:
        raise ValueError(f"Telegram bot token file is empty: {token_path}")
    return token


def token_from_sources(
    *,
    token: str | None = None,
    token_file: str | Path | None = None,
    token_env: str = "MYCODEX_TELEGRAM_BOT_TOKEN",
    db_path: str | Path | None = None,
    require: bool = True,
) -> str | None:
    if token:
        return token
    if token_file:
        return token_from_file(token_file)
    env_token = os.environ.get(token_env)
    if env_token:
        return env_token
    default_path = default_token_file(db_path)
    if default_path.exists():
        return token_from_file(default_path)
    if require:
        raise ValueError(
            f"missing Telegram bot token; pass --token, pass --token-file, set {token_env}, "
            f"or create {default_path}"
        )
    return None


def default_token_file(db_path: str | Path | None = None) -> Path:
    if db_path is None:
        return DEFAULT_TOKEN_FILE
    return Path(db_path).expanduser().resolve().parent / "secrets" / "telegram_bot_token"
