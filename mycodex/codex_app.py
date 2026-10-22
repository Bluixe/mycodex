from __future__ import annotations

import json
import math
import os
import queue
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from . import __version__


class CodexAppServerError(RuntimeError):
    pass


class CodexRuntimeCompatibilityError(CodexAppServerError):
    """The selected binary cannot load the user's current configuration."""


@dataclass
class TurnResult:
    thread_id: str
    turn_id: str | None
    output_text: str
    completed: bool
    raw: dict[str, Any]
    status: str = "unknown"
    token_usage: dict[str, Any] = field(default_factory=dict)


class CodexAppServerClient:
    """Single-consumer JSON-lines client for the local Codex app-server.

    Serialize request/wait calls. Control methods may be called from a wait
    callback; concurrent consumers on separate Python threads are unsupported.
    """

    def __init__(self, codex_bin: str = "codex") -> None:
        self.codex_bin = _resolve_codex_bin(codex_bin)
        self.proc: subprocess.Popen[str] | None = None
        self._next_id = 1
        self._events: queue.Queue[dict[str, Any]] = queue.Queue()
        self._buffer: list[dict[str, Any]] = []
        self._reader: threading.Thread | None = None
        self._stderr: threading.Thread | None = None
        self._stderr_lines: list[str] = []
        self._write_lock = threading.Lock()
        self.interrupt_grace_seconds = 3.0
        self.operation_deadline: float | None = None

    def __enter__(self) -> "CodexAppServerClient":
        self.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def start(self) -> None:
        if self.proc is not None:
            return
        self._events = queue.Queue()
        self._buffer.clear()
        self.proc = subprocess.Popen(
            [self.codex_bin, "app-server", "--listen", "stdio://"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        self._reader = threading.Thread(target=self._read_stdout, daemon=True)
        self._reader.start()
        self._stderr = threading.Thread(target=self._read_stderr, daemon=True)
        self._stderr.start()
        try:
            self.request(
                "initialize",
                {
                    "clientInfo": {
                        "name": "mycodex",
                        "title": "mycodex",
                        "version": __version__,
                    },
                    "capabilities": {"experimentalApi": True},
                },
                timeout=30,
            )
            self.notify("initialized")
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        proc = self.proc
        if proc is None:
            return
        if proc.stdin and not proc.stdin.closed:
            try:
                proc.stdin.close()
            except OSError:
                pass
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
        for stream in (proc.stdout, proc.stderr):
            if stream is not None:
                stream.close()
        self.proc = None

    def _read_stdout(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        try:
            for line in self.proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    self._events.put({"method": "client/parseError", "params": {"line": line}})
                    continue
                if isinstance(obj, dict):
                    self._events.put(obj)
        finally:
            self._events.put({"method": "client/disconnected", "params": {}})

    def _read_stderr(self) -> None:
        assert self.proc is not None and self.proc.stderr is not None
        for line in self.proc.stderr:
            self._stderr_lines.append(line.rstrip())

    def _write(self, obj: dict[str, Any]) -> None:
        if self.proc is None or self.proc.stdin is None or self.proc.stdin.closed:
            raise CodexAppServerError("codex app-server is not running")
        with self._write_lock:
            try:
                self.proc.stdin.write(json.dumps(obj, ensure_ascii=False) + "\n")
                self.proc.stdin.flush()
            except (OSError, ValueError) as exc:
                raise CodexAppServerError("codex app-server connection closed") from exc

    def notify(self, method: str, params: Any | None = None) -> None:
        msg: dict[str, Any] = {"method": method}
        if params is not None:
            msg["params"] = params
        self._write(msg)

    def _rpc_timeout(self, method: str, timeout: float) -> float:
        timeout = float(timeout)
        if not math.isfinite(timeout) or timeout <= 0:
            raise CodexAppServerError(f"no time budget remaining before {method}")
        if method in {"turn/interrupt", "thread/archive"}:
            return min(timeout, 5.0)
        if self.operation_deadline is not None:
            remaining = self.operation_deadline - time.monotonic()
            if remaining <= 0:
                raise CodexAppServerError(f"operation deadline exceeded before {method}")
            timeout = min(timeout, remaining)
        return timeout

    def request(self, method: str, params: Any | None = None, timeout: int = 60) -> Any:
        timeout = self._rpc_timeout(method, timeout)
        req_id = self._next_id
        self._next_id += 1
        msg: dict[str, Any] = {"id": req_id, "method": method, "params": params}
        self._write(msg)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            event = self._next_event(deadline, include_buffer=False)
            if event is None:
                break
            if event.get("method") == "client/disconnected":
                # Preserve EOF for the turn waiter after nested interrupt/read RPCs.
                self._buffer.append(event)
                raise CodexAppServerError(f"codex app-server disconnected while waiting for {method}")
            if event.get("id") == req_id and ("result" in event or "error" in event):
                if "error" in event:
                    error_text = json.dumps(event["error"], ensure_ascii=False)
                    if ("config" in error_text.lower() and any(marker in error_text.lower() for marker in (
                            "unknown variant", "failed to load", "failed to parse", "invalid configuration"))):
                        raise CodexRuntimeCompatibilityError(
                            f"{method} failed: the selected Codex runtime cannot load the configured settings. "
                            "Select a compatible executable with MYCODEX_CODEX_BIN or use the current "
                            "desktop-provided CODEX_CLI_PATH. The configuration was not modified."
                        )
                    raise CodexAppServerError(f"{method} failed: {event['error']}")
                return event.get("result")
            if self._is_server_request(event):
                self._auto_respond_to_server_request(event)
            else:
                self._buffer.append(event)
        stderr = "\n".join(self._stderr_lines[-10:])
        raise CodexAppServerError(f"timed out waiting for {method}; stderr={stderr}")

    def _next_event(self, deadline: float, *, include_buffer: bool = True) -> dict[str, Any] | None:
        if include_buffer and self._buffer:
            return self._buffer.pop(0)
        remaining = max(0.0, deadline - time.monotonic())
        if remaining == 0:
            return None
        try:
            return self._events.get(timeout=remaining)
        except queue.Empty:
            return None

    @staticmethod
    def _is_server_request(event: dict[str, Any]) -> bool:
        return "id" in event and "method" in event and "result" not in event and "error" not in event

    def _auto_respond_to_server_request(self, event: dict[str, Any]) -> None:
        method = event.get("method")
        req_id = event.get("id")
        if req_id is None:
            return
        result: Any
        if method == "item/commandExecution/requestApproval":
            result = {"decision": "decline"}
        elif method == "item/fileChange/requestApproval":
            result = {"decision": "decline"}
        elif method == "item/permissions/requestApproval":
            result = {"permissions": {}, "scope": "turn"}
        elif method == "item/tool/requestUserInput":
            result = {"answers": {}}
        elif method == "mcpServer/elicitation/request":
            result = {"action": "decline", "content": None, "_meta": None}
        else:
            result = {"error": f"mycodex cannot handle server request {method}"}
        self._write({"id": req_id, "result": result})

    def list_threads(
        self,
        *,
        cwd: str | None = None,
        limit: int = 100,
        use_state_db_only: bool = True,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "limit": limit,
            "sortKey": "updated_at",
            "sortDirection": "desc",
            "sourceKinds": [
                "cli",
                "vscode",
                "exec",
                "appServer",
                "subAgent",
                "subAgentReview",
                "subAgentCompact",
                "subAgentThreadSpawn",
                "subAgentOther",
                "unknown",
            ],
            "useStateDbOnly": use_state_db_only,
        }
        if cwd:
            params["cwd"] = cwd
        result = self.request("thread/list", params, timeout=60)
        return list((result or {}).get("data") or [])

    def start_or_resume_thread(
        self,
        *,
        thread_id: str | None,
        cwd: str,
        sandbox: str = "read-only",
        approval_policy: str = "never",
        model: str | None = None,
        reasoning_effort: str | None = None,
        config_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "cwd": cwd,
            "sandbox": sandbox,
            "approvalPolicy": approval_policy,
        }
        if model:
            params["model"] = model
        if config_overrides is not None:
            params['config'] = dict(config_overrides)
        if thread_id:
            params["threadId"] = thread_id
            result = self.request("thread/resume", params, timeout=90)
        else:
            result = self.request("thread/start", params, timeout=90)
        thread = (result or {}).get("thread")
        if not thread:
            raise CodexAppServerError("Codex did not return a thread")
        return thread

    def start_turn(
        self,
        *,
        thread_id: str,
        prompt: str,
        cwd: str | None = None,
        approval_policy: str | None = None,
        model: str | None = None,
        reasoning_effort: str | None = None,
        output_schema: dict[str, Any] | None = None,
    ) -> tuple[str | None, dict[str, Any]]:
        params: dict[str, Any] = {
            "threadId": thread_id,
            "input": [{"type": "text", "text": prompt, "text_elements": []}],
        }
        if cwd:
            params["cwd"] = cwd
        if approval_policy:
            params["approvalPolicy"] = approval_policy
        if model:
            params["model"] = model
        if reasoning_effort:
            params["effort"] = reasoning_effort
        if output_schema is not None:
            params["outputSchema"] = output_schema
        result = self.request("turn/start", params, timeout=60)
        turn = (result or {}).get("turn") or {}
        if not turn.get("id"):
            raise CodexAppServerError("Codex did not return a turn id")
        return turn.get("id"), turn

    def interrupt_turn(self, *, thread_id: str, turn_id: str, timeout: int = 5) -> Any:
        return self.request("turn/interrupt", {"threadId": thread_id, "turnId": turn_id}, timeout=timeout)

    def steer_turn(self, *, thread_id: str, turn_id: str, prompt: str) -> Any:
        return self.request(
            "turn/steer",
            {"threadId": thread_id, "expectedTurnId": turn_id,
             "input": [{"type": "text", "text": prompt, "text_elements": []}]},
        )

    def wait_for_turn(
        self,
        *,
        thread_id: str,
        turn_id: str | None,
        timeout: int,
        on_event: Callable[[dict[str, Any]], None] | None = None,
        cancellation_callback: Callable[[], bool] | None = None,
        usage_callback: Callable[[dict[str, Any]], bool | None] | None = None,
        _interrupt_on_timeout: bool = True,
        _usage_baseline: dict[str, int] | None = None,
    ) -> TurnResult:
        deadline = time.monotonic() + timeout
        deltas: dict[str, list[str]] = {}
        phases: dict[str, str] = {}
        messages: dict[str, str] = {}
        completed_turn: dict[str, Any] | None = None
        errors: list[Any] = []
        token_usage: dict[str, Any] = {}
        usage_baseline: dict[str, int] | None = _usage_baseline
        status = "timeout"
        stop_reason = ""
        requested_stop_reason = ""
        drained_output = ""
        interrupt_requested = False

        def matches(params: dict[str, Any]) -> bool:
            return params.get("threadId") == thread_id and (
                turn_id is None or params.get("turnId") == turn_id
            )

        while time.monotonic() < deadline:
            if cancellation_callback is not None and cancellation_callback():
                status, stop_reason = "cancelled", "cancelled by caller"
                break
            poll_deadline = min(deadline, time.monotonic() + 0.25) if cancellation_callback else deadline
            event = self._next_event(poll_deadline)
            if event is None:
                if time.monotonic() >= deadline:
                    break
                continue
            if self._is_server_request(event):
                self._auto_respond_to_server_request(event)
                continue
            method = event.get("method")
            if method == "client/disconnected":
                status, stop_reason = "failed", "app-server disconnected before turn completion"
                break
            if method == "client/parseError":
                status, stop_reason = "failed", "invalid JSON from app-server"
                errors.append(event.get("params") or {})
                break
            params = event.get("params") or {}
            # Callbacks may persist events or enforce budgets; never mix another
            # thread's output/usage into this run.
            if method not in {"client/parseError"} and params.get("threadId") != thread_id:
                continue
            if on_event is not None:
                on_event(event)
            if method == "item/agentMessage/delta":
                if matches(params):
                    deltas.setdefault(str(params.get("itemId") or ""), []).append(params.get("delta") or "")
            elif method in {"item/started", "item/completed"}:
                item = params.get("item") or {}
                if matches(params) and item.get("type") == "agentMessage":
                    item_id = str(item.get("id") or "")
                    phases[item_id] = item.get("phase") or ""
                    if method == "item/completed" and isinstance(item.get("text"), str):
                        messages[item_id] = item["text"]
            elif method == "thread/tokenUsage/updated":
                if params.get("turnId") not in (None, turn_id):
                    continue
                usage = params.get("tokenUsage")
                if isinstance(usage, dict):
                    token_usage = dict(usage)
                    total, last = usage.get("total"), usage.get("last")
                    if isinstance(total, dict) and isinstance(last, dict):
                        if usage_baseline is None:
                            usage_baseline = {
                                key: max(0, value - last.get(key, 0))
                                for key, value in total.items()
                                if type(value) is int and type(last.get(key, 0)) is int
                            }
                        token_usage["turn"] = {
                            key: max(0, value - usage_baseline.get(key, value))
                            for key, value in total.items() if type(value) is int
                        }
                    if usage_callback is not None and usage_callback(dict(token_usage)) is False:
                        status, stop_reason = "cancelled", "token budget callback requested stop"
                        break
            elif method == "turn/completed":
                turn = params.get("turn") or {}
                if turn_id is None or turn.get("id") == turn_id:
                    completed_turn = turn
                    status = turn.get("status") or "failed"
                    if status not in {"completed", "interrupted", "failed"}:
                        stop_reason = f"invalid terminal turn status: {status}"
                        status = "failed"
                    if turn.get("error"):
                        errors.append(turn["error"])
                        status = "failed"
                    break
            elif method in {"turn/failed", "turn/interrupted"} and matches(params):
                status = "failed" if method == "turn/failed" else "interrupted"
                errors.append(params)
                break
            elif method == "error":
                if params.get("turnId") in (None, turn_id):
                    errors.append(params)
                    # Recoverable provider errors can be followed by a successful
                    # retry; an explicitly non-retryable error is terminal.
                    if params.get("willRetry") is False:
                        status, stop_reason = "failed", "non-retryable Codex error"
                        break

        if _interrupt_on_timeout and status in {"timeout", "cancelled"} and turn_id:
            if not stop_reason:
                stop_reason = f"turn timed out after {timeout} seconds"
            requested_stop_reason = stop_reason
            interrupt_requested = True
            try:
                self.interrupt_turn(thread_id=thread_id, turn_id=turn_id)
            except CodexAppServerError as exc:
                errors.append({"interrupt_error": str(exc)})

            def observe_final_usage(usage):
                if usage_callback is not None:
                    usage_callback(usage)
                return True

            # An interrupt RPC response acknowledges the request, not completion
            # of in-flight tools. Only a terminal notification is a checkpoint.
            drained = self.wait_for_turn(
                thread_id=thread_id, turn_id=turn_id,
                timeout=min(5.0, max(0.0, self.interrupt_grace_seconds)),
                on_event=on_event, usage_callback=observe_final_usage,
                _interrupt_on_timeout=False, _usage_baseline=usage_baseline,
            )
            errors.extend(drained.raw.get("errors") or [])
            if drained.raw.get("turn") and drained.status in {"completed", "interrupted", "failed"}:
                completed_turn = drained.raw["turn"]
                status = drained.status
                drained_output = drained.output_text
                if drained.token_usage:
                    token_usage = drained.token_usage
                stop_reason = "" if status == "completed" else requested_stop_reason
            else:
                status = "needs_reconciliation"
                stop_reason = requested_stop_reason + "; terminal interruption was not confirmed"

        # The completed final message is authoritative. Joining deltas from all
        # items also joins commentary into JSON and can invalidate contracts.
        def choose(items: dict[str, str]) -> str:
            finals = [text for item_id, text in items.items() if phases.get(item_id) == "final_answer"]
            legacy = [text for item_id, text in items.items() if not phases.get(item_id)]
            return (finals or legacy or [""])[-1].strip()

        output = choose(messages)
        if not output:
            output = choose({key: "".join(chunks) for key, chunks in deltas.items()})
        if completed_turn and status == "completed" and not output:
            for item in completed_turn.get("items") or []:
                if item.get("type") == "agentMessage":
                    item_id = str(item.get("id") or "")
                    phases[item_id] = item.get("phase") or ""
                    messages[item_id] = item.get("text") or ""
            output = choose(messages)
        if drained_output:
            output = drained_output
        raw = {"turn": completed_turn or {}, "errors": errors, "status": status,
               "token_usage": token_usage, "stop_reason": stop_reason,
               "interrupt_requested": interrupt_requested,
               "interruption_confirmed": bool(completed_turn) and status == "interrupted",
               "terminal_confirmed": bool(completed_turn) and status in {"completed", "interrupted", "failed"},
               "requested_stop_reason": requested_stop_reason}
        return TurnResult(
            thread_id=thread_id,
            turn_id=turn_id,
            output_text=output,
            completed=status == "completed",
            raw=raw,
            status=status,
            token_usage=token_usage,
        )

    def read_latest_agent_message(self, thread_id: str, turn_id: str | None = None) -> str:
        result = self.request(
            "thread/read",
            {"threadId": thread_id, "includeTurns": True},
            timeout=60,
        )
        thread = (result or {}).get("thread") or {}
        for turn in reversed(thread.get("turns") or []):
            if turn_id is not None and turn.get("id") != turn_id:
                continue
            for item in reversed(turn.get("items") or []):
                if (item.get("type") == "agentMessage" and item.get("text")
                        and item.get("phase") != "commentary"):
                    return item["text"].strip()
        return ""


def _resolve_codex_bin(codex_bin: str) -> str:
    # Explicit caller intent wins; then an application-level user override,
    # then the current desktop's companion binary, and finally PATH discovery.
    if codex_bin != "codex":
        return codex_bin
    configured = os.environ.get("MYCODEX_CODEX_BIN")
    if configured:
        return configured
    desktop_binary = os.environ.get("CODEX_CLI_PATH")
    if desktop_binary and os.path.isfile(desktop_binary) and os.access(desktop_binary, os.X_OK):
        if os.name != "nt" or os.path.splitext(desktop_binary)[1].lower() in {".exe", ".com", ".cmd", ".bat"}:
            return desktop_binary
    found = shutil.which("codex")
    if found:
        return found
    for candidate in (
        "/opt/homebrew/bin/codex",
        "/usr/local/bin/codex",
        os.path.expanduser("~/.codex/bin/codex"),
    ):
        if os.path.exists(candidate):
            return candidate
    return codex_bin
