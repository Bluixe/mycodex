from __future__ import annotations

import json
import queue
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable


class CodexAppServerError(RuntimeError):
    pass


@dataclass
class TurnResult:
    thread_id: str
    turn_id: str | None
    output_text: str
    completed: bool
    raw: dict[str, Any]


class CodexAppServerClient:
    """Small JSON-lines client for `codex app-server --listen stdio://`."""

    def __init__(self, codex_bin: str = "codex") -> None:
        self.codex_bin = codex_bin
        self.proc: subprocess.Popen[str] | None = None
        self._next_id = 1
        self._events: queue.Queue[dict[str, Any]] = queue.Queue()
        self._buffer: list[dict[str, Any]] = []
        self._reader: threading.Thread | None = None
        self._stderr: threading.Thread | None = None
        self._stderr_lines: list[str] = []

    def __enter__(self) -> "CodexAppServerClient":
        self.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def start(self) -> None:
        if self.proc is not None:
            return
        self.proc = subprocess.Popen(
            [self.codex_bin, "app-server", "--listen", "stdio://"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self._reader = threading.Thread(target=self._read_stdout, daemon=True)
        self._reader.start()
        self._stderr = threading.Thread(target=self._read_stderr, daemon=True)
        self._stderr.start()
        self.request(
            "initialize",
            {
                "clientInfo": {
                    "name": "mycodex",
                    "title": "mycodex",
                    "version": "0.1.0",
                },
                "capabilities": {"experimentalApi": True},
            },
            timeout=30,
        )
        self.notify("initialized")

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
        self.proc = None

    def _read_stdout(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                self._events.put({"method": "client/parseError", "params": {"line": line}})
                continue
            self._events.put(obj)

    def _read_stderr(self) -> None:
        assert self.proc is not None and self.proc.stderr is not None
        for line in self.proc.stderr:
            self._stderr_lines.append(line.rstrip())

    def _write(self, obj: dict[str, Any]) -> None:
        if self.proc is None or self.proc.stdin is None or self.proc.stdin.closed:
            raise CodexAppServerError("codex app-server is not running")
        self.proc.stdin.write(json.dumps(obj, ensure_ascii=False) + "\n")
        self.proc.stdin.flush()

    def notify(self, method: str, params: Any | None = None) -> None:
        msg: dict[str, Any] = {"method": method}
        if params is not None:
            msg["params"] = params
        self._write(msg)

    def request(self, method: str, params: Any | None = None, timeout: int = 60) -> Any:
        req_id = self._next_id
        self._next_id += 1
        msg: dict[str, Any] = {"id": req_id, "method": method, "params": params}
        self._write(msg)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            event = self._next_event(deadline, include_buffer=False)
            if event is None:
                break
            if event.get("id") == req_id and ("result" in event or "error" in event):
                if "error" in event:
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
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "cwd": cwd,
            "sandbox": sandbox,
            "approvalPolicy": approval_policy,
        }
        if model:
            params["model"] = model
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
        result = self.request("turn/start", params, timeout=60)
        turn = (result or {}).get("turn") or {}
        return turn.get("id"), turn

    def wait_for_turn(
        self,
        *,
        thread_id: str,
        turn_id: str | None,
        timeout: int,
        on_event: Callable[[dict[str, Any]], None] | None = None,
    ) -> TurnResult:
        deadline = time.monotonic() + timeout
        deltas: list[str] = []
        completed_turn: dict[str, Any] | None = None
        final_agent_message = ""
        errors: list[Any] = []
        while time.monotonic() < deadline:
            event = self._next_event(deadline)
            if event is None:
                break
            if self._is_server_request(event):
                self._auto_respond_to_server_request(event)
                continue
            if on_event is not None:
                on_event(event)
            method = event.get("method")
            params = event.get("params") or {}
            if method == "item/agentMessage/delta":
                if params.get("threadId") == thread_id and (turn_id is None or params.get("turnId") == turn_id):
                    deltas.append(params.get("delta") or "")
            elif method == "item/completed":
                item = params.get("item") or {}
                if (
                    params.get("threadId") == thread_id
                    and (turn_id is None or params.get("turnId") == turn_id)
                    and item.get("type") == "agentMessage"
                ):
                    final_agent_message = item.get("text") or final_agent_message
            elif method == "turn/completed":
                if params.get("threadId") == thread_id:
                    turn = params.get("turn") or {}
                    if turn_id is None or turn.get("id") == turn_id:
                        completed_turn = turn
                        break
            elif method == "error":
                errors.append(params)
        output = "".join(deltas).strip() or final_agent_message.strip()
        if completed_turn and not output:
            output = self.read_latest_agent_message(thread_id)
        raw = {"turn": completed_turn or {}, "errors": errors}
        return TurnResult(
            thread_id=thread_id,
            turn_id=turn_id,
            output_text=output,
            completed=completed_turn is not None,
            raw=raw,
        )

    def read_latest_agent_message(self, thread_id: str) -> str:
        result = self.request(
            "thread/read",
            {"threadId": thread_id, "includeTurns": True},
            timeout=60,
        )
        thread = (result or {}).get("thread") or {}
        for turn in reversed(thread.get("turns") or []):
            for item in reversed(turn.get("items") or []):
                if item.get("type") == "agentMessage" and item.get("text"):
                    return item["text"].strip()
        return ""
