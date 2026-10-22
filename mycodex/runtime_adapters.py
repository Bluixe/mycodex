"""Optional runtime transports sharing mycodex's fail-closed turn semantics.

The SDK adapter is deliberately opt-in. It replaces transport/routing only:
contract validation, final-message selection, budgets and cancellation keep the
same implementation as the native app-server client.
"""
from __future__ import annotations

import importlib
import os
import queue
import subprocess
import threading
from typing import Any, Callable

from . import __version__
from .codex_app import CodexAppServerClient, CodexAppServerError, CodexRuntimeCompatibilityError


TESTED_SDK_VERSION = "0.163.0"


def _sdk_components():
    try:
        package = importlib.import_module("openai_codex")
        client = importlib.import_module("openai_codex.client")
        pydantic = importlib.import_module("pydantic")
    except ImportError as exc:
        raise CodexAppServerError(
            f"SDK backend requires the optional openai-codex=={TESTED_SDK_VERSION} dependency"
        ) from exc
    if package.__version__ != TESTED_SDK_VERSION:
        raise CodexAppServerError(
            f"SDK backend tested with openai-codex=={TESTED_SDK_VERSION}; "
            f"installed {package.__version__}. Select a tested dependency explicitly."
        )

    class WireResponse(pydantic.BaseModel):
        model_config = pydantic.ConfigDict(extra="allow")

    return client.CodexClient, client.CodexConfig, WireResponse


def decline_sdk_request(method: str, _params: dict | None = None) -> dict:
    """Override SDK defaults, which otherwise accept command/file approvals."""
    if method in {"item/commandExecution/requestApproval", "item/fileChange/requestApproval"}:
        return {"decision": "decline"}
    if method == "item/permissions/requestApproval":
        return {"permissions": {}, "scope": "turn"}
    if method == "item/tool/requestUserInput":
        return {"answers": {}}
    if method == "mcpServer/elicitation/request":
        return {"action": "decline", "content": None, "_meta": None}
    return {"error": f"mycodex cannot handle server request {method}"}


def _wire_result(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    # Preserve fields received on the wire rather than synthesizing SDK default
    # enum values, which can differ between adjacent runtime schema versions.
    return value.model_dump(mode="json", by_alias=True, exclude_none=True, exclude_unset=True)


class SdkAppServerClient(CodexAppServerClient):
    """SDK transport with bounded RPCs, explicit denial and shared turn parsing.

    sdk_config is intended for isolated transport tests and explicit advanced
    callers. Ordinary callers use the same binary resolver as the native client.
    """

    def __init__(self, codex_bin: str = "codex", *, sdk_config: Any | None = None) -> None:
        super().__init__(codex_bin)
        self._sdk_config = sdk_config
        self._sdk = None
        self._response_model = None
        self._stopping = threading.Event()
        self._notification_readers: list[threading.Thread] = []
        self._turn_readers: set[str] = set()

    def start(self) -> None:
        if self._sdk is not None:
            return
        factory, config_type, self._response_model = _sdk_components()
        config = self._sdk_config or config_type(
            codex_bin=self.codex_bin, client_name="mycodex", client_title="mycodex",
            client_version=__version__, experimental_api=True,
        )
        self._events = queue.Queue()
        self._buffer.clear()
        self._stopping.clear()
        self._turn_readers.clear()
        self._sdk = factory(config=config, approval_handler=decline_sdk_request)
        try:
            self._sdk.start()
            self._bounded_call("initialize", self._sdk.initialize, 30)
            self._start_reader(None)
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        self._stopping.set()
        sdk, self._sdk = self._sdk, None
        if sdk is not None:
            # SDK 0.163 closes stdin and terminates its process but leaves the
            # stdout/stderr wrappers open. This adapter pins that version and
            # explicitly releases the owned handles after its readers stop.
            proc = getattr(sdk, "_proc", None)
            sdk.close()
            if isinstance(proc, subprocess.Popen):
                if proc.poll() is None:
                    proc.kill()
                proc.wait(timeout=2)
                for stream in (proc.stdin, proc.stdout, proc.stderr):
                    if stream is not None and not stream.closed:
                        stream.close()
        for reader in self._notification_readers:
            if reader is not threading.current_thread():
                reader.join(timeout=0.5)
        self._notification_readers.clear()

    def _bounded_call(self, method: str, call: Callable[[], Any], timeout: float) -> Any:
        """SDK calls have no deadline parameter; close transport on expiry."""
        timeout = self._rpc_timeout(method, timeout)
        result: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)

        def run():
            try:
                self._rpc_timeout(method, timeout)
                result.put((True, call()))
            except BaseException as exc:
                result.put((False, exc))

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        try:
            succeeded, value = result.get(timeout=max(0, timeout))
        except queue.Empty:
            self.close()
            worker.join(timeout=0.5)
            raise CodexAppServerError(f"SDK timed out waiting for {method}; transport closed") from None
        if succeeded:
            return value
        detail = str(value).lower()
        if "config" in detail and any(marker in detail for marker in (
                "unknown variant", "failed to load", "failed to parse", "invalid configuration")):
            raise CodexRuntimeCompatibilityError(
                "The SDK's selected Codex runtime cannot load configured settings. "
                "Select a compatible executable with MYCODEX_CODEX_BIN; configuration was not modified."
            ) from value
        raise CodexAppServerError(f"SDK {method} failed: {value}") from value

    def request(self, method: str, params: Any | None = None, timeout: int = 60) -> dict:
        sdk = self._sdk
        if sdk is None:
            raise CodexAppServerError("SDK app-server is not running")
        params = dict(params or {})
        if method == "turn/start":
            call = lambda: sdk.turn_start(params["threadId"], params["input"], params=params)
        elif method == "thread/start":
            call = lambda: sdk.thread_start(params)
        elif method == "thread/resume":
            call = lambda: sdk.thread_resume(params["threadId"], params)
        elif method == "thread/list":
            call = lambda: sdk.thread_list(params)
        elif method == "thread/read":
            call = lambda: sdk.thread_read(params["threadId"], include_turns=params.get("includeTurns", False))
        elif method == "thread/archive":
            call = lambda: sdk.thread_archive(params["threadId"])
        elif method == "turn/interrupt":
            call = lambda: sdk.turn_interrupt(params["threadId"], params["turnId"])
        elif method == "turn/steer":
            call = lambda: sdk.turn_steer(params["threadId"], params["expectedTurnId"], params["input"])
        else:
            call = lambda: sdk.request(method, params, response_model=self._response_model)
        result = _wire_result(self._bounded_call(method, call, timeout))
        if method == "turn/start" and (turn_id := (result.get("turn") or {}).get("id")):
            self._start_reader(turn_id)
        return result

    def notify(self, method: str, params: Any | None = None) -> None:
        if self._sdk is None:
            raise CodexAppServerError("SDK app-server is not running")
        self._sdk.notify(method, params)

    def _start_reader(self, turn_id: str | None) -> None:
        sdk = self._sdk
        if turn_id is not None:
            if turn_id in self._turn_readers:
                return
            self._turn_readers.add(turn_id)

        def pump():
            try:
                while not self._stopping.is_set():
                    notification = (sdk.next_turn_notification(turn_id) if turn_id is not None
                                    else sdk.next_notification())
                    payload = notification.payload
                    params = payload.params if hasattr(payload, "params") else _wire_result(payload)
                    self._events.put({"method": notification.method, "params": params})
                    if turn_id is not None and notification.method == "turn/completed":
                        return
            except BaseException as exc:
                if not self._stopping.is_set():
                    self._events.put({"method": "client/disconnected", "params": {"reason": type(exc).__name__}})
            finally:
                if turn_id is not None:
                    sdk.unregister_turn_notifications(turn_id)

        reader = threading.Thread(target=pump, daemon=True)
        self._notification_readers.append(reader)
        reader.start()


def create_runtime_client(backend: str | None = None, *, codex_bin: str = "codex") -> CodexAppServerClient:
    """Choose a transport explicitly; unavailable SDKs never silently fall back."""
    backend = backend or os.environ.get("MYCODEX_RUNTIME_BACKEND", "app-server")
    if backend == "app-server":
        return CodexAppServerClient(codex_bin)
    if backend == "sdk":
        return SdkAppServerClient(codex_bin)
    raise ValueError("MYCODEX_RUNTIME_BACKEND must be app-server or sdk")
