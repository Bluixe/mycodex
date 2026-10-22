from __future__ import annotations

import importlib
import threading
import time
import unittest
from unittest.mock import Mock, patch

from mycodex.codex_app import CodexAppServerClient, CodexAppServerError
from mycodex.runtime_adapters import (
    SdkAppServerClient, create_runtime_client, decline_sdk_request,
)


class RuntimeAdapterTests(unittest.TestCase):
    def test_sdk_never_uses_default_acceptance(self):
        for method in ("item/commandExecution/requestApproval", "item/fileChange/requestApproval"):
            self.assertEqual(decline_sdk_request(method), {"decision": "decline"})
        self.assertEqual(decline_sdk_request("item/permissions/requestApproval"),
                         {"permissions": {}, "scope": "turn"})
        self.assertEqual(decline_sdk_request("item/tool/requestUserInput"), {"answers": {}})
        self.assertEqual(decline_sdk_request("mcpServer/elicitation/request")["action"], "decline")

    def test_factory_requires_an_explicit_known_transport(self):
        self.assertIsInstance(create_runtime_client("app-server"), CodexAppServerClient)
        self.assertIsInstance(create_runtime_client("sdk"), SdkAppServerClient)
        with self.assertRaises(ValueError):
            create_runtime_client("unknown")

    def test_missing_optional_sdk_does_not_fall_back_silently(self):
        original = importlib.import_module
        with patch("mycodex.runtime_adapters.importlib.import_module",
                   side_effect=lambda name: (_ for _ in ()).throw(ImportError()) if name == "openai_codex" else original(name)):
            with self.assertRaisesRegex(CodexAppServerError, "optional openai-codex"):
                SdkAppServerClient().start()

    def test_sdk_rpc_timeout_closes_transport_and_unblocks_caller(self):
        client = SdkAppServerClient()
        stopped = threading.Event()
        sdk = Mock()
        sdk.close.side_effect = stopped.set
        client._sdk = sdk
        before = time.monotonic()
        with self.assertRaisesRegex(CodexAppServerError, "transport closed"):
            client._bounded_call("test", lambda: stopped.wait(5), timeout=0.01)
        self.assertLess(time.monotonic() - before, 1)
        sdk.close.assert_called_once()
        self.assertIsNone(client._sdk)

    def test_expired_operation_budget_does_not_send_sdk_turn_start(self):
        client = SdkAppServerClient()
        sdk = Mock()
        client._sdk = sdk
        client.operation_deadline = time.monotonic() - 1
        with self.assertRaisesRegex(CodexAppServerError, "deadline exceeded"):
            client.request("turn/start", {"threadId": "thread-1", "input": []})
        sdk.turn_start.assert_not_called()

    def test_sdk_initialization_failure_closes_transport(self):
        sdk = Mock()
        sdk.initialize.side_effect = RuntimeError("cannot initialize")
        factory = Mock(return_value=sdk)
        config = Mock()
        with patch("mycodex.runtime_adapters._sdk_components", return_value=(factory, config, object)):
            with self.assertRaisesRegex(CodexAppServerError, "cannot initialize"):
                SdkAppServerClient().start()
        self.assertIs(factory.call_args.kwargs["approval_handler"], decline_sdk_request)
        sdk.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
