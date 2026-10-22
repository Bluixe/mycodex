"""Real optional SDK + scripted stdio peer; no model or account calls."""
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

from mycodex.codex_app import CodexAppServerError
from mycodex.runtime_adapters import SdkAppServerClient


SCRIPTED_PEER = r'''
import json, sys
mode = sys.argv[1]
turn = {"id": "turn-1", "items": [], "status": "inProgress"}
thread_id = "thread-1"
schema_present = False
def send(value):
    print(json.dumps(value), flush=True)
def event(method, **params):
    send({"method": method, "params": {"threadId": thread_id, "turnId": "turn-1", **params}})
def usage():
    values = {"cachedInputTokens": 0, "inputTokens": 20, "outputTokens": 5,
              "reasoningOutputTokens": 0, "totalTokens": 25}
    event("thread/tokenUsage/updated", tokenUsage={"last": values,
         "total": {**values, "totalTokens": 1025, "inputTokens": 1020}})
def finish(status="completed", **answer):
    event("item/completed", item={"id": "answer", "type": "agentMessage", "phase": "final_answer",
          "text": json.dumps(answer)})
    event("turn/completed", turn={**turn, "status": status})
for line in sys.stdin:
    request = json.loads(line)
    method = request.get("method")
    if "id" not in request:
        continue
    result = {}
    if method == "initialize":
        result = {"userAgent": "codex/0.163.0", "serverInfo": {"name": "scripted-peer", "version": "0.163.0"}}
    elif method == "turn/start":
        schema_present = request["params"].get("outputSchema") is not None
        result = {"turn": turn}
    elif method == "turn/steer":
        result = {"turnId": "turn-1"}
    elif method == "slow":
        continue
    send({"id": request["id"], "result": result})
    if method == "turn/start":
        if mode == "eof":
            break
        if mode == "approval":
            send({"id": "approval-1", "method": "item/commandExecution/requestApproval",
                  "params": {"threadId": thread_id, "turnId": "turn-1"}})
            approved = json.loads(next(sys.stdin))
            finish(decision=approved.get("result", {}).get("decision"))
        elif mode in ("completed", "failed", "interrupted"):
            usage()
            event("item/completed", item={"id": "progress", "type": "agentMessage",
                  "phase": "commentary", "text": "checking"})
            finish(mode, schema_present=schema_present)
        elif mode == "budget":
            usage()
    elif method == "turn/interrupt":
        event("turn/completed", turn={**turn, "status": "interrupted"})
    elif method == "turn/steer":
        finish(steered=True)
'''


@unittest.skipUnless(importlib.util.find_spec("openai_codex"), "Optional SDK integration requires openai-codex==0.163.0")
class SdkTransportIntegrationTests(unittest.TestCase):
    def setUp(self):
        from openai_codex import CodexConfig
        self.temp = tempfile.TemporaryDirectory()
        peer = Path(self.temp.name) / "peer.py"
        peer.write_text(SCRIPTED_PEER, encoding="utf-8")
        self.clients = []
        self.config = lambda mode: CodexConfig(launch_args_override=(sys.executable, str(peer), mode))

    def tearDown(self):
        for client in self.clients:
            client.close()
        self.temp.cleanup()

    def client(self, mode):
        client = SdkAppServerClient(sdk_config=self.config(mode))
        self.clients.append(client)
        client.start()
        return client

    def start(self, client):
        return client.start_turn(thread_id="thread-1", prompt="scripted fixture only",
                                 output_schema={"type": "object", "properties": {}, "additionalProperties": False})

    def wait(self, client, **kwargs):
        return client.wait_for_turn(thread_id="thread-1", turn_id="turn-1", timeout=1, **kwargs)

    def test_native_schema_final_message_and_usage_survive_real_sdk_transport(self):
        client = self.client("completed")
        self.start(client)
        result = self.wait(client)
        self.assertTrue(result.completed)
        self.assertEqual(json.loads(result.output_text), {"schema_present": True})
        self.assertEqual(result.token_usage["turn"]["totalTokens"], 25)

    def test_failed_and_interrupted_sdk_turns_do_not_become_success(self):
        for status in ("failed", "interrupted"):
            with self.subTest(status=status):
                client = self.client(status)
                self.start(client)
                result = self.wait(client)
                self.assertFalse(result.completed)
                self.assertEqual(result.status, status)

    def test_sdk_control_steers_without_losing_turn_notifications(self):
        client = self.client("steer")
        self.start(client)
        client.steer_turn(thread_id="thread-1", turn_id="turn-1", prompt="change scope")
        self.assertEqual(json.loads(self.wait(client).output_text), {"steered": True})

    def test_sdk_budget_callback_interrupts_in_flight_turn(self):
        client = self.client("budget")
        self.start(client)
        result = self.wait(client, usage_callback=lambda usage: usage["turn"]["totalTokens"] < 20)
        self.assertEqual(result.status, "interrupted")
        self.assertTrue(result.raw["interruption_confirmed"])
        self.assertFalse(result.completed)

    def test_sdk_approval_default_is_overridden_with_decline(self):
        client = self.client("approval")
        self.start(client)
        result = self.wait(client)
        self.assertEqual(json.loads(result.output_text), {"decision": "decline"})

    def test_sdk_eof_and_rpc_deadline_fail_closed(self):
        client = self.client("eof")
        try:
            self.start(client)
        except CodexAppServerError:
            pass
        else:
            self.assertFalse(self.wait(client).completed)
        other = self.client("steer")
        with self.assertRaisesRegex(CodexAppServerError, "timed out"):
            other.request("slow", {}, timeout=0.02)


if __name__ == "__main__":
    unittest.main()
