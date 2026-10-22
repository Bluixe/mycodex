from __future__ import annotations

import io
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from mycodex.codex_app import (CodexAppServerClient, CodexAppServerError,
                              CodexRuntimeCompatibilityError, TurnResult, _resolve_codex_bin)
from mycodex.db import Store
from mycodex.service import send_prompt


def event(method: str, **params):
    return {"method": method, "params": {"threadId": "thread-1", "turnId": "turn-1", **params}}


def terminal(status="completed", **turn):
    return event("turn/completed", turn={"id": "turn-1", "status": status, **turn})


class CodexRuntimeTests(unittest.TestCase):
    def test_analysis_thread_config_overrides_are_explicit_and_do_not_change_defaults(self):
        client = CodexAppServerClient('unused-fixture-runtime')
        overrides = {'features.shell_tool': False, 'web_search': 'disabled'}
        with patch.object(client, 'request', return_value={'thread': {'id': 'analysis'}}) as request:
            client.start_or_resume_thread(thread_id=None, cwd='temporary-analysis', config_overrides=overrides)
            params = request.call_args.args[1]
            self.assertEqual(request.call_args.args[0], 'thread/start')
            self.assertEqual(params['config'], overrides)
            self.assertIsNot(params['config'], overrides)
            self.assertEqual(params['sandbox'], 'read-only')
            self.assertEqual(params['approvalPolicy'], 'never')
            client.start_or_resume_thread(thread_id=None, cwd='ordinary-project')
            self.assertNotIn('config', request.call_args.args[1])

    def test_explicit_binary_wins_over_environment_and_desktop(self):
        with patch.dict(os.environ, {"MYCODEX_CODEX_BIN": "configured", "CODEX_CLI_PATH": "desktop"}):
            self.assertEqual(_resolve_codex_bin("explicit"), "explicit")
            self.assertEqual(_resolve_codex_bin("codex"), "configured")

    def test_current_existing_desktop_binary_wins_over_stale_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            executable = Path(tmp) / "codex.exe"
            executable.write_bytes(b"test-placeholder-not-launched")
            with patch.dict(os.environ, {"MYCODEX_CODEX_BIN": "", "CODEX_CLI_PATH": str(executable)}), \
                    patch("mycodex.codex_app.os.access", return_value=True), \
                    patch("mycodex.codex_app.shutil.which", return_value="stale-path-codex"):
                self.assertEqual(_resolve_codex_bin("codex"), str(executable))

    def test_missing_or_non_executable_desktop_binary_falls_back_to_path(self):
        with patch.dict(os.environ, {"MYCODEX_CODEX_BIN": "", "CODEX_CLI_PATH": "missing.exe"}), \
                patch("mycodex.codex_app.shutil.which", return_value="path-codex"):
            self.assertEqual(_resolve_codex_bin("codex"), "path-codex")

    def client(self, *events):
        client = CodexAppServerClient("unused-test-binary")
        client.interrupt_turn = MagicMock(return_value={})
        client.interrupt_grace_seconds = 0.01
        for value in events:
            client._events.put(value)
        return client

    def wait(self, client, **kwargs):
        return client.wait_for_turn(thread_id="thread-1", turn_id="turn-1", timeout=0.02, **kwargs)

    def test_final_message_is_authoritative_instead_of_concatenated_deltas(self):
        client = self.client(
            event("item/agentMessage/delta", itemId="progress", delta="Investigating..."),
            event("item/completed", item={"id": "progress", "type": "agentMessage",
                                          "phase": "commentary", "text": "Investigating..."}),
            event("item/agentMessage/delta", itemId="answer", delta='{"auto_'),
            event("item/completed", item={"id": "answer", "type": "agentMessage",
                                          "phase": "final_answer", "text": '{"auto_execute": false}'}),
            terminal(),
        )
        result = self.wait(client)
        self.assertTrue(result.completed)
        self.assertEqual(result.output_text, '{"auto_execute": false}')

    def test_commentary_does_not_override_final_response(self):
        client = self.client(
            event("item/completed", item={"id": "answer", "type": "agentMessage",
                                          "phase": "final_answer", "text": "final"}),
            event("item/completed", item={"id": "note", "type": "agentMessage",
                                          "phase": "commentary", "text": "progress"}),
            terminal(),
        )
        self.assertEqual(self.wait(client).output_text, "final")

    def test_failed_interrupted_and_missing_terminal_status_fail_closed(self):
        for status in ("failed", "interrupted", "inProgress", None):
            with self.subTest(status=status):
                client = self.client(terminal(status, error={"message": "failure"}))
                result = self.wait(client)
                self.assertFalse(result.completed)
                self.assertIn(result.status, {"failed", "interrupted"})

    def test_embedded_final_item_is_used_without_reading_old_turn(self):
        client = self.client(terminal(items=[{
            "id": "answer", "type": "agentMessage", "phase": "final_answer", "text": "current",
        }]))
        client.read_latest_agent_message = MagicMock(return_value="old contract")
        self.assertEqual(self.wait(client).output_text, "current")
        client.read_latest_agent_message.assert_not_called()

    def test_only_commentary_does_not_become_contract_output(self):
        client = self.client(
            event("item/started", item={"id": "progress", "type": "agentMessage", "phase": "commentary"}),
            event("item/agentMessage/delta", itemId="progress", delta="not final"), terminal(),
        )
        self.assertEqual(self.wait(client).output_text, "")

    def test_eof_fails_immediately(self):
        client = self.client({"method": "client/disconnected", "params": {}})
        result = self.wait(client)
        self.assertFalse(result.completed)
        self.assertEqual(result.status, "failed")
        self.assertIn("disconnected", result.raw["stop_reason"])

    def test_reader_publishes_eof(self):
        client = self.client()
        client.proc = MagicMock(stdout=io.StringIO('{"method":"notification"}\n'))
        client._read_stdout()
        self.assertEqual(client._events.get_nowait()["method"], "notification")
        self.assertEqual(client._events.get_nowait()["method"], "client/disconnected")

    def test_malformed_transport_and_inconsistent_terminal_error_fail_closed(self):
        for value in ({"method": "client/parseError", "params": {"line": "not JSON"}},
                      terminal(error={"message": "failed despite completed status"})):
            with self.subTest(value=value):
                self.assertFalse(self.wait(self.client(value, terminal())).completed)

    def test_request_detects_eof_instead_of_timeout(self):
        client = self.client({"method": "client/disconnected", "params": {}})
        client._write = MagicMock()
        with self.assertRaisesRegex(CodexAppServerError, "disconnected"):
            client.request("thread/read", {}, timeout=1)

    def test_expired_operation_budget_never_writes_a_turn_start_request(self):
        client = self.client()
        client.operation_deadline = time.monotonic() - 1
        client._write = MagicMock()
        with self.assertRaisesRegex(CodexAppServerError, "deadline exceeded"):
            client.request("turn/start", {"threadId": "thread-1"})
        client._write.assert_not_called()

    def test_rpc_deadlines_clamp_setup_but_allow_bounded_cleanup(self):
        client = self.client()
        client.operation_deadline = time.monotonic() + 0.1
        self.assertLessEqual(client._rpc_timeout("thread/start", 90), 0.1)
        client.operation_deadline = time.monotonic() - 1
        self.assertEqual(client._rpc_timeout("turn/interrupt", 60), 5)
        self.assertEqual(client._rpc_timeout("thread/archive", 60), 5)

    def test_incompatible_runtime_config_error_has_guidance_without_config_contents(self):
        client = self.client({"id": 1, "error": {"message":
            'failed to load config.toml: unknown variant priority; sensitive-config-value'}})
        client._write = MagicMock()
        with self.assertRaises(CodexRuntimeCompatibilityError) as failure:
            client.request("thread/start", {})
        self.assertIn("MYCODEX_CODEX_BIN", str(failure.exception))
        self.assertNotIn("sensitive-config-value", str(failure.exception))

    def test_timeout_requests_interrupt_and_does_not_complete(self):
        client = self.client()
        result = self.wait(client)
        self.assertEqual(result.status, "needs_reconciliation")
        self.assertFalse(result.completed)
        self.assertFalse(result.raw["interruption_confirmed"])
        client.interrupt_turn.assert_called_once_with(thread_id="thread-1", turn_id="turn-1")

    def test_caller_cancellation_is_polled_during_idle(self):
        client = self.client()
        checks = iter([False, True])
        result = client.wait_for_turn(thread_id="thread-1", turn_id="turn-1", timeout=1,
                                      cancellation_callback=lambda: next(checks))
        self.assertEqual(result.status, "needs_reconciliation")
        client.interrupt_turn.assert_called_once()

    def test_usage_is_persisted_and_budget_callback_stops_turn(self):
        usage = {"last": {"totalTokens": 12}, "total": {"totalTokens": 200}}
        client = self.client(event("thread/tokenUsage/updated", tokenUsage=usage))
        client.interrupt_turn.side_effect = lambda **_: client._events.put(terminal("interrupted")) or {}
        budget = MagicMock(return_value=False)
        result = self.wait(client, usage_callback=budget)
        self.assertEqual(result.status, "interrupted")
        self.assertTrue(result.raw["interruption_confirmed"])
        self.assertEqual(result.token_usage["total"], usage["total"])
        self.assertEqual(result.token_usage["turn"], {"totalTokens": 12})
        budget.assert_called_once_with({**usage, "turn": {"totalTokens": 12}})
        client.interrupt_turn.assert_called_once()

    def test_interrupt_ack_requires_matching_terminal_notification(self):
        client = self.client()
        result = self.wait(client, cancellation_callback=lambda: True)
        self.assertEqual(result.status, "needs_reconciliation")
        self.assertFalse(result.raw["interruption_confirmed"])
        self.assertFalse(result.raw["terminal_confirmed"])

    def test_confirmed_interrupt_is_a_real_terminal_checkpoint(self):
        client = self.client()
        client.interrupt_turn.side_effect = lambda **_: client._events.put(terminal("interrupted")) or {}
        result = self.wait(client, cancellation_callback=lambda: True)
        self.assertEqual(result.status, "interrupted")
        self.assertTrue(result.raw["interruption_confirmed"])
        self.assertTrue(result.raw["terminal_confirmed"])

    def test_final_completion_wins_race_with_cancel_request(self):
        client = self.client(event("item/completed", item={"id": "answer", "type": "agentMessage",
                                  "phase": "final_answer", "text": "finished"}), terminal())
        result = self.wait(client, cancellation_callback=lambda: True)
        self.assertEqual(result.status, "completed")
        self.assertTrue(result.completed)
        self.assertEqual(result.output_text, "finished")
        self.assertFalse(result.raw["interruption_confirmed"])
        self.assertTrue(result.raw["terminal_confirmed"])
        self.assertEqual(result.raw["stop_reason"], "")

    def test_usage_turn_counters_cover_multiple_calls_on_resumed_thread(self):
        client = self.client(
            event("thread/tokenUsage/updated", tokenUsage={"total": {"totalTokens": 1010}, "last": {"totalTokens": 10}}),
            event("thread/tokenUsage/updated", tokenUsage={"total": {"totalTokens": 1035}, "last": {"totalTokens": 25}}),
            terminal(),
        )
        result = self.wait(client)
        self.assertEqual(result.token_usage["total"]["totalTokens"], 1035)
        self.assertEqual(result.token_usage["last"]["totalTokens"], 25)
        self.assertEqual(result.token_usage["turn"]["totalTokens"], 35)

    def test_other_thread_or_turn_cannot_complete_or_consume_budget(self):
        client = self.client(
            event("thread/tokenUsage/updated", threadId="other", tokenUsage={"total": {"totalTokens": 999}}),
            event("turn/completed", turn={"id": "other-turn", "status": "failed"}), terminal(),
        )
        budget = MagicMock(return_value=False)
        self.assertTrue(self.wait(client, usage_callback=budget).completed)
        budget.assert_not_called()

    def test_non_retryable_errors_fail_but_retryable_errors_can_recover(self):
        failed = self.wait(self.client(event("error", willRetry=False, error={"message": "bad"}), terminal()))
        recovered = self.wait(self.client(event("error", willRetry=True, error={"message": "retry"}), terminal()))
        self.assertEqual(failed.status, "failed")
        self.assertTrue(recovered.completed)

    def test_output_schema_and_control_requests_use_native_protocol(self):
        client = CodexAppServerClient("unused-test-binary")
        schema = {"type": "object", "properties": {"answer": {"type": "string"}}}
        client.request = MagicMock(return_value={"turn": {"id": "turn-1"}})
        client.start_turn(thread_id="thread-1", prompt="question", output_schema=schema)
        self.assertEqual(client.request.call_args.args[1]["outputSchema"], schema)
        client.interrupt_turn(thread_id="thread-1", turn_id="turn-1")
        self.assertEqual(client.request.call_args.args[0], "turn/interrupt")
        client.steer_turn(thread_id="thread-1", turn_id="turn-1", prompt="new focus")
        self.assertEqual(client.request.call_args.args[0], "turn/steer")
        self.assertEqual(client.request.call_args.args[1]["expectedTurnId"], "turn-1")

    def test_missing_turn_id_fails_before_wait(self):
        client = self.client()
        client.request = MagicMock(return_value={"turn": {}})
        with self.assertRaisesRegex(CodexAppServerError, "turn id"):
            client.start_turn(thread_id="thread-1", prompt="question")


class SendPromptRuntimeTests(unittest.TestCase):
    def run_send(self, tmp, result, **kwargs):
        store = Store(Path(tmp) / "db.sqlite")
        store.__enter__()
        root = Path(tmp) / "repo"
        root.mkdir()
        project = store.add_project("demo", root)
        client = MagicMock()
        client.start_or_resume_thread.return_value = {"id": "thread-1"}
        client.start_turn.return_value = ("turn-1", {"id": "turn-1"})
        client.wait_for_turn.return_value = result
        with patch("mycodex.service.CodexAppServerClient") as factory:
            factory.return_value.__enter__.return_value = client
            value = send_prompt(store, "demo", "prompt", sync_first=False, **kwargs)
        row = store.list_runs(project["id"])[0]
        row = dict(row)
        store.__exit__(None, None, None)
        return value, row, client

    def test_failed_partial_contract_is_diagnostic_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            partial = '{"auto_execute": true, "execution_class": "monitor-only", "reason": "run"}'
            result = TurnResult("thread-1", "turn-1", partial, False, {"errors": ["failed"]}, status="failed")
            value, row, _ = self.run_send(tmp, result)
            self.assertEqual(value["status"], "failed")
            self.assertEqual(value["output_text"], "")
            self.assertEqual(row["output_text"], "")
            self.assertEqual(json.loads(row["raw_json"])["diagnostic_output_text"], partial)

    def test_invalid_automatic_boolean_is_rejected_before_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = '{"auto_execute": "false", "execution_class": "monitor-only", "reason": "hold"}'
            result = TurnResult("thread-1", "turn-1", output, True, {}, status="completed")
            value, row, client = self.run_send(tmp, result, kind="experiment:1:control")
            self.assertEqual(value["status"], "failed")
            self.assertIn("auto_execute", value["contract_validation_error"])
            self.assertEqual(value["output_text"], "")
            self.assertIn("output_schema", client.start_turn.call_args.kwargs)

    def test_valid_legacy_contract_and_usage_remain_available(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = '```json\n{"auto_execute": false, "execution_class": "blocked", "reason": "missing input"}\n```'
            usage = {"total": {"totalTokens": 10}}
            result = TurnResult("thread-1", "turn-1", output, True, {}, status="completed", token_usage=usage)
            value, row, _ = self.run_send(tmp, result, kind="experiment:1:contract_repair", trace_id="job-1")
            self.assertEqual(value["status"], "completed")
            self.assertEqual(value["token_usage"], usage)
            self.assertEqual(value["trace_id"], "job-1")
            self.assertEqual(json.loads(row["raw_json"])["contract_schema"], "mycodex.experiment.v1")

    def test_cancellation_before_start_does_not_invoke_codex(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = TurnResult("thread-1", "turn-1", "unused", True, {})
            value, row, client = self.run_send(tmp, result, cancellation_callback=lambda: True)
            self.assertEqual(value["status"], "cancelled")
            client.start_or_resume_thread.assert_not_called()
            self.assertEqual(row["status"], "cancelled")

    def test_callbacks_are_forwarded_to_runtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = TurnResult("thread-1", "turn-1", "answer", True, {})
            cancel, budget = MagicMock(return_value=False), MagicMock(return_value=True)
            _, _, client = self.run_send(tmp, result, cancellation_callback=cancel, usage_callback=budget)
            options = client.wait_for_turn.call_args.kwargs
            self.assertIs(options["cancellation_callback"], cancel)
            self.assertIs(options["usage_callback"], budget)


if __name__ == "__main__":
    unittest.main()
