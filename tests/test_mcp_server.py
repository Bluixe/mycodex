import io
import json
import tempfile
import unittest
from pathlib import Path

from mycodex.db import Store
from mycodex.mcp_server import call_tool, serve, tool_catalog


class MCPTests(unittest.TestCase):
    def test_read_only_catalog_and_explicit_write_authority(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "db.sqlite") as store:
            store.add_project("demo", tmp)
            names = [tool["name"] for tool in tool_catalog()]
            self.assertNotIn("mycodex_submit_job", names)
            with self.assertRaises(ValueError):
                call_tool(store, "mycodex_submit_job", {"project": "demo", "kind": "sync_sessions"})
            with self.assertRaises(ValueError):
                call_tool(store, "mycodex_submit_job", {"project": "demo", "kind": "prompt",
                          "payload": {"prompt": "x", "sandbox": "danger-full-access"}}, allow_writes=True)
            job = call_tool(store, "mycodex_submit_job", {"project": "demo", "kind": "sync_sessions"}, allow_writes=True)
            self.assertEqual(job["status"], "queued")

    def test_protocol_roundtrip_and_invalid_array_do_not_kill_server(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "db.sqlite") as store:
            store.add_project("demo", tmp)
            messages = [
                {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05"}},
                {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "mycodex_submit_job", "arguments": {"project": "demo", "kind": "sync_sessions", "depends_on": [{}]}}},
                {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "mycodex_projects", "arguments": {}}},
            ]
            output = io.StringIO()
            serve(store, allow_writes=True, stdin=io.StringIO("\n".join(map(json.dumps, messages))), stdout=output)
            replies = [json.loads(line) for line in output.getvalue().splitlines()]
            self.assertTrue(replies[1]["result"]["isError"])
            self.assertFalse(replies[2]["result"]["isError"])
            self.assertEqual(json.loads(replies[2]["result"]["content"][0]["text"])[0]["name"], "demo")
