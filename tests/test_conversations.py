from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from mycodex.conversations import CONTEXT_SEPARATOR, ConversationArchive
from mycodex.db import Store


def event(kind, payload, timestamp="2026-10-23T00:00:00Z"):
    return {"type": kind, "payload": payload, "timestamp": timestamp}


def message(role, text, **extra):
    return event("response_item", {"type": "message", "role": role,
        "content": [{"type": "input_text" if role == "user" else "output_text", "text": text}], **extra})


class ConversationArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.logs = self.root / "sessions"
        self.logs.mkdir()
        self.store = Store(self.root / "store.sqlite")
        self.addCleanup(self.store.close)
        self.project = self.store.add_project("demo", self.repo)
        self.archive = ConversationArchive(self.store, source_roots=[self.logs])

    def rollout(self, records, *, thread="thread-1", cwd=None, path=None, header_id=None):
        path = path or self.logs / f"{thread}.jsonl"
        header = event("session_meta", {"id": header_id or thread, "cwd": str(cwd or self.repo)})
        path.write_bytes(b"".join((json.dumps(obj,ensure_ascii=False)+"\n").encode() for obj in [header,*records]))
        self.store.upsert_session(self.project["id"], {"id":thread,"path":str(path),"cwd":str(self.repo)})
        self.store.commit()
        return path

    def test_roles_and_analysis_tools_do_not_become_human_facts(self):
        self.rollout([message("user","I prefer small tests"), message("assistant","User prefers huge runs"),
            message("assistant","private reasoning",channel="analysis"), message("assistant","summary thought",phase="summary"),
            message("developer","injected"), message("system","system"), message("tool","result"),
            event("response_item",{"type":"reasoning","summary":[{"text":"internal"}]}),
            event("response_item",{"type":"function_call_output","output":"secret"}),
            message("assistant","tool invocation",recipient="tools.exec")])
        receipt=self.archive.sync("demo")
        self.assertTrue(receipt["complete"])
        records=self.archive.messages("demo")
        self.assertEqual({r["text"]:r["role"] for r in records},
                         {"I prefer small tests":"user","User prefers huge runs":"assistant"})
        self.assertEqual(len(self.archive.messages("demo",roles=["user"])),1)

    def test_cross_format_duplicates_and_repeated_turns(self):
        self.rollout([event("event_msg",{"type":"task_started","turn_id":"one"}),
            event("event_msg",{"type":"user_message","message":"continue"}), message("user","continue"),
            event("event_msg",{"type":"agent_message","message":"done"}), message("assistant","done"),
            event("event_msg",{"type":"task_started","turn_id":"two"}),message("user","continue")])
        self.archive.sync("demo")
        records=self.archive.messages("demo")
        self.assertEqual(len(records),3)
        self.assertEqual(sum(r["text"]=="continue" for r in records),2)
        self.assertTrue(all(r["source_kind"]=="response_item" for r in records))

    def test_event_only_and_missing_turn_do_not_invent_turn_id(self):
        self.rollout([event("event_msg",{"type":"user_message","message":"hello"}),
                      message("user","hello"),event("event_msg",{"type":"agent_message","message":"hi"})])
        self.archive.sync("demo")
        self.assertEqual(len(self.archive.messages("demo")),2)
        self.assertTrue(all(r["turn_id"] is None for r in self.archive.messages("demo")))

    def test_injected_blocks_removed_but_human_text_after_blocks_retained(self):
        injected = "# AGENTS.md instructions for demo\n<INSTRUCTIONS>do x</INSTRUCTIONS>\n<environment_context><cwd>bad</cwd></environment_context>\nKeep my wording."
        item=message("user",injected)
        item["payload"]["content"].extend([
            {"type":"input_text","text":"## Memory\nUser always loves dangerous runs."},
            {"type":"input_text","text":"<in-app-browser-context source='ambient-ui-state'>open page</in-app-browser-context>"},
            {"type":"input_text","text":"<codex_internal_context source='goal'>auto continue</codex_internal_context>"},
            {"type":"input_text","text":"Please verify the result."},
        ])
        self.rollout([item,message("user","<environment_context>incomplete")])
        self.archive.sync("demo")
        records=self.archive.messages("demo")
        self.assertEqual(len(records),1)
        self.assertEqual(records[0]["text"],"Keep my wording.\n\n\n\nPlease verify the result.")

    def test_obvious_credentials_redacted_before_cache(self):
        text="api_key=supersecret123 sk-proj-"+"a"*30+" ghp_"+"b"*30+" https://me:password@example.test"
        self.rollout([message("user",text)])
        self.archive.sync("demo")
        record=self.archive.messages("demo")[0]
        self.assertNotIn("supersecret123",record["text"])
        self.assertNotIn("a"*30,record["text"])
        self.assertNotIn("me:password",record["text"])
        self.assertNotIn("supersecret123",self.store.conn.execute("SELECT text FROM conversation_messages").fetchone()[0])

    def test_question_reply_imports_only_human_answer_not_assistant_question(self):
        wrapper = '<send_user_message_question_reply>' + json.dumps([
            {"question":"You always prefer unsafe work, correct?","answer":"Use bounded tests."}
        ]) + '</send_user_message_question_reply>'
        self.rollout([message("user",wrapper)])
        self.archive.sync("demo")
        record=self.archive.messages("demo")[0]
        self.assertEqual(record["text"],"Use bounded tests.")
        self.assertNotIn("unsafe",record["text"])

    def test_question_answer_arrays_use_labels_without_assistant_question(self):
        wrapper='<send_user_message_question_reply>'+json.dumps([
            {"question":"assistant asks a question","answer":["first",{"label":"Option A","value":"internal-id"}]},
            {"question":"another assistant question","answer":{"label":"Selected label"}}
        ])+'</send_user_message_question_reply>'
        self.rollout([message("user",wrapper)])
        self.archive.sync("demo")
        self.assertEqual(self.archive.messages("demo")[0]["text"],"first\nOption A\nSelected label")

    def test_sync_idempotent_and_append_keeps_message_ids(self):
        path=self.rollout([message("user","first")])
        self.archive.sync("demo")
        first=self.archive.messages("demo")[0]
        self.archive.sync("demo")
        self.assertEqual(self.archive.messages("demo")[0]["id"],first["id"])
        with path.open("ab") as f:
            f.write((json.dumps(message("assistant","second"))+"\n").encode())
        self.assertEqual(self.archive.coverage("demo")["sessions"][0]["status"],"stale")
        self.archive.sync("demo")
        self.assertEqual(len(self.archive.messages("demo")),2)
        self.assertEqual(self.archive.get_message(first["id"])["text"],"first")
        self.rollout([message("user","edited")])
        self.archive.sync("demo")
        self.assertIsNone(self.archive.get_message(first["id"]))
        self.assertEqual([r["text"] for r in self.archive.messages("demo")],["edited"])

    def test_cwd_index_and_header_identity_mismatch_fail_closed(self):
        path=self.rollout([message("user","other project secret")],cwd=self.root)
        self.assertFalse(self.archive.sync("demo")["complete"])
        self.assertEqual(self.archive.messages("demo"),[])
        self.rollout([message("user","wrong id")],header_id="foreign")
        self.archive.sync("demo")
        self.assertEqual(self.archive.messages("demo"),[])
        self.rollout([message("user","good")])
        self.archive.sync("demo")
        record=self.archive.messages("demo")[0]
        self.store.conn.execute("UPDATE sessions SET cwd=?",(str(self.root),))
        self.store.commit()
        self.assertIsNone(self.archive.get_message(record["id"]))
        self.assertEqual(self.archive.messages("demo"),[])

    def test_source_traversal_and_outside_root_refused(self):
        outside=self.root/"outside.jsonl"
        self.rollout([message("user","private")],path=outside)
        receipt=self.archive.sync("demo")
        self.assertEqual(receipt["sessions"][0]["error"],"source_outside_codex_sessions")
        self.assertEqual(self.archive.messages("demo"),[])
        self.store.conn.execute("UPDATE sessions SET path=?",(str(self.logs/".."/"outside.jsonl"),))
        self.store.commit()
        self.assertEqual(self.archive.sync("demo")["sessions"][0]["error"],"invalid_source_path")

    def test_project_isolation_and_no_unregistered_rollouts(self):
        self.rollout([message("user","demo human")])
        second=self.store.add_project("second",self.root/"second")
        self.archive.sync("demo")
        self.assertEqual(self.archive.messages("second"),[])
        self.assertEqual(self.archive.coverage("second")["indexed_sessions"],0)
        self.assertEqual(len(self.archive.messages()),1)
        self.store.conn.execute("UPDATE sessions SET project_id=?",(second["id"],))
        self.store.commit()
        self.assertEqual(self.archive.messages("demo"),[])

    def test_segmented_reads_preserve_exact_byte_offsets_and_utf8(self):
        path=self.rollout([message("user","你好🙂"),message("assistant","回答"),message("user","next")])
        with patch("mycodex.conversations._CHUNK_BYTES",7):
            self.archive.sync("demo")
        records=self.archive.messages("demo")
        self.assertEqual(len(records),3)
        with path.open("rb") as f:
            for record in records:
                f.seek(record["locator"]["byte_offset"])
                self.assertEqual(json.loads(f.readline())["payload"]["content"][0]["text"],record["text"])

    def test_tail_partial_utf8_and_jsonl_boundary_are_not_decoded_as_messages(self):
        self.rollout([message("user","旧🙂"*1000),event("turn_context",{"cwd":str(self.repo),"turn_id":"recent"}),
                      message("user","latest one"),message("assistant","latest two")])
        receipt=self.archive.sync("demo",max_scan_bytes=700)
        self.assertFalse(receipt["complete"])
        self.assertTrue(receipt["sessions"][0]["leading_fragment"])
        self.assertLessEqual(receipt["sessions"][0]["bytes_scanned"],700)
        self.assertEqual({r["text"] for r in self.archive.messages("demo")},{"latest one","latest two"})

    def test_migrated_header_requires_explicit_matching_turn_context(self):
        self.rollout([message("user","old unrelated"),
            message("user",f"<environment_context><cwd>{self.repo}</cwd></environment_context>still unauthorized"),
            event("turn_context",{"cwd":str(self.repo),"turn_id":"current"}),message("user","current human"),
            event("turn_context",{"cwd":str(self.root),"turn_id":"foreign"}),message("user","foreign again")],cwd=self.root)
        receipt=self.archive.sync("demo")
        self.assertEqual([r["text"] for r in self.archive.messages("demo")],["current human"])
        source=receipt["sessions"][0]
        self.assertTrue(source["migrated_context"])
        self.assertEqual(source["verified_turn_contexts"],1)
        self.assertEqual(source["cwd_mismatch_records"],3)
        self.assertFalse(source["complete"])

    def test_tail_unknown_context_is_not_assumed_to_be_header_cwd(self):
        self.rollout([message("user","x"*3000),message("user","unattributed tail")])
        receipt=self.archive.sync("demo",max_scan_bytes=500)
        self.assertEqual(self.archive.messages("demo"),[])
        self.assertTrue(receipt["sessions"][0]["leading_fragment"])
        self.assertEqual(receipt["sessions"][0]["cwd_mismatch_records"],1)

    def test_tail_exact_line_boundary_does_not_drop_authorizing_context(self):
        path=self.rollout([message("user","x"*3000),
            event("turn_context",{"cwd":str(self.repo),"turn_id":"recent"}),message("user","latest")])
        lines=path.read_bytes().splitlines(keepends=True)
        # Header bytes + preceding newline byte + two exact final JSON lines.
        budget=len(lines[0])+1+len(lines[-2])+len(lines[-1])
        receipt=self.archive.sync("demo",max_scan_bytes=budget)
        self.assertEqual([r["text"] for r in self.archive.messages("demo")],["latest"])
        self.assertFalse(receipt["sessions"][0]["discarded_leading_fragment"])
        self.assertEqual(receipt["sessions"][0]["bytes_scanned"],budget)

    def test_huge_line_skipped_bounded_and_later_message_retained(self):
        self.rollout([message("user","x"*1800),message("user","last")])
        with patch("mycodex.conversations._MAX_LINE_BYTES",600),patch("mycodex.conversations._CHUNK_BYTES",80):
            receipt=self.archive.sync("demo")
        self.assertFalse(receipt["complete"])
        self.assertEqual(receipt["sessions"][0]["oversized_lines"],1)
        self.assertEqual([r["text"] for r in self.archive.messages("demo")],["last"])

    def test_invalid_and_incomplete_trailing_lines_not_complete(self):
        path=self.rollout([message("user","good")])
        with path.open("ab") as f:
            f.write(b'not json\n{"type":"response_item"')
        receipt=self.archive.sync("demo")
        self.assertFalse(receipt["complete"])
        self.assertTrue(receipt["sessions"][0]["trailing_fragment"])
        self.assertEqual(receipt["sessions"][0]["invalid_lines"],2)
        self.assertEqual(len(self.archive.messages("demo")),1)

    def test_message_and_text_caps_explicitly_report_partial_coverage(self):
        self.rollout([message("user","one"),message("user","two"),message("assistant","x"*100)])
        with patch("mycodex.conversations._MAX_TEXT_CHARS",20):
            receipt=self.archive.sync("demo",max_messages_per_session=2)
        self.assertFalse(receipt["complete"])
        self.assertTrue(receipt["sessions"][0]["message_limit_truncated"])
        self.assertTrue(any(r["truncated"] for r in self.archive.messages("demo")))
        self.assertEqual(len(self.archive.messages("demo")),2)

    def test_unknown_time_is_not_current_and_recent_context_respects_size(self):
        unknown=message("user","unknown date")
        unknown["timestamp"]="unknown"
        current=message("user","new request")
        current["timestamp"]=datetime.now(timezone.utc).isoformat()
        self.rollout([unknown,current])
        self.archive.sync("demo")
        packet=self.archive.project_context("demo",max_chars=24)
        self.assertEqual([r["text"] for r in packet["messages"]],["new request"])
        self.assertEqual(packet["coverage"]["unknown_time_messages"],1)
        self.assertEqual(packet["coverage"]["context_excluded_unknown_time"],1)
        tiny=self.archive.project_context("demo",max_chars=3)
        self.assertLessEqual(sum(len(r["text"]) for r in tiny["messages"]),3)
        self.assertEqual(tiny["coverage"]["context_truncated_messages"],1)

    def test_latest_long_message_keeps_prefix_tail_and_adjacent_human_request(self):
        human=message("user","Please verify this latest implementation.")
        human["timestamp"]=datetime.now(timezone.utc).isoformat()
        answer=message("assistant","CURRENT BEGIN "+"x"*15000+" CURRENT END")
        answer["timestamp"]=datetime.now(timezone.utc).isoformat()
        self.rollout([human,answer])
        self.archive.sync("demo")
        original=self.archive.messages("demo")[0]
        packet=self.archive.project_context("demo",max_chars=12000)
        selected=next(r for r in packet["messages"] if r["id"]==original["id"])
        self.assertTrue(selected["text"].startswith("CURRENT BEGIN"))
        self.assertTrue(selected["text"].endswith("CURRENT END"))
        self.assertTrue(selected["context_truncated"])
        self.assertEqual(len(selected["text"]),6000)
        self.assertEqual(selected["context_separator"],CONTEXT_SEPARATOR)
        self.assertEqual(selected["text"],CONTEXT_SEPARATOR.join(
            original["text"][span["start"]:span["end"]] for span in selected["context_spans"]))
        self.assertEqual(len(selected["context_spans"]),2)
        self.assertEqual(selected["context_spans"][0]["start"],0)
        self.assertEqual(selected["context_spans"][-1]["end"],len(original["text"]))
        self.assertTrue(any(r["role"]=="user" for r in packet["messages"]))
        self.assertEqual(packet["coverage"]["context_truncated_messages"],1)
        self.assertEqual(packet["coverage"]["latest_selected_at"],original["recorded_at"])
        self.assertEqual(self.archive.get_message(original["id"])["text"],original["text"])
        self.assertNotIn("context_truncated",self.archive.get_message(original["id"]))

    def test_very_small_context_budgets_never_exceed_limit(self):
        record=message("user","a long current message")
        record["timestamp"]=datetime.now(timezone.utc).isoformat()
        self.rollout([record])
        self.archive.sync("demo")
        for budget in range(12):
            with self.subTest(budget=budget):
                packet=self.archive.project_context("demo",max_chars=budget)
                self.assertLessEqual(sum(len(r["text"]) for r in packet["messages"]),budget)
                self.assertEqual(packet["coverage"]["latest_selected_at"] is None,budget==0)
                for item in packet["messages"]:
                    real=self.archive.get_message(item["id"])["text"]
                    spans=item["context_spans"]
                    self.assertEqual(item["context_separator"],CONTEXT_SEPARATOR)
                    self.assertEqual(item["text"],CONTEXT_SEPARATOR.join(real[s["start"]:s["end"]] for s in spans))
                    self.assertTrue(all(0<=s["start"]<s["end"]<=len(real) for s in spans))
                    self.assertTrue(all(left["end"]<=right["start"] for left,right in zip(spans,spans[1:])))

    def test_turn_cwd_change_excludes_foreign_messages(self):
        self.rollout([event("turn_context",{"cwd":str(self.root),"turn_id":"foreign"}),message("user","foreign"),
                      event("turn_context",{"cwd":str(self.repo),"turn_id":"same"}),message("user","local")])
        receipt=self.archive.sync("demo")
        self.assertEqual(receipt["sessions"][0]["cwd_mismatch_records"],1)
        self.assertFalse(receipt["complete"])
        self.assertEqual([r["text"] for r in self.archive.messages("demo")],["local"])

    def test_malformed_turn_context_cannot_inherit_previous_cwd_authority(self):
        self.rollout([message("user","good"),event("turn_context",{"turn_id":"unknown","cwd":None}),
                      message("user","unknown cwd")])
        receipt=self.archive.sync("demo")
        self.assertFalse(receipt["complete"])
        self.assertEqual([r["text"] for r in self.archive.messages("demo")],["good"])

    def test_initialization_is_idempotent_and_source_never_written(self):
        path=self.rollout([message("user","unchanged")])
        before=path.read_bytes(),path.stat().st_mtime_ns
        ConversationArchive(self.store,source_roots=[self.logs]).sync("demo")
        self.assertEqual((path.read_bytes(),path.stat().st_mtime_ns),before)
        self.assertEqual(self.archive.coverage("demo")["cached_messages"],1)

    def test_unknown_project_and_bad_limits_raise(self):
        with self.assertRaises(ValueError):self.archive.sync("unknown")
        with self.assertRaises(ValueError):self.archive.sync("demo",max_scan_bytes=0)
        with self.assertRaises(ValueError):self.archive.messages("demo",roles=["system"])
        with self.assertRaises(ValueError):self.archive.messages("demo",limit=-1)


if __name__ == "__main__":
    unittest.main()
