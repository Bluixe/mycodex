from __future__ import annotations

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from mycodex.db import Store
from mycodex.web import create_app


class _CachedInsights:
    """DTO fixture only; the refresh route uses the real durable queue."""
    def __init__(self):
        self.calls = []
        self.reports = [{"project_name": "demo", "report_id": 1, "version": 2,
            "objective": "完成有据可查的研究", "summary": "<script>alert('report')</script>",
            "stage": "验证阶段", "progress": ["已整理接口"], "blockers": ["尚缺完整日志"],
            "next_steps": ["核验结果"], "assessment": "助手自报结果仍需核对产物",
            "citations": [17], "facts": [{"text": "用户希望核验", "kind": "user_stated", "citations": [17]},
                                         {"text": "助手报告已完成", "kind": "assistant_reported", "citations": [17]},
                                         {"text": "阶段推断", "kind": "inferred", "citations": [17]}],
            "latest_message_at": "2026-10-23T02:00:00Z", "generated_at": "2026-10-23T01:00:00Z",
            "coverage": {"visible_message_count": 3, "gaps": ["只覆盖部分对话"]},
            "model_status": "completed", "mode": "model", "stale": True, "last_attempt": None},
            {"project_name": "other", "report_id": None, "version": None, "objective": "",
             "summary": "", "stage": "", "progress": [], "blockers": [], "next_steps": [],
             "assessment": "", "citations": [], "facts": [], "latest_message_at": None,
             "coverage": {}, "generated_at": None, "model_status": "not_generated",
             "mode": "unavailable", "stale": False, "last_attempt": None}]
        self.items = [{"id": 1, "key": "writing", "category": "preference", "scope": "global",
            "project_name": None, "text": "</textarea><script>alert('profile')</script>",
            "status": "candidate", "citations": [17], "evidence_kind": "user_stated", "version": 1,
            "basis": "inferred", "quote": "<script>alert('quote')</script> 我希望证据可定位。",
            "created_at": "2026-10-23T01:00:00Z", "updated_at": "2026-10-23T01:00:00Z"},
            {"id": 2, "key": "forgotten", "category": "goal", "scope": "global", "project_name": None,
             "text": "old-forgotten-content-must-not-render", "status": "forgotten", "citations": [99],
             "evidence_kind": "user_stated", "version": 2, "created_at": None, "updated_at": None}]

    def overview(self):
        return {"projects": copy.deepcopy(self.reports), "profile": self.profile(), "generated_at": "2026-10-23T02:00:00Z"}

    def project_report(self, name):
        return copy.deepcopy(next(report for report in self.reports if report["project_name"] == name))

    def profile(self, include_inactive=False):
        return {"items": copy.deepcopy([item for item in self.items if include_inactive or item["status"] not in {"rejected", "forgotten"}]),
                "coverage": {"visible_message_count": 3}, "generated_at": None, "model_status": "not_generated", "mode": "extractive"}

    def update_profile(self, item_id, *, action, text):
        item = next((item for item in self.items if item["id"] == item_id), None)
        if item is None:
            raise ValueError("unknown profile item")
        self.calls.append((item_id, action, text))
        item["status"] = {"confirm": "confirmed", "reject": "rejected", "correct": "corrected", "forget": "forgotten"}[action]
        item["version"] += 1
        if action == "correct":
            item["text"] = text
        if action == "forget":
            item.update(text="", citations=[])
        return copy.deepcopy(item)

    def evidence(self, message_id):
        if message_id != 17:
            return None
        return {"id": 17, "text": "<script>alert('source')</script> [REDACTED_TOKEN]",
                "role": "user", "project_name": "demo", "recorded_at": None,
                "source_kind": "codex_rollout", "source_path": "local/file.jsonl",
                "locator": {"line": 4}, "content_hash": "abc", "truncated": True, "redacted": True}


class WebInsightTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "state.sqlite"
        with Store(self.db) as store:
            for name in ("demo", "other", "mycodex"):
                project_path = Path(self.temp.name) / name
                project_path.mkdir()
                store.add_project(name, str(project_path))
        self.service = _CachedInsights()
        factory = patch("mycodex.web._insights", return_value=self.service)
        factory.start()
        self.addCleanup(factory.stop)
        self.client = TestClient(create_app(self.db))
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)

    def test_report_and_home_show_cached_evidence_staleness_and_pending_state(self):
        response = self.client.get("/insights")
        self.assertEqual(response.status_code, 200)
        for text in ("项目洞察", "用户明确陈述", "助手报告", "分析推断", "待分析", "只覆盖部分对话", "当前 Codex 登录"):
            self.assertIn(text, response.text)
        self.assertIn('href="/conversations/messages/17"', response.text)
        self.assertIn("&lt;script&gt;", response.text)
        self.assertNotIn("<script>alert('report')", response.text)
        home = self.client.get("/")
        self.assertEqual(home.status_code, 200)
        self.assertIn("insights?project=demo", home.text)
        self.assertIn("需更新分析", home.text)
        self.assertIn("尚未生成有来源的项目综述", home.text)
        self.assertEqual(self.client.get("/api/jobs").json(), [])
        self.assertEqual(self.client.get("/api/insights?project=demo").json()["version"], 2)
        self.assertEqual(len(self.client.get("/api/insights").json()["projects"]), 2)

    def test_evidence_escapes_source_and_exposes_only_dto_with_unknown_time(self):
        response = self.client.get("/conversations/messages/17")
        self.assertEqual(response.status_code, 200)
        self.assertIn("&lt;script&gt;", response.text)
        self.assertNotIn("<script>alert('source')", response.text)
        self.assertIn("已脱敏", response.text)
        self.assertIn("展示内容已截断", response.text)
        self.assertIn("未知", response.text)
        evidence = self.client.get("/api/conversations/messages/17").json()
        self.assertIsNone(evidence["recorded_at"])
        self.assertTrue(evidence["redacted"])
        for path in ("/api/conversations/messages/999", "/conversations/messages/999",
                     "/api/insights?project=missing", "/insights?project=missing"):
            self.assertEqual(self.client.get(path).status_code, 404)

    def test_profile_edit_forms_escape_content_and_disable_automatic_refresh(self):
        response = self.client.get("/profile")
        self.assertEqual(response.status_code, 200)
        self.assertIn('data-refresh-interval="0"', response.text)
        self.assertIn("候选 · 待确认", response.text)
        self.assertIn("模型归纳，待确认", response.text)
        self.assertIn('<blockquote class="profile-quote">&lt;script&gt;', response.text)
        self.assertNotIn("<script>alert('quote')", response.text)
        self.assertIn("&lt;/textarea&gt;&lt;script&gt;", response.text)
        self.assertNotIn("</textarea><script>", response.text)
        self.assertIn('action="/profile/1/correct"', response.text)
        self.assertNotIn("old-forgotten-content", response.text)
        inactive = self.client.get("/profile?include_inactive=true")
        self.assertIn("已遗忘", inactive.text)
        self.assertNotIn("old-forgotten-content", inactive.text)
        self.assertNotIn("/conversations/messages/99", inactive.text)

    def test_profile_basis_explicit_and_legacy_unknown_remain_distinct(self):
        self.service.items[0]["basis"] = "explicit"
        self.assertIn("用户明确表达", self.client.get("/profile").text)
        self.service.items[0].pop("basis")
        self.service.items[0].pop("quote")
        legacy = self.client.get("/profile")
        self.assertIn("表达依据类型未知，待确认", legacy.text)
        self.assertIn("暂无逐字引文", legacy.text)
        self.assertNotIn("用户明确表达", legacy.text)
        self.assertIn('href="/conversations/messages/17"', legacy.text)

    def test_profile_confirm_correct_reject_and_forget_save_explicit_feedback(self):
        for action, payload, expected in (("confirm", {}, "confirmed"),
                                           ("correct", {"text": "偏好中文、简洁表达"}, "corrected")):
            result = self.client.post(f"/profile/1/{action}", data=payload, follow_redirects=False)
            self.assertEqual((result.status_code, result.headers["location"]), (303, "/profile"))
            self.assertEqual(self.client.get("/api/profile").json()["items"][0]["status"], expected)
        self.assertEqual(self.client.get("/api/profile").json()["items"][0]["text"], "偏好中文、简洁表达")
        self.client.post("/profile/1/reject")
        self.assertEqual(self.client.get("/api/profile").json()["items"], [])
        self.client.post("/profile/1/forget")
        item = self.client.get("/api/profile?include_inactive=true").json()["items"][0]
        self.assertEqual((item["status"], item["text"], item["citations"]), ("forgotten", "", []))

    def test_invalid_profile_updates_and_cross_origin_posts_do_not_mutate(self):
        for path, data, expected in (("/profile/999/confirm", {}, 404),
                                     ("/profile/1/delete", {}, 404),
                                     ("/profile/1/correct", {"text": " "}, 400),
                                     ("/profile/1/correct", {"text": "x" * 2001}, 400)):
            self.assertEqual(self.client.post(path, data=data).status_code, expected)
        for path in ("/profile/1/forget", "/insights/refresh"):
            self.assertEqual(self.client.post(path, headers={"Origin": "https://other.example"}).status_code, 403)
        self.assertEqual(self.service.calls, [])
        self.assertEqual(self.client.get("/api/jobs").json(), [])

    def test_refresh_is_durable_deduplicated_and_never_executes_a_model_inline(self):
        with patch("mycodex.insight_tasks.execute_insight_refresh", side_effect=AssertionError("HTTP must only enqueue")):
            for _ in range(2):
                response = self.client.post("/insights/refresh", follow_redirects=False)
                self.assertEqual((response.status_code, response.headers["location"]), (303, "/jobs"))
        jobs = self.client.get("/api/jobs").json()
        self.assertEqual(len(jobs), 1)
        self.assertEqual((jobs[0]["kind"], jobs[0]["status"]), ("insight_refresh", "queued"))
        self.assertEqual(jobs[0]["payload"]["project_names"], ["demo", "other"])
        selected = self.client.post("/insights/refresh", data={"project_names": ["mycodex", "demo"]}, follow_redirects=False)
        self.assertEqual(selected.status_code, 303)
        self.assertEqual(self.client.get("/api/jobs").json()[0]["payload"]["project_names"], ["demo", "mycodex"])
        self.assertEqual(self.client.post("/insights/refresh", data={"project_names": "missing"}).status_code, 404)


class RealInsightBackendWebSmokeTests(unittest.TestCase):
    def test_cached_archive_routes_read_real_backend_without_model_or_source_rescan(self):
        from mycodex.conversations import ConversationArchive
        from mycodex.insights import Insights

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project_path = root / "project"
            logs = root / "sessions"
            project_path.mkdir()
            logs.mkdir()
            db = root / "state.sqlite"
            source = logs / "thread.jsonl"
            source.write_text("\n".join(json.dumps(row) for row in [
                {"type": "session_meta", "payload": {"id": "thread", "cwd": str(project_path)}},
                {"type": "response_item", "timestamp": "2026-10-23T00:00:00Z", "payload": {
                    "type": "message", "role": "user", "content": [{"type": "input_text",
                    "text": "Please verify <script>bad()</script>; api_key=sample-secret-only"}]}},
            ]) + "\n", encoding="utf-8")
            with Store(db) as store:
                project = store.add_project("demo", str(project_path))
                store.upsert_session(project["id"], {"id": "thread", "cwd": str(project_path), "path": str(source)})
                store.commit()
                archive = ConversationArchive(store, source_roots=[logs])
                archive.sync("demo")
                message_id = archive.messages("demo")[0]["id"]
            with patch.object(Insights, "refresh", side_effect=AssertionError("GET must not call a model")), \
                    patch.object(ConversationArchive, "sync", side_effect=AssertionError("GET must use the cached archive")), \
                    TestClient(create_app(db)) as client:
                for page in ("/", "/insights", "/profile", "/api/insights", "/api/profile"):
                    self.assertEqual(client.get(page).status_code, 200, page)
                evidence = client.get(f"/api/conversations/messages/{message_id}")
                self.assertEqual(evidence.status_code, 200)
                self.assertIn("[REDACTED_CREDENTIAL]", evidence.json()["text"])
                self.assertNotIn("sample-secret-only", evidence.text)
                html = client.get(f"/conversations/messages/{message_id}")
                self.assertEqual(html.status_code, 200)
                self.assertIn("&lt;script&gt;", html.text)
                self.assertNotIn("<script>bad()", html.text)
                self.assertEqual(client.get("/api/jobs").json(), [])


if __name__ == "__main__":
    unittest.main()
