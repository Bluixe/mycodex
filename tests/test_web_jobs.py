import tempfile
import unittest
from pathlib import Path
from fastapi.testclient import TestClient
from mycodex.db import Store
from mycodex.web import create_app


class WebJobTests(unittest.TestCase):
    def test_browser_submission_is_durable_and_cancelable(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "state.sqlite"
            with Store(db) as store:
                store.add_project("demo", tmp)
            with TestClient(create_app(db)) as client:
                response = client.post("/projects/demo/send", data={"prompt": "hello"})
                self.assertEqual(response.status_code, 200)
                self.assertIn("Tasks and decisions", response.text)
                jobs = client.get("/api/jobs").json()
                self.assertEqual(jobs[0]["status"], "queued")
                self.assertEqual(jobs[0]["payload"]["prompt"], "hello")
                client.post(f"/jobs/{jobs[0]['id']}/cancel")
                self.assertEqual(client.get("/api/jobs").json()[0]["status"], "cancelled")
                self.assertEqual(client.post("/projects/demo/send", headers={"Origin": "https://example.org"}, data={"prompt": "bad"}).status_code, 403)

    def test_unknown_status_and_jobs_return_not_found(self):
        with tempfile.TemporaryDirectory() as tmp, TestClient(create_app(Path(tmp) / "state.sqlite")) as client:
            self.assertEqual(client.get("/api/jobs/123").status_code, 404)
            self.assertEqual(client.get("/api/projects/missing/decisions").status_code, 404)
