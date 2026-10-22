"""Independent regressions for pause/review/trace boundaries found in review."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mycodex.db import Store
from mycodex.jobs import JobQueue, run_worker
from mycodex.review_workflow import record_review_outcome, submit_review, workflow_report
from mycodex.tracing import job_trace


class IterationReviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / "db.sqlite")
        self.project = self.store.add_project("demo", self.temp.name)
        self.queue = JobQueue(self.store)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_direct_synthesis_submission_cannot_replace_default_thread(self):
        workflow = submit_review(self.store, "demo", "review", roles=["evidence"], token_budget=1000)
        ids = [job["id"] for job in workflow["reviews"]]
        synthesis = self.queue.submit("demo", "review_synthesis", {
            "prompt": "synthesize", "review_job_ids": ids,
        }, depends_on=ids)
        self.assertIs(synthesis["payload"].get("update_default_thread", True), False)

    def test_empty_owner_assessment_does_not_claim_verified_benefit(self):
        workflow = submit_review(self.store, "demo", "review", roles=["implementation"], token_budget=1000)
        result = record_review_outcome(self.store, workflow["workflow_id"], verifier="owner",
                                       findings=[], notes="No actionable contribution was identified.")
        self.assertEqual(result["accepted_findings"], 0)
        self.assertFalse(result["benefit_verified"])

    def test_running_run_span_is_not_exported_as_a_terminal_error(self):
        job = self.queue.submit("demo", "prompt", {"prompt": "work"})
        self.queue.claim("owner", job_id=job["id"])
        self.queue.start(job["id"], "owner")
        run_id = self.store.add_run(self.project["id"], kind="queued", prompt="private input")
        self.store.conn.execute("UPDATE runs SET raw_json=? WHERE id=?",
                                (json.dumps({"trace_id": f"job:{job['id']}"}), run_id))
        self.store.commit()
        spans = job_trace(self.store, job["id"])["resourceSpans"][0]["scopeSpans"][0]["spans"]
        child = next(span for span in spans if span["name"] == "mycodex.codex.turn")
        self.assertEqual(child["status"]["code"], 0)
        self.assertNotIn("private input", json.dumps(spans))

    def test_unknown_prior_usage_stays_unknown_after_unbounded_resume(self):
        job = self.queue.submit("demo", "prompt", {"prompt": "work"})
        self.queue.claim("owner", job_id=job["id"])
        self.queue.start(job["id"], "owner")
        self.queue.pause(job["id"])
        self.queue.finish(job["id"], "owner", "cancelled", result={
            "status": "interrupted", "interruption_confirmed": True, "thread_id": "same-thread",
            "elapsed_seconds": 1, "budget_usage": {"total_tokens": None},
        })
        self.queue.resume(job["id"])

        def complete(*_args, **kwargs):
            kwargs["usage_callback"]({"turn": {"totalTokens": 7}})
            return {"status": "completed", "thread_id": "same-thread"}

        with patch("mycodex.service.send_prompt", side_effect=complete):
            run_worker(self.store, once=True, job_id=job["id"])
        result = self.queue.get(job["id"])["result"]
        self.assertIsNone(result["budget_usage"]["total_tokens"])
        self.assertEqual(result["budget_usage"]["attempt_tokens"], 7)

    def test_trace_does_not_drop_older_correlated_run_after_project_activity(self):
        job = self.queue.submit('demo', 'prompt', {'prompt': 'private source input'})
        associated = self.store.add_run(self.project['id'], kind='queued', prompt='private source input')
        self.store.finish_run(associated, status='completed', output_text='private output',
            raw_json={'trace_id': f"job:{job['id']}", 'token_usage': {'turn': {'totalTokens': 31}}})
        self.store.conn.execute('UPDATE runs SET started_at=1,completed_at=2 WHERE id=?', (associated,))
        self.store.conn.executemany('''INSERT INTO runs
            (project_id,kind,prompt,status,raw_json,started_at,completed_at)
            VALUES(?,?,?,?,?,?,?)''', [(self.project['id'], 'unrelated', 'secret unrelated input', 'completed', '{}', 100 + index, 101 + index)
                                      for index in range(501)])
        self.store.commit()
        trace = job_trace(self.store, job['id'])
        spans = trace['resourceSpans'][0]['scopeSpans'][0]['spans']
        run_ids = [attribute['value'].get('intValue') for span in spans
                   for attribute in span['attributes'] if attribute['key'] == 'mycodex.run.id']
        self.assertIn(str(associated), run_ids)
        self.assertNotIn('private source input', json.dumps(trace))
        self.assertNotIn('secret unrelated input', json.dumps(trace))

    def test_trace_event_limit_reports_actual_dropped_count(self):
        job = self.queue.submit('demo', 'prompt', {'prompt': 'private source input'})
        self.store.conn.executemany('INSERT INTO job_events(job_id,event_type,payload_json,created_at) VALUES(?,?,?,?)',
            [(job['id'], 'usage_observed', json.dumps({'private_detail': 'never exported'}), index) for index in range(1010)])
        self.store.commit()
        trace = job_trace(self.store, job['id'])
        root = trace['resourceSpans'][0]['scopeSpans'][0]['spans'][0]
        self.assertEqual(len(root['events']), 1000)
        self.assertEqual(root['droppedEventsCount'], 11)  # Includes the original submitted event.
        self.assertNotIn('never exported', json.dumps(trace))

    def test_rejected_findings_are_an_assessment_without_verified_benefit(self):
        workflow = submit_review(self.store, 'demo', 'review', roles=['implementation'])
        job = self.queue.claim('reviewer')
        self.queue.finish(job['id'], 'reviewer', 'succeeded',
            result={'output_text': 'A suspected problem', 'thread_id': 'independent-review'})
        report = record_review_outcome(self.store, workflow['workflow_id'], verifier='owner',
            findings=[{'review_job_id': job['id'], 'summary': 'Suspected regression',
                       'disposition': 'rejected', 'evidence': 'Reproduction shows expected behavior.'}],
            notes='The reviewer found no confirmed issue.')
        self.assertTrue(report['assessment_recorded'])
        self.assertFalse(report['benefit_verified'])
        self.assertEqual(report['accepted_findings'], 0)
        self.assertIsNone(report['observed_tokens'])
        self.assertIsNone(report['net_savings_tokens'])

    def test_setup_failure_reports_pending_cancellation_and_unknown_partial_cost(self):
        original = JobQueue.submit
        calls = []
        def fail_after_first(queue, *args, **kwargs):
            calls.append(1)
            if len(calls) > 1:
                raise ValueError('second review submission failed')
            job = original(queue, *args, **kwargs)
            queue.claim('concurrent-worker', job_id=job['id'])
            queue.start(job['id'], 'concurrent-worker')
            return job
        with patch.object(JobQueue, 'submit', new=fail_after_first):
            with self.assertRaisesRegex(ValueError, 'second review submission failed'):
                submit_review(self.store, 'demo', 'review', roles=['evidence', 'implementation'])
        record = self.store.conn.execute('SELECT id FROM review_workflows ORDER BY id DESC LIMIT 1').fetchone()
        report = workflow_report(self.store, record['id'])
        self.assertEqual(report['decision'], 'setup_failed')
        self.assertEqual(report['jobs'][0]['status'], 'running')
        self.assertIsNotNone(self.queue.get(report['jobs'][0]['id'])['cancel_requested_at'])
        self.assertIn('request', report['reason'].lower())
        self.assertIsNone(report['observed_tokens'])
        self.assertFalse(report['benefit_verified'])


if __name__ == "__main__":
    unittest.main()
