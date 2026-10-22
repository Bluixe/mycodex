import tempfile
import unittest
from pathlib import Path

from mycodex.db import Store
from mycodex.jobs import JobQueue
from mycodex.review_workflow import submit_review, workflow_report, record_review_outcome


class ReviewMetricsTests(unittest.TestCase):
    def test_routine_skips_calls_while_explicit_risk_selects_relevant_role(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp)/'db.sqlite') as store:
            store.add_project('demo',tmp)
            skip = submit_review(store,'demo','Summarize saved status.')
            self.assertEqual(skip['decision'],'skip')
            self.assertEqual(JobQueue(store).list(),[])
            workflow = submit_review(store,'demo','Compare conflicting passages.', risks=['evidence-conflict'])
            self.assertEqual(workflow['roles'],['evidence'])
            self.assertEqual(len(workflow['reviews']),1)
            report = workflow_report(store,workflow['workflow_id'])
            self.assertIsNone(report['observed_tokens'])
            self.assertIsNone(report['net_savings_tokens'])

    def test_only_observed_cost_and_verified_findings_are_reported(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp)/'db.sqlite') as store:
            store.add_project('demo',tmp)
            workflow = submit_review(store,'demo','Check a code change.',risks=['code-change'])
            queue = JobQueue(store)
            first = queue.claim('reviewer')
            with self.assertRaises(ValueError):
                record_review_outcome(store,workflow['workflow_id'],verifier='owner',notes='checked',findings=[{
                    'review_job_id':first['id'],'summary':'An error','disposition':'accepted','evidence':'test:failure'}])
            queue.finish(first['id'],'reviewer','succeeded',result={'output_text':'Concrete failure',
                'thread_id':'independent','budget_usage':{'total_tokens':123},'elapsed_seconds':2.1})
            second = queue.claim('synthesis')
            queue.finish(second['id'],'synthesis','succeeded',result={'budget_usage':{'total_tokens':77}})
            report = record_review_outcome(store,workflow['workflow_id'],verifier='owner',notes='Reproduced the failing test.',findings=[{
                'review_job_id':first['id'],'summary':'Reproduced failure','disposition':'accepted','evidence':'tests/example.py:repro'}])
            self.assertEqual(report['observed_tokens'],200)
            self.assertEqual(report['accepted_findings'],1)
            self.assertTrue(report['benefit_verified'])
            self.assertIsNone(report['net_savings_tokens'])
            with self.assertRaises(ValueError):
                record_review_outcome(store,workflow['workflow_id'],verifier='owner',notes='overwrite',findings=[])
