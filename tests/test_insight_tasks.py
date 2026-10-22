from __future__ import annotations

import tempfile
import threading
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from mycodex.db import Store
from mycodex.insight_tasks import submit_insight_refresh, validate_insight_scope
from mycodex.jobs import JobQueue, run_worker


class InsightTaskTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'state.sqlite'
        self.store = Store(self.path)
        self.store.add_project('mycodex', Path(self.temp.name)/'control')
        self.store.add_project('alpha', Path(self.temp.name)/'alpha')
        self.store.add_project('beta', Path(self.temp.name)/'beta')

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_default_scope_frozen_and_original_threads_never_resumed(self):
        self.store.update_default_thread(self.store.get_project('alpha')['id'], 'original-thread')
        job = submit_insight_refresh(self.store)
        self.assertEqual(job['payload']['project_names'], ['alpha', 'beta'])
        self.assertEqual(job['resource_key'], 'insights:local')
        self.assertEqual(job['access_mode'], 'write')
        self.assertTrue(job['payload']['new_thread'])
        self.assertFalse(job['payload']['update_default_thread'])
        self.assertEqual(job['payload']['sandbox'], 'read-only')
        self.assertEqual(job['payload']['notification'], {'enabled': False})

    def test_invalid_scopes_and_options_fail_before_submission(self):
        for scope in ([], ['missing'], 'alpha', [True], ['alpha']*21):
            with self.assertRaises(ValueError):
                validate_insight_scope(self.store, scope)
        for extra in ({'use_model':1}, {'force':'true'}, {'timeout':0}, {'max_total_tokens':True}):
            with self.assertRaises(ValueError):
                submit_insight_refresh(self.store, ['alpha'], **extra)
        with self.assertRaises(ValueError):
            JobQueue(self.store).submit('alpha', 'insight_refresh', {'project_names':['beta']})
        self.assertEqual(JobQueue(self.store).list(), [])

    def test_repeated_and_simultaneous_requests_deduplicate_durably(self):
        barrier = threading.Barrier(2)
        def submit(_):
            with Store(self.path) as store:
                barrier.wait(timeout=5)
                return submit_insight_refresh(store, ['alpha'])['id']
        with ThreadPoolExecutor(max_workers=2) as pool:
            ids = list(pool.map(submit, range(2)))
        self.assertEqual(ids[0], ids[1])
        self.assertEqual(len(JobQueue(self.store).list()), 1)

    def test_different_scopes_serialize_and_completed_refresh_can_recur(self):
        first = submit_insight_refresh(self.store, ['alpha'])
        second = submit_insight_refresh(self.store, ['beta'])
        queue = JobQueue(self.store)
        claimed = queue.claim('one')
        self.assertEqual(claimed['id'], first['id'])
        self.assertIsNone(queue.claim('two'))
        queue.finish(first['id'], 'one', 'succeeded')
        self.assertEqual(queue.claim('two')['id'], second['id'])
        self.assertNotEqual(submit_insight_refresh(self.store, ['alpha'])['id'], first['id'])

    def test_worker_propagates_cancellation_usage_and_partial_failure(self):
        seen = []
        class FakeInsights:
            def __init__(self, store):
                pass
            def refresh(self, names, **kwargs):
                seen.append(kwargs)
                self_case.assertFalse(kwargs['cancellation_callback']())
                self_case.assertTrue(kwargs['usage_callback']({'turn':{'totalTokens':10}}))
                return {'projects':[], 'errors':['beta: model failed'], 'model_runs':[]}
        self_case = self
        module = types.ModuleType('mycodex.insights')
        module.Insights = FakeInsights
        job = submit_insight_refresh(self.store, ['alpha'], max_total_tokens=20)
        with patch.dict('sys.modules', {'mycodex.insights':module}), \
                patch('mycodex.service.sync_sessions', return_value=[]):
            run_worker(self.store, once=True, job_id=job['id'])
        result = JobQueue(self.store).get(job['id'])
        self.assertEqual(result['status'], 'failed')
        self.assertIn('beta: model failed', result['error'])
        self.assertEqual(result['result']['budget_usage']['total_tokens'], 10)
        self.assertEqual(seen[0]['max_total_tokens'], 20)
        self.assertEqual(self.store.get_project('alpha')['default_thread_id'], None)

    def test_failed_session_discovery_prevents_new_inference(self):
        module = types.ModuleType('mycodex.insights')
        module.Insights = lambda store: self.fail('analysis must not start when discovery fails')
        job = submit_insight_refresh(self.store, ['alpha'])
        with patch.dict('sys.modules', {'mycodex.insights':module}), \
                patch('mycodex.service.sync_sessions', side_effect=RuntimeError('unavailable')):
            run_worker(self.store, once=True, job_id=job['id'])
        result = JobQueue(self.store).get(job['id'])
        self.assertEqual(result['status'], 'failed')
        self.assertIn('conversation index update failed', result['error'])
        self.assertEqual(result['result']['usage']['model_calls'], 0)
