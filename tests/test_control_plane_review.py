"""Independent regression checks for the integrated control-plane boundaries."""
from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from mycodex.db import Store
from mycodex.jobs import JobQueue, _notify_job
from mycodex.mcp_server import call_tool, serve
from mycodex.review_workflow import submit_review, synthesis_prompt


class ControlPlaneReviewTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / 'state.sqlite')
        self.project = self.store.add_project('demo', self.tmp.name, sandbox='workspace-write')
        self.queue = JobQueue(self.store)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_mcp_cannot_override_credential_and_notification_configuration(self):
        with self.assertRaises(ValueError):
            call_tool(self.store, 'mycodex_submit_job', {
                'project': 'demo', 'kind': 'sync_sessions',
                'payload': {'notification': {'enabled': True,
                    'token_file': str(Path(self.tmp.name) / 'unrelated-credential'),
                    'proxy_url': 'https://untrusted.example'}}}, allow_writes=True)
        self.assertEqual(self.queue.list(), [])

    def test_writers_in_distinct_threads_cannot_run_in_same_project_concurrently(self):
        first = self.queue.submit('demo', 'prompt', {'prompt': 'write one', 'thread_id': 'thread-one'})
        self.queue.submit('demo', 'prompt', {'prompt': 'write two', 'thread_id': 'thread-two'})
        self.assertEqual(self.queue.claim('one')['id'], first['id'])
        self.assertIsNone(self.queue.claim('two'))

    def test_nonreview_jobs_cannot_masquerade_as_independent_review_dependencies(self):
        job = self.queue.submit('demo', 'prompt', {'prompt': 'ordinary task', 'kind': 'manual',
                                                   'new_thread': True, 'sandbox': 'read-only'})
        with self.assertRaises(ValueError):
            self.queue.submit('demo', 'review_synthesis', {
                'prompt': 'summarize independent reviews', 'review_job_ids': [job['id']]},
                depends_on=[job['id']])

    def test_real_independent_reviews_wait_for_all_outputs_before_synthesis(self):
        workflow = submit_review(self.store, 'demo', 'Review source evidence and implementation.',
            roles=['evidence', 'implementation'], token_budget=1200)
        review_ids = [job['id'] for job in workflow['reviews']]
        first = self.queue.claim('one')
        second = self.queue.claim('two')
        self.assertEqual({first['id'], second['id']}, set(review_ids))
        self.assertIsNone(self.queue.claim('three'))
        self.queue.finish(first['id'], 'one', 'succeeded',
            result={'status': 'completed', 'thread_id': 'independent-one', 'output_text': 'Evidence observation'})
        self.assertIsNone(self.queue.claim('three'))
        self.queue.finish(second['id'], 'two', 'succeeded',
            result={'status': 'completed', 'thread_id': 'independent-two', 'output_text': 'Implementation observation'})
        synthesis = self.queue.claim('three')
        self.assertEqual(synthesis['id'], workflow['synthesis']['id'])
        text = synthesis_prompt(self.store, synthesis['payload'])
        self.assertIn('Evidence observation', text)
        self.assertIn('Implementation observation', text)
        self.assertEqual(sum(job['payload']['max_total_tokens'] for job in workflow['reviews'])
                         + synthesis['payload']['max_total_tokens'], workflow['token_budget'])

    def test_malformed_mcp_mutation_does_not_write_or_kill_server(self):
        messages = [
            {'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {}},
            {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call', 'params': {
                'name': 'mycodex_submit_job', 'arguments': {'project': 'demo', 'kind': 'sync_sessions',
                    'payload': {'sandbox': []}}}},
            {'jsonrpc': '2.0', 'id': 3, 'method': 'tools/call', 'params': {
                'name': 'mycodex_submit_job', 'arguments': {'project': 'demo', 'kind': 'sync_sessions',
                    'depends_on': [False]}}},
            {'jsonrpc': '2.0', 'id': 4, 'method': 'tools/call', 'params': {
                'name': 'mycodex_projects', 'arguments': {}}},
        ]
        output = io.StringIO()
        serve(self.store, allow_writes=True,
            stdin=io.StringIO('\n'.join(map(json.dumps, messages))), stdout=output)
        replies = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertTrue(replies[1]['result']['isError'])
        self.assertTrue(replies[2]['result']['isError'])
        self.assertFalse(replies[3]['result']['isError'])
        self.assertEqual(self.queue.list(), [])

    def test_configured_notification_matches_public_signature_and_records_outcome(self):
        job = self.queue.submit('demo', 'sync_sessions', {'notification': {'enabled': True,
            'token_env': 'TEST_TOKEN_ENV', 'proxy_url': None}}, source='daemon')
        leased = self.queue.claim('worker')
        self.queue.finish(leased['id'], 'worker', 'succeeded', result={'output_text': 'completed'})
        with patch('mycodex.telegram.notify_project_bindings', autospec=True,
                   return_value={'attempted': 0, 'sent': 0, 'skipped': 0, 'errors': []}) as notify:
            _notify_job(self.store, self.queue.get(job['id']))
        notify.assert_called_once_with(self.store, self.project['id'], 'completed',
            token_file=None, token_env='TEST_TOKEN_ENV', proxy_url=None)
        self.assertEqual(self.queue.events(job['id'])[0]['event_type'], 'notification_result')

    def test_telegram_submission_failure_releases_chat_busy_marker(self):
        from mycodex.telegram import TelegramBot, TelegramContext
        bot = TelegramBot(db_path=self.store.path, token='unused-test-token')
        bot.client = MagicMock()
        context = TelegramContext(chat_id='-100', message_thread_id='7', user_id='1',
            username='owner', chat_type='supergroup', text='hello', update_id=123)
        binding = {'project_id': self.project['id'], 'project_name': 'demo',
                   'project_path': self.tmp.name, 'thread_id': None}
        with patch('mycodex.jobs.JobQueue.submit', side_effect=ValueError('submission failed')):
            with self.assertRaisesRegex(ValueError, 'submission failed'):
                bot._start_prompt_task(context, binding, 'hello', kind='telegram')
        self.assertNotIn(('-100', '7'), bot._active_bindings)

    def test_replayed_telegram_update_cannot_move_to_a_changed_binding(self):
        from mycodex.telegram import TelegramBot, TelegramContext
        original = self.queue.submit('demo', 'prompt', {'prompt': 'original'},
            source='telegram', idempotency_key='telegram:-100:123')
        bot = TelegramBot(db_path=self.store.path, token='unused-test-token')
        bot.client = MagicMock()
        context = TelegramContext(chat_id='-100', message_thread_id='7', user_id='1',
            username='owner', chat_type='supergroup', text='original', update_id=123)
        changed_binding = {'project_name': 'a-new-binding-that-must-not-be-used'}
        with patch.object(bot, '_start_prompt_task_impl') as submit:
            bot._start_prompt_task(context, changed_binding, 'original', kind='telegram')
        submit.assert_not_called()
        self.assertEqual(len(self.queue.list()), 1)
        self.assertIn(f"#{original['id']}", bot.client.send_message.call_args.args[1])


if __name__ == '__main__':
    unittest.main()
