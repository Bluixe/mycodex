import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
from mycodex.db import Store
from mycodex.jobs import JobQueue, run_worker
from mycodex.web import create_app


class JobControlTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'state.sqlite'
        self.store = Store(self.path)
        self.store.add_project('demo', self.tmp.name)
        self.queue = JobQueue(self.store)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_paused_task_survives_reopen_and_goal_edit_does_not_change_authority(self):
        job = self.queue.submit('demo','prompt',{'prompt':'first','max_total_tokens':700})
        paused = self.queue.pause(job['id'])
        self.assertIsNone(self.queue.claim('worker'))
        revised = self.queue.revise(job['id'],'revised',expected_revision=paused['revision'])
        self.assertEqual(revised['payload']['sandbox'], job['payload']['sandbox'])
        self.assertEqual(revised['payload']['max_total_tokens'],700)
        with self.assertRaisesRegex(ValueError,'reload'):
            self.queue.revise(job['id'],'lost edit',expected_revision=paused['revision'])
        with Store(self.path) as reopened:
            self.assertEqual(JobQueue(reopened).resume(job['id'])['status'],'queued')
        self.assertEqual(self.queue.claim('worker')['payload']['prompt'],'revised')

    def test_running_pause_resumes_same_thread_with_remaining_budget(self):
        job = self.queue.submit('demo','prompt',{'prompt':'task','max_total_tokens':500,'timeout':100})
        def interrupted(*args, **kwargs):
            kwargs['usage_callback']({'turn':{'totalTokens':200}})
            with Store(self.path) as other:
                JobQueue(other).pause(job['id'])
            self.assertTrue(kwargs['cancellation_callback']())
            return {'status':'interrupted','thread_id':'actual-thread','run_id':1,'interruption_confirmed':True}
        with patch('mycodex.service.send_prompt',side_effect=interrupted):
            run_worker(self.store,once=True,job_id=job['id'])
        self.assertEqual(self.queue.get(job['id'])['status'],'paused')
        resumed = self.queue.resume(job['id'])
        self.assertEqual(resumed['checkpoint']['consumed_tokens'],200)
        def complete(*args, **kwargs):
            self.assertEqual(kwargs['thread_id'],'actual-thread')
            self.assertFalse(kwargs['new_thread'])
            self.assertFalse(kwargs['usage_callback']({'turn':{'totalTokens':300}}))
            return {'status':'completed','thread_id':'actual-thread'}
        with patch('mycodex.service.send_prompt',side_effect=complete):
            run_worker(self.store,once=True,job_id=job['id'])
        self.assertEqual(self.queue.get(job['id'])['result']['budget_usage']['total_tokens'],500)

    def test_cancel_wins_over_pause_and_blocked_work_cannot_resume(self):
        job = self.queue.submit('demo','prompt',{'prompt':'x'})
        self.queue.claim('worker')
        self.queue.start(job['id'],'worker')
        self.queue.pause(job['id'])
        self.queue.cancel(job['id'])
        self.queue.finish(job['id'],'worker','cancelled',result={'status':'interrupted'})
        self.assertEqual(self.queue.get(job['id'])['status'],'cancelled')
        with self.assertRaises(ValueError):
            self.queue.resume(job['id'])

    def test_completed_turn_over_budget_blocks_automatic_dependents(self):
        parent = self.queue.submit('demo','prompt',{'prompt':'x','max_total_tokens':100})
        child = self.queue.submit('demo','prompt',{'prompt':'next'},depends_on=[parent['id']])
        def complete(*args, **kwargs):
            self.assertFalse(kwargs['usage_callback']({'turn':{'totalTokens':200}}))
            return {'status':'completed'}
        with patch('mycodex.service.send_prompt',side_effect=complete):
            run_worker(self.store,once=True,job_id=parent['id'])
        actual = self.queue.get(parent['id'])
        self.assertEqual(actual['status'],'blocked')
        self.assertEqual(actual['result']['status'],'completed')
        self.queue.recover()
        self.assertEqual(self.queue.get(child['id'])['status'],'blocked')

    def test_process_exit_after_start_blocks_instead_of_repeating_side_effect(self):
        job = self.queue.submit('demo','prompt',{'prompt':'x'})
        marker = Path(self.tmp.name) / 'effect'
        code = """import os,sys
from mycodex.db import Store
from mycodex.jobs import JobQueue
from pathlib import Path
with Store(sys.argv[1]) as store:
 q=JobQueue(store); j=q.claim('crashed',lease_seconds=1); q.start(j['id'],'crashed')
 Path(sys.argv[2]).write_text('one effect')
 os._exit(17)
"""
        result = subprocess.run([sys.executable,'-c',code,str(self.path),str(marker)],capture_output=True,timeout=10)
        self.assertEqual(result.returncode,17,result.stderr)
        current = self.queue.get(job['id'])
        self.queue.recover(now=current['lease_expires_at']+1)
        self.assertEqual(self.queue.get(job['id'])['status'],'blocked')
        self.assertIsNone(self.queue.claim('replacement'))
        self.assertEqual(marker.read_text(),'one effect')
        with self.assertRaises(ValueError):
            self.queue.resume(job['id'])

    def test_web_controls_enforce_revisions_and_render_state(self):
        job = self.queue.submit('demo','prompt',{'prompt':'before'})
        with TestClient(create_app(self.path)) as client:
            self.assertEqual(client.post(f"/jobs/{job['id']}/pause").status_code,200)
            paused = self.queue.get(job['id'])
            response = client.post(f"/jobs/{job['id']}/revise",data={'prompt':'after','revision':paused['revision']})
            self.assertIn('after',response.text)
            self.assertEqual(client.post(f"/jobs/{job['id']}/revise",data={'prompt':'stale','revision':paused['revision']}).status_code,409)
            self.assertEqual(client.post(f"/jobs/{job['id']}/resume").status_code,200)
            self.assertEqual(client.get(f"/api/jobs/{job['id']}/trace").status_code,200)

    def test_cli_default_also_saves_a_durable_task(self):
        from mycodex.cli import build_parser
        args = build_parser().parse_args(['send','--project','demo','hello'])
        with patch('mycodex.service.send_prompt',return_value={'status':'completed'}), patch('builtins.print'):
            args.handler(self.store,args)
        self.assertEqual(self.queue.list()[0]['status'],'succeeded')
