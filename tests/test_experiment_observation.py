import base64
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mycodex.db import Store
from mycodex.experiment_ops import (
    RemoteExecResult, _post_launch_status_check, execute_execution_contract,
    parse_remote_ref, remote_job_status,
)


def observation(supervisor='not_running', child='running'):
    receipt = {'execution_id': 'test-id', 'manifest_sha256': 'test-hash', 'status': 'running',
               'pid': 12, 'supervisor_pid': 34}
    encoded = base64.b64encode(json.dumps(receipt).encode()).decode()
    return RemoteExecResult(stdout=(f'REMOTE_STATUS=running\nREMOTE_RECEIPT={encoded}\n'
        f'REMOTE_SUPERVISOR_STATUS={supervisor}\nREMOTE_CHILD_STATUS={child}\n'))


class ExperimentOwnershipObservationTests(unittest.TestCase):
    def test_post_launch_child_alive_with_dead_supervisor_needs_reconciliation(self):
        result = _post_launch_status_check(parse_remote_ref('u@host:/repo'), '12', '/log',
            ssh_key=None, ssh_password_file=None, timeout=10, delay=0, lines=10,
            runner=lambda *args: observation(), receipt_path='/receipt.json',
            execution_id='test-id', manifest_sha256='test-hash')
        self.assertEqual(result['status'], 'needs_reconciliation')
        self.assertTrue(result['receipt_verified'])

    def test_status_requires_both_original_process_identities(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / 'db.sqlite') as store:
            project = store.add_project('demo', tmp)
            experiment = store.add_experiment(project['id'], name='eval', thread_id='thread', remote_ref='u@host:/repo')
            store.add_experiment_event(experiment['id'], event_type='remote_job_started', payload={
                'remote_pid': '12', 'log_path': '/log', 'receipt_path': '/receipt.json',
                'execution_id': 'test-id', 'manifest_sha256': 'test-hash'})
            for supervisor, child in [('not_running', 'running'), ('running', 'identity_changed'),
                                      ('identity_unverified', 'running'), ('running', 'running')]:
                calls = []
                def runner(*args):
                    calls.append(args)
                    return observation(supervisor, child)
                result = remote_job_status(store, 'demo', 'eval', remote_runner=runner)
                self.assertEqual(result['status'], 'running' if supervisor == child == 'running' else 'needs_reconciliation')
                self.assertEqual(len(calls), 1)  # Observation never restarts a supervisor.
                self.assertIn('REMOTE_SUPERVISOR_STATUS', calls[0][1])

    def test_contract_does_not_complete_when_remote_ownership_is_ambiguous(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / 'db.sqlite') as store:
            project = store.add_project('demo', tmp)
            store.add_experiment(project['id'], name='eval', thread_id='thread', remote_ref='u@host:/repo')
            contract = {'auto_execute': True, 'execution_class': 'read-only-preflight',
                        'patch': {'required': False}, 'remote': {'command': 'echo ready', 'timeout': 10, 'commit': 'abc'}}
            with patch('mycodex.experiment_ops.launch_remote_job', return_value={
                    'post_launch_check': {'status': 'needs_reconciliation'}}):
                result = execute_execution_contract(store, 'demo', 'eval', contract)
            self.assertFalse(result['executed'])
            self.assertEqual(result['reason'], 'contract_remote_execution_needs_reconciliation')


if __name__ == '__main__':
    unittest.main()
