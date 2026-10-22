"""Experiment queue outcome regressions at the generic worker boundary."""
import unittest
from unittest.mock import patch

from mycodex.jobs import _execute_job


class ExperimentJobOutcomeTests(unittest.TestCase):
    def test_nested_protocol_statuses_preserve_failure_and_blocking(self):
        scenarios = [
            ({"task": {"status": "succeeded"}, "execution": {"executed": True}}, "completed"),
            ({"task": {"status": "failed"}, "execution": {"executed": False, "reason": "local_check_failed"}}, "failed"),
            ({"task": {"status": "failed"}, "executed": False, "reason": "invalid_contract_json"}, "failed"),
            ({"task": {"status": "blocked"}, "execution": {"executed": False, "reason": "contract_incomplete"}}, "blocked"),
        ]
        for entry, expected in scenarios:
            with self.subTest(expected=expected, entry=entry), patch(
                    "mycodex.experiment_ops.run_ready_experiment_tasks",
                    return_value={"executed_tasks": 1, "tasks": [entry]}):
                result = _execute_job(None, {"kind": "experiment_task", "payload": {"name": "demo"},
                                            "project_name": "project"}, lambda: False, lambda usage: True)
                self.assertEqual(result["status"], expected)

    def test_empty_protocol_queue_is_idle_and_cancelled_job_does_not_launch(self):
        job = {"kind": "experiment_task", "payload": {"name": "demo"}, "project_name": "project"}
        with patch("mycodex.experiment_ops.run_ready_experiment_tasks", return_value={"tasks": []}) as execute:
            self.assertEqual(_execute_job(None, job, lambda: False, lambda usage: True)["status"], "idle")
            execute.reset_mock()
            self.assertEqual(_execute_job(None, job, lambda: True, lambda usage: True)["status"], "cancelled")
            execute.assert_not_called()
