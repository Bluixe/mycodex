from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mycodex.db import Store
from mycodex.experiment_ops import (
    RemoteExecResult,
    _contract_produces_wandb_run,
    _jsonable,
    _run_path_from_launch_or_contract,
    _remote_status_script,
    _ssh_args,
    _strip_password_prompts,
    evaluate_stop_conditions,
    launch_remote_job,
    parse_remote_ref,
    parse_wandb_run_path,
    poll_wandb_run,
    remote_job_status,
)


class ExperimentOpsTests(unittest.TestCase):
    def test_parse_remote_ref_with_port(self) -> None:
        target = parse_remote_ref("ma-user@example.com:30415:/home/ma-user/repo")
        self.assertEqual(target.login, "ma-user@example.com")
        self.assertEqual(target.port, 30415)
        self.assertEqual(target.path, "/home/ma-user/repo")
        self.assertEqual(target.ref, "ma-user@example.com:30415:/home/ma-user/repo")

    def test_parse_wandb_run_path(self) -> None:
        run_ref = parse_wandb_run_path("entity/project/run123")
        self.assertEqual(run_ref.entity, "entity")
        self.assertEqual(run_ref.project, "project")
        self.assertEqual(run_ref.run_id, "run123")

    def test_ssh_args_password_auth_disables_batch_mode(self) -> None:
        target = parse_remote_ref("root@example.com:2222:/repo")
        args = _ssh_args(target, None, batch_mode=False, password_auth=True)
        joined = " ".join(args)
        self.assertIn("BatchMode=no", joined)
        self.assertIn("PreferredAuthentications=password", joined)
        self.assertNotIn("BatchMode=yes", joined)
        self.assertEqual(args[-2:], ["-p", "2222"])

    def test_remote_status_tail_filters_progress_noise(self) -> None:
        script = _remote_status_script("123", "/tmp/run.log", 40)
        self.assertIn("tr '\\r' '\\n'", script)
        self.assertIn("/[0-9]+%\\|/ || /it\\/s/ { next }", script)
        self.assertIn("length($0) > 1000", script)

    def test_strip_password_prompt_output(self) -> None:
        output = "root@example.com's password: \r\nREMOTE_STATUS=running\n"
        self.assertEqual(_strip_password_prompts(output), "REMOTE_STATUS=running\n")
        self.assertEqual(_strip_password_prompts("password: REMOTE_STATUS=running\n"), "REMOTE_STATUS=running\n")

    def test_launch_remote_job_records_pid_and_log_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="ma-user@example.com:30415:/remote/repo",
                    status="running",
                )
                calls = []

                def runner(target, script, ssh_key, timeout):
                    calls.append((target, script, ssh_key, timeout))
                    return RemoteExecResult(
                        stdout=(
                            "REMOTE_COMMIT=abc123\n"
                            "REMOTE_PID=4242\n"
                            "REMOTE_LOG=/remote/repo/logs/run.log\n"
                            "REMOTE_PID_FILE=/remote/repo/logs/run.log.pid\n"
                        )
                    )

                result = launch_remote_job(
                    store,
                    "demo",
                    "self-iterate",
                    command="python train.py",
                    commit="abc123",
                    require_clean=False,
                    wandb_entity="entity",
                    wandb_project="project",
                    wandb_run_id="run123",
                    remote_runner=runner,
                )

                self.assertEqual(result["remote_pid"], "4242")
                self.assertIn("python train.py", calls[0][1])
                updated = store.get_experiment(project["id"], experiment["name"])
                self.assertEqual(updated["phase"], "experiment_running")
                self.assertEqual(updated["artifact_ref"], "/remote/repo/logs/run.log")
                events = store.list_experiment_events(experiment["id"], limit=5)
                self.assertEqual(events[0]["event_type"], "remote_job_started")

    def test_launch_remote_job_dry_run_reports_password_auth(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                store.add_project("demo", project_path)
                store.add_experiment(
                    1,
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="ma-user@example.com:30415:/remote/repo",
                    status="running",
                    config={"ssh_password_file": "/tmp/secret"},
                )

                result = launch_remote_job(
                    store,
                    "demo",
                    "self-iterate",
                    command="python train.py",
                    commit="abc123",
                    require_clean=False,
                    dry_run=True,
                )

                self.assertEqual(result["ssh_auth"], "password_file")

    def test_remote_status_uses_latest_remote_job_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="ma-user@example.com:/remote/repo",
                    status="running",
                )
                store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_job_started",
                    payload={"remote_pid": "99", "log_path": "/tmp/job.log"},
                    status="running",
                )

                def runner(target, script, ssh_key, timeout):
                    return RemoteExecResult(stdout="REMOTE_STATUS=running\n")

                result = remote_job_status(
                    store,
                    "demo",
                    "self-iterate",
                    remote_runner=runner,
                )
                self.assertEqual(result["status"], "running")
                updated = store.get_experiment(project["id"], experiment["name"])
                self.assertEqual(updated["phase"], "experiment_running")

    def test_poll_wandb_records_metric_and_stop_condition(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    status="running",
                )

                def fetcher(run_ref, metrics, history_limit):
                    return {
                        "run_path": run_ref.path,
                        "state": "running",
                        "latest_step": 10,
                        "latest_metrics": {"success_rate": 0.625},
                        "summary": {"success_rate": 0.625},
                        "history_tail": [],
                    }

                result = poll_wandb_run(
                    store,
                    "demo",
                    "self-iterate",
                    run_path="entity/project/run123",
                    metrics=["success_rate"],
                    stop_conditions=["success_rate>=0.5"],
                    wandb_fetcher=fetcher,
                )

                self.assertEqual(result["outcome"], "stop_condition_met")
                self.assertTrue(result["should_stop"])
                updated = store.get_experiment(project["id"], experiment["name"])
                self.assertEqual(updated["phase"], "waiting_for_decision")
                self.assertIn("Stop condition met", updated["latest_decision"])

    def test_evaluate_stop_conditions(self) -> None:
        matched = evaluate_stop_conditions(
            {"success_rate": 0.75, "loss": 1.2},
            ["success_rate>=0.7", "loss<1.0", "missing>=1"],
        )
        self.assertEqual(matched, ["success_rate>=0.7"])

    def test_jsonable_ignores_objects_with_broken_item_attr(self) -> None:
        class BrokenItemAttr:
            def __getattr__(self, name):
                if name == "item":
                    raise KeyError("item")
                raise AttributeError(name)

            def __str__(self):
                return "broken"

        self.assertEqual(_jsonable({"value": BrokenItemAttr()}), {"value": "broken"})

    def test_read_only_preflight_does_not_reuse_finished_wandb_run(self) -> None:
        contract = {
            "execution_class": "read-only-preflight",
            "wandb": {
                "entity": "entity",
                "project": "project",
                "run_id": "finished-run",
            },
        }
        launch = {
            "wandb_entity": "entity",
            "wandb_project": "project",
            "wandb_run_id": "finished-run",
        }

        self.assertFalse(_contract_produces_wandb_run(contract))
        self.assertIsNone(_run_path_from_launch_or_contract(launch, contract))

    def test_smoke_train_tracks_new_wandb_run(self) -> None:
        contract = {
            "execution_class": "smoke-train",
            "wandb": {
                "entity": "entity",
                "project": "project",
                "run_id": "run-2",
            },
        }
        launch = {
            "wandb_entity": "entity",
            "wandb_project": "project",
            "wandb_run_id": "run-2",
        }

        self.assertTrue(_contract_produces_wandb_run(contract))
        self.assertEqual(_run_path_from_launch_or_contract(launch, contract), "entity/project/run-2")


if __name__ == "__main__":
    unittest.main()
