from __future__ import annotations

import hashlib
import tempfile
import json
import subprocess
import time
import unittest
from unittest.mock import patch
from pathlib import Path

from mycodex.db import Store
from mycodex.service import start_experiment_peer_review_turn
from mycodex.experiment_ops import (
    RemoteExecResult,
    _assess_peer_review_bypass,
    _contract_execution_failures,
    _contract_produces_wandb_run,
    _jsonable,
    _peer_review_blocked_since,
    _run_path_from_launch_or_contract,
    _remote_job_run_id,
    _remote_status_script,
    _ssh_args,
    _strip_password_prompts,
    classify_remote_experiment_snapshot,
    evaluate_stop_conditions,
    execute_execution_contract,
    extract_execution_contract,
    extract_peer_review_decision,
    launch_remote_job,
    parse_remote_ref,
    parse_wandb_run_path,
    poll_wandb_run,
    remote_job_status,
    record_execution_contract_task,
    recover_stale_experiment_tasks,
    run_ready_experiment_tasks,
    run_experiment_watchdog,
    supervise_experiment,
    sync_remote_experiment_state,
)


def _remote_snapshot(
    *,
    run_id: str = "train-blockzscore-vd005",
    eval_label: str = "eval-blockzscore-vd005",
    train_alive: bool = True,
    queue_alive: bool = True,
    train_done: bool = False,
    queue_done: bool = False,
    rows: int = 0,
    expected_rows: int = 8,
    transform: str = "block_zscore",
    command_transform: str | None = "block_zscore",
    dropout: str = "0.05",
    command_dropout: str | None = "0.05",
    log_tail: str = "",
    supervisor_alive: bool = True,
    watchdog_alive: bool = True,
    state_status: str = "running",
    host_boot_id: str = "boot-a",
    uptime_seconds: float = 3600.0,
) -> dict:
    now = time.time()
    command_parts = ["python", "train.py"]
    if command_transform is not None:
        command_parts += ["--auto_relabel_value_transform", command_transform]
    if command_dropout is not None:
        command_parts += ["--auto_relabel_value_dropout", command_dropout]
    command_text = " ".join(command_parts)
    return {
        "sync_status": "ok",
        "remote_now": now,
        "host": {
            "hostname": "remote-test",
            "boot_id": host_boot_id,
            "uptime_seconds": uptime_seconds,
        },
        "root": "/runs/controller",
        "base": "/runs",
        "state": {"status": state_status, "current": {"run_id": run_id, "eval_label": eval_label}},
        "current": {"run_id": run_id, "eval_label": eval_label},
        "run_id": run_id,
        "eval_label": eval_label,
        "paths": {
            "state": "/runs/controller/state.json",
            "events": "/runs/controller/events.jsonl",
            "run_root": f"/runs/{run_id}",
            "eval_root": f"/runs/{eval_label}",
            "queue_root": f"/runs/followup-{run_id}",
        },
        "files": {
            "state": {"path": "/runs/controller/state.json", "exists": True, "mtime": now, "size": 10},
            "events": {"path": "/runs/controller/events.jsonl", "exists": True, "mtime": now, "size": 20},
            "contract": {"path": "/runs/controller/contract.env", "exists": True, "mtime": now, "size": 10},
            "command": {"path": "/runs/controller/command.sh", "exists": True, "mtime": now, "size": 10},
            "summary": {"path": "/runs/eval/summary.csv", "exists": True, "mtime": now, "size": 10},
        },
        "pids": {
            "train": {"path": "/runs/train.pid", "pid": "111", "alive": train_alive},
            "eval_queue": {"path": "/runs/queue.pid", "pid": "222", "alive": queue_alive},
            "supervisor": {"path": "/runs/supervisor.pid", "pid": "333", "alive": supervisor_alive},
            "watchdog": {"path": "/runs/watchdog.pid", "pid": "444", "alive": watchdog_alive},
        },
        "logs": {
            "train": {"path": "/runs/train.log", "exists": True, "mtime": now, "tail": log_tail, "tail_sha256": "trainhash"},
            "eval_queue": {"path": "/runs/queue.log", "exists": True, "mtime": now, "tail": "", "tail_sha256": "queuehash"},
            "supervisor": {"path": "/runs/supervisor.log", "exists": True, "mtime": now, "tail": "", "tail_sha256": "superhash"},
            "watchdog": {"path": "/runs/watchdog.log", "exists": True, "mtime": now, "tail": "", "tail_sha256": "watchhash"},
        },
        "contract": {
            "path": "/runs/controller/contract.env",
            "env": {
                "AUTO_RELABEL_VALUE_TRANSFORM": transform,
                "AUTO_RELABEL_VALUE_DROPOUT": dropout,
                "EVAL_EPOCHS": "1 2 4 final",
                "EVAL_ENVS": "bossfight caveflyer",
            },
            "sha256": "contracthash",
        },
        "command": {"path": "/runs/controller/command.sh", "text": command_text, "sha256": "commandhash"},
        "summary": {
            "path": "/runs/eval/summary.csv",
            "rows": rows,
            "expected_rows": expected_rows,
            "complete": bool(expected_rows and rows >= expected_rows),
            "eval_epochs": ["1", "2", "4", "final"],
            "eval_envs": ["bossfight", "caveflyer"],
        },
        "remote_events_tail": [
            {
                "line_number": 1,
                "raw": "{\"event\":\"remote_job_started\",\"message\":\"started\"}",
                "sha256": "remoteeventhash1",
                "parsed": {"event": "remote_job_started", "message": "started"},
            }
        ],
        "train_done_marker": train_done,
        "queue_done_marker": queue_done,
        "gpu": ["0, RTX, 100, 80"],
        "processes": [],
    }


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

    def test_remote_job_run_id_uses_wandb_identity(self) -> None:
        self.assertEqual(
            _remote_job_run_id({"wandb_run_id": "train-env300"}),
            "train-env300",
        )

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
                    post_launch_check_delay=-1,
                    remote_runner=runner,
                )

                self.assertEqual(result["remote_pid"], "4242")
                self.assertIn("python train.py", calls[0][1])
                updated = store.get_experiment(project["id"], experiment["name"])
                self.assertEqual(updated["phase"], "experiment_running")
                self.assertEqual(updated["artifact_ref"], "/remote/repo/logs/run.log")
                events = store.list_experiment_events(experiment["id"], limit=5)
                self.assertEqual(events[0]["event_type"], "remote_job_started")

    def test_launch_remote_job_records_early_exit_after_post_launch_check(self) -> None:
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
                    calls.append(script)
                    if len(calls) == 1:
                        return RemoteExecResult(
                            stdout=(
                                "REMOTE_COMMIT=abc123\n"
                                "REMOTE_PID=4242\n"
                                "REMOTE_LOG=/remote/repo/logs/run.log\n"
                                "REMOTE_PID_FILE=/remote/repo/logs/run.log.pid\n"
                            )
                        )
                    return RemoteExecResult(
                        stdout=(
                            "REMOTE_STATUS=not_running\n"
                            "REMOTE_LOG_TAIL_START\n"
                            "bash: -c: line 44: unexpected EOF while looking for matching `)'\n"
                            "REMOTE_LOG_TAIL_END\n"
                        )
                    )

                result = launch_remote_job(
                    store,
                    "demo",
                    "self-iterate",
                    command="python train.py",
                    commit="abc123",
                    require_clean=False,
                    post_launch_check_delay=0,
                    remote_runner=runner,
                )

                self.assertEqual(result["post_launch_check"]["status"], "not_running")
                events = store.list_experiment_events(experiment["id"], limit=5)
                self.assertEqual(events[0]["event_type"], "remote_job_exited_early")
                self.assertEqual(events[0]["level"], "error")
                updated = store.get_experiment(project["id"], experiment["name"])
                self.assertEqual(updated["status"], "failed")

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
                for index in range(150):
                    store.add_experiment_event(
                        experiment["id"],
                        event_type="agent_message_delta",
                        message=f"noise {index}",
                        source="codex",
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

    def test_extract_peer_review_decision(self) -> None:
        text = """review complete

```json
{
  "decision": "approve",
  "reason": "The contract is complete.",
  "required_changes": [],
  "risk_notes": ["watch W&B"]
}
```
"""
        decision = extract_peer_review_decision(text)
        self.assertIsNotNone(decision)
        self.assertEqual(decision["decision"], "approve")

    def test_extract_execution_contract_reports_nested_contract(self) -> None:
        text = """analysis

```json
{
  "auto_execute": true,
  "execution_class": "monitor-only",
  "patch": {"required": false},
  "remote": {"command": "printf ok\\n", "timeout": 10},
  "artifacts": {"inputs": [], "outputs": [], "derived_fields": []}
}
```
"""
        contract = extract_execution_contract(text)
        self.assertIsNotNone(contract)
        self.assertEqual(contract["execution_class"], "monitor-only")
        self.assertEqual(contract["remote"]["command"], "printf ok\n")

    def test_monitor_only_contract_executes_remote_command(self) -> None:
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
                contract = {
                    "auto_execute": True,
                    "execution_class": "monitor-only",
                    "patch": {"required": False},
                    "remote": {"command": "printf POSTRUN_JSON=ok\\n", "timeout": 10},
                }

                with patch("mycodex.experiment_ops.run_remote_script") as run_remote:
                    run_remote.return_value = RemoteExecResult(stdout="POSTRUN_JSON=ok\n")
                    result = execute_execution_contract(
                        store,
                        "demo",
                        "self-iterate",
                        contract,
                    )

                self.assertTrue(result["executed"])
                self.assertEqual(result["remote_command"]["stdout"], "POSTRUN_JSON=ok\n")
                run_remote.assert_called_once()
                events = store.list_experiment_events(experiment["id"], limit=5)
                event_types = [event["event_type"] for event in events]
                self.assertIn("remote_monitor_command_completed", event_types)

    def test_record_execution_contract_task_marks_monitor_contract_ready(self) -> None:
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
                contract = {
                    "auto_execute": True,
                    "execution_class": "monitor-only",
                    "patch": {"required": False},
                    "remote": {"command": "printf POSTRUN_JSON=ok\\n", "timeout": 10},
                }

                first = record_execution_contract_task(
                    store,
                    "demo",
                    "self-iterate",
                    contract,
                    trigger="manual_start",
                    auto_execute_policy=True,
                    context_key="terminal-1",
                )
                second = record_execution_contract_task(
                    store,
                    "demo",
                    "self-iterate",
                    contract,
                    trigger="manual_start",
                    auto_execute_policy=True,
                    context_key="terminal-1",
                )

                self.assertEqual(first["task"]["status"], "ready")
                self.assertEqual(first["readiness"]["reason"], "contract_ready")
                self.assertEqual(first["task"]["id"], second["task"]["id"])
                tasks = store.list_experiment_tasks(experiment["id"], statuses=["ready"])
                self.assertEqual(len(tasks), 1)
                event_types = [event["event_type"] for event in store.list_experiment_events(experiment["id"], limit=5)]
                self.assertIn("experiment_task_recorded", event_types)

    def test_ready_experiment_task_executes_and_records_completion(self) -> None:
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
                contract = {
                    "auto_execute": True,
                    "execution_class": "monitor-only",
                    "patch": {"required": False},
                    "remote": {"command": "printf POSTRUN_JSON=ok\\n", "timeout": 10},
                }
                task_record = record_execution_contract_task(
                    store,
                    "demo",
                    "self-iterate",
                    contract,
                    trigger="manual_start",
                    auto_execute_policy=True,
                    context_key="terminal-1",
                )

                with patch("mycodex.experiment_ops.run_remote_script") as run_remote:
                    run_remote.return_value = RemoteExecResult(stdout="POSTRUN_JSON=ok\n")
                    result = run_ready_experiment_tasks(
                        store,
                        "demo",
                        "self-iterate",
                        owner="test-worker",
                        lease_seconds=60,
                    )

                self.assertEqual(result["executed_tasks"], 1)
                self.assertTrue(result["tasks"][0]["execution"]["executed"])
                task = store.get_experiment_task(experiment["id"], task_record["task"]["id"])
                self.assertIsNotNone(task)
                self.assertEqual(task["status"], "succeeded")
                self.assertEqual(task["attempts"], 1)
                run_remote.assert_called_once()
                event_types = [event["event_type"] for event in store.list_experiment_events(experiment["id"], limit=20)]
                self.assertIn("experiment_task_lease_acquired", event_types)
                self.assertIn("experiment_task_succeeded", event_types)
                self.assertIn("remote_monitor_command_completed", event_types)

    def test_recover_stale_leased_task_returns_it_to_ready(self) -> None:
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
                task_record = record_execution_contract_task(
                    store,
                    "demo",
                    "self-iterate",
                    {
                        "auto_execute": True,
                        "execution_class": "monitor-only",
                        "patch": {"required": False},
                        "remote": {"command": "printf ok\\n", "timeout": 10},
                    },
                    trigger="manual_start",
                    auto_execute_policy=True,
                    context_key="task-recover-lease",
                )
                lease_now = int(time.time())
                leased = store.lease_next_experiment_task(
                    experiment["id"],
                    owner="worker-before-restart",
                    lease_seconds=10,
                    now=lease_now,
                )
                self.assertIsNotNone(leased)

                result = recover_stale_experiment_tasks(
                    store,
                    project_name="demo",
                    experiment_name="self-iterate",
                    now=lease_now + 11,
                )

                self.assertEqual(result["recovered_tasks"], 1)
                task = store.get_experiment_task(experiment["id"], task_record["task"]["id"])
                self.assertIsNotNone(task)
                self.assertEqual(task["status"], "ready")
                self.assertEqual(task["lease_owner"], "")
                self.assertIsNone(task["lease_expires_at"])

    def test_recover_stale_running_task_without_remote_launch_returns_ready(self) -> None:
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
                task_record = record_execution_contract_task(
                    store,
                    "demo",
                    "self-iterate",
                    {
                        "auto_execute": True,
                        "execution_class": "monitor-only",
                        "patch": {"required": False},
                        "remote": {"command": "printf ok\\n", "timeout": 10},
                    },
                    trigger="manual_start",
                    auto_execute_policy=True,
                    context_key="task-recover-running",
                )
                store.lease_next_experiment_task(experiment["id"], owner="worker-before-restart", lease_seconds=60)
                store.update_experiment_task_status(experiment["id"], task_record["task"]["id"], "running")

                result = recover_stale_experiment_tasks(
                    store,
                    project_name="demo",
                    experiment_name="self-iterate",
                    running_timeout_seconds=3600,
                    now=int(time.time()) + 4000,
                )

                self.assertEqual(result["recovered_tasks"], 1)
                task = store.get_experiment_task(experiment["id"], task_record["task"]["id"])
                self.assertIsNotNone(task)
                self.assertEqual(task["status"], "ready")
                self.assertEqual(task["lease_owner"], "")

    def test_recover_stale_running_task_with_remote_launch_blocks_retry(self) -> None:
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
                task_record = record_execution_contract_task(
                    store,
                    "demo",
                    "self-iterate",
                    {"auto_execute": True, "execution_class": "eval-only", "patch": {"required": False}},
                    trigger="manual_start",
                    auto_execute_policy=True,
                    context_key="task-recover-launched",
                )
                task_id = task_record["task"]["id"]
                store.lease_next_experiment_task(experiment["id"], owner="worker-before-restart", lease_seconds=60)
                store.update_experiment_task_status(experiment["id"], task_id, "running")
                store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_job_started",
                    message="Remote eval started.",
                    payload={"protocol_task_id": str(task_id), "remote_pid": "4242"},
                    status="running",
                )

                result = recover_stale_experiment_tasks(
                    store,
                    project_name="demo",
                    experiment_name="self-iterate",
                    running_timeout_seconds=3600,
                    now=int(time.time()) + 4000,
                )

                self.assertEqual(result["recovered_tasks"], 1)
                task = store.get_experiment_task(experiment["id"], task_id)
                self.assertIsNotNone(task)
                self.assertEqual(task["status"], "blocked")
                events = store.list_experiment_events(experiment["id"], limit=10)
                event_types = [event["event_type"] for event in events]
                self.assertIn("experiment_task_recovery_blocked", event_types)

    def test_watchdog_executes_ready_protocol_task_without_remote_target(self) -> None:
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
                contract = {
                    "auto_execute": True,
                    "execution_class": "monitor-only",
                    "patch": {"required": False},
                    "remote": {"command": "printf POSTRUN_JSON=ok\\n", "timeout": 10},
                }
                record_execution_contract_task(
                    store,
                    "demo",
                    "self-iterate",
                    contract,
                    trigger="manual_start",
                    auto_execute_policy=True,
                    context_key="terminal-1",
                )

                with patch("mycodex.experiment_ops.run_remote_script") as run_remote:
                    run_remote.return_value = RemoteExecResult(stdout="POSTRUN_JSON=ok\n")
                    result = run_experiment_watchdog(
                        store,
                        "demo",
                        experiment_name="self-iterate",
                        remote_sync=False,
                        max_cycles=1,
                        control_timeout=60,
                    )

                self.assertEqual(result["experiments"][0]["action"], "protocol_task")
                self.assertEqual(result["experiments"][0]["task_result"]["executed_tasks"], 1)
                task = store.list_experiment_tasks(experiment["id"], limit=1)[0]
                self.assertEqual(task["status"], "succeeded")
                run_remote.assert_called_once()

    def test_watchdog_monitor_only_does_not_execute_ready_protocol_task(self) -> None:
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
                task_record = record_execution_contract_task(
                    store,
                    "demo",
                    "self-iterate",
                    {
                        "auto_execute": True,
                        "execution_class": "monitor-only",
                        "patch": {"required": False},
                        "remote": {"command": "printf ok\\n", "timeout": 10},
                    },
                    trigger="manual_start",
                    auto_execute_policy=True,
                    context_key="monitor-only-watchdog",
                )

                with patch("mycodex.experiment_ops.run_remote_script") as run_remote:
                    result = run_experiment_watchdog(
                        store,
                        "demo",
                        experiment_name="self-iterate",
                        remote_sync=False,
                        monitor_only=True,
                        max_cycles=1,
                        control_timeout=60,
                    )

                self.assertEqual(result["experiments"][0]["action"], "monitor_only")
                task = store.get_experiment_task(experiment["id"], task_record["task"]["id"])
                self.assertIsNotNone(task)
                self.assertEqual(task["status"], "ready")
                run_remote.assert_not_called()
                event_types = [event["event_type"] for event in store.list_experiment_events(experiment["id"], limit=10)]
                self.assertIn("watchdog_monitor_only_completed", event_types)

    def test_eval_only_explicit_commit_ignores_dirty_worktree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                subprocess.run(["git", "init"], cwd=project_path, check=True, capture_output=True)
                subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=project_path, check=True)
                subprocess.run(["git", "config", "user.name", "Test"], cwd=project_path, check=True)
                tracked = project_path / "tracked.txt"
                tracked.write_text("clean\n")
                subprocess.run(["git", "add", "tracked.txt"], cwd=project_path, check=True)
                subprocess.run(["git", "commit", "-m", "init"], cwd=project_path, check=True, capture_output=True)
                commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=project_path, text=True).strip()
                tracked.write_text("dirty\n")

                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="ma-user@example.com:30415:/remote/repo",
                    status="running",
                )
                contract = {
                    "auto_execute": True,
                    "execution_class": "eval-only",
                    "patch": {"required": False},
                    "remote": {"command": "printf eval\\n", "timeout": 10, "commit": commit},
                    "wandb": {"entity": "me", "project": "evals", "run_name": "heldout-eval"},
                    "metrics": ["eval/mean_return"],
                    "hard_stop": ["crash"],
                }

                with patch("mycodex.experiment_ops.run_remote_script") as run_remote:
                    run_remote.side_effect = [
                        RemoteExecResult(
                            stdout=(
                                f"REMOTE_COMMIT={commit}\n"
                                "REMOTE_PID=123\n"
                                "REMOTE_LOG=/remote/log\n"
                                "REMOTE_PID_FILE=/remote/log.pid\n"
                            )
                        ),
                        RemoteExecResult(stdout="REMOTE_STATUS=running\n"),
                    ]
                    result = execute_execution_contract(store, "demo", "self-iterate", contract)

                self.assertTrue(result["executed"])
                self.assertEqual(result["launch"]["commit"], commit)
                self.assertEqual(run_remote.call_count, 2)
                events = store.list_experiment_events(experiment["id"], limit=10)
                event_types = [event["event_type"] for event in events]
                self.assertIn("dirty_worktree_ignored_for_eval", event_types)
                self.assertIn("remote_job_started", event_types)

    def test_monitor_only_contract_prepares_remote_python_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="ma-user@example.com:30415:/remote/repo",
                    status="running",
                )
                contract = {
                    "auto_execute": True,
                    "execution_class": "monitor-only",
                    "patch": {"required": False},
                    "remote": {"command": "PYTHONDONTWRITEBYTECODE=1 python3 -B - <<'PY'\nprint('ok')\nPY", "timeout": 10},
                }

                with patch("mycodex.experiment_ops.run_remote_script") as run_remote:
                    run_remote.return_value = RemoteExecResult(stdout="ok\n")
                    result = execute_execution_contract(
                        store,
                        "demo",
                        "self-iterate",
                        contract,
                        remote_python="/root/miniconda3/envs/dpt/bin/python3",
                    )

                self.assertTrue(result["executed"])
                script = run_remote.call_args.args[1]
                self.assertIn("export MYCODEX_REMOTE_PYTHON=/root/miniconda3/envs/dpt/bin/python3", script)
                self.assertIn("export PATH=/root/miniconda3/envs/dpt/bin:$PATH", script)
                self.assertIn("PYTHONDONTWRITEBYTECODE=1 python3 -B -", script)

    def test_remote_command_sha256_local_check_is_handled_by_executor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="ma-user@example.com:30415:/remote/repo",
                    status="running",
                )
                remote_command = "printf POSTRUN_JSON=ok\\n"
                contract = {
                    "auto_execute": True,
                    "execution_class": "monitor-only",
                    "patch": {"required": False},
                    "local_checks": [
                        "outer mycodex must compute sha256 over exact remote.command bytes and require equality with remote.command_sha256 before launch"
                    ],
                    "remote": {
                        "command": remote_command,
                        "command_sha256": hashlib.sha256(remote_command.encode("utf-8")).hexdigest(),
                        "timeout": 10,
                    },
                }

                with patch("mycodex.experiment_ops.run_remote_script") as run_remote:
                    run_remote.return_value = RemoteExecResult(stdout="POSTRUN_JSON=ok\n")
                    result = execute_execution_contract(
                        store,
                        "demo",
                        "self-iterate",
                        contract,
                    )

                self.assertTrue(result["executed"])
                self.assertEqual(result["checks"][0]["kind"], "remote_command_sha256")
                run_remote.assert_called_once()

    def test_remote_command_sha256_local_check_blocks_on_mismatch(self) -> None:
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
                contract = {
                    "auto_execute": True,
                    "execution_class": "monitor-only",
                    "patch": {"required": False},
                    "local_checks": [
                        "outer mycodex must compute sha256 over exact remote.command bytes and require equality with remote.command_sha256 before launch"
                    ],
                    "remote": {
                        "command": "printf POSTRUN_JSON=ok\\n",
                        "command_sha256": "0" * 64,
                        "timeout": 10,
                    },
                }

                with patch("mycodex.experiment_ops.run_remote_script") as run_remote:
                    result = execute_execution_contract(
                        store,
                        "demo",
                        "self-iterate",
                        contract,
                    )

                self.assertFalse(result["executed"])
                self.assertEqual(result["reason"], "local_check_failed")
                run_remote.assert_not_called()
                events = store.list_experiment_events(experiment["id"], limit=5)
                self.assertEqual(events[0]["event_type"], "blocked")

    def test_assertion_style_local_checks_are_handled_by_executor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="ma-user@example.com:30415:/remote/repo",
                    status="running",
                )
                contract = {
                    "auto_execute": True,
                    "execution_class": "monitor-only",
                    "patch": {"required": False},
                    "local_checks": [
                        "printf abc == abc",
                        "printf '' is empty",
                    ],
                    "remote": {"command": "printf POSTRUN_JSON=ok\\n", "timeout": 10},
                }

                with patch("mycodex.experiment_ops.run_remote_script") as run_remote:
                    run_remote.return_value = RemoteExecResult(stdout="POSTRUN_JSON=ok\n")
                    result = execute_execution_contract(store, "demo", "self-iterate", contract)

                self.assertTrue(result["executed"])
                self.assertEqual(result["checks"][0]["kind"], "stdout_equals")
                self.assertEqual(result["checks"][1]["kind"], "stdout_empty")
                run_remote.assert_called_once()

    def test_procgen_wrapper_default_local_check_is_handled_by_executor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                wrapper = project_path / "experiments" / "run_procgen_multi_env_train.sh"
                wrapper.parent.mkdir(parents=True)
                wrapper.write_text(
                    'LR="${LR:-0.001}"\n'
                    'AUTO_RELABEL_VALUE_BLOCK_LEN="${AUTO_RELABEL_VALUE_BLOCK_LEN:-50}"\n'
                )
                project = store.add_project("demo", project_path)
                store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="ma-user@example.com:30415:/remote/repo",
                    status="running",
                )
                contract = {
                    "auto_execute": True,
                    "execution_class": "monitor-only",
                    "patch": {"required": False},
                    "local_checks": [
                        "verified wrapper defaults include LR=0.001 and AUTO_RELABEL_VALUE_BLOCK_LEN=50"
                    ],
                    "remote": {"command": "printf POSTRUN_JSON=ok\\n", "timeout": 10},
                }

                with patch("mycodex.experiment_ops.run_remote_script") as run_remote:
                    run_remote.return_value = RemoteExecResult(stdout="POSTRUN_JSON=ok\n")
                    result = execute_execution_contract(store, "demo", "self-iterate", contract)

                self.assertTrue(result["executed"])
                self.assertEqual(result["checks"][0]["kind"], "procgen_wrapper_defaults")
                run_remote.assert_called_once()

    def test_execute_contract_rejects_noop_monitor_only(self) -> None:
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
                contract = {
                    "auto_execute": True,
                    "execution_class": "monitor-only",
                    "patch": {"required": False},
                    "remote": {},
                }

                with patch("mycodex.experiment_ops.run_remote_script") as run_remote:
                    result = execute_execution_contract(
                        store,
                        "demo",
                        "self-iterate",
                        contract,
                    )

                self.assertFalse(result["executed"])
                self.assertEqual(result["reason"], "contract_incomplete")
                self.assertIn("missing_remote_command", result["failures"])
                run_remote.assert_not_called()
                events = store.list_experiment_events(experiment["id"], limit=5)
                self.assertEqual(events[0]["event_type"], "blocked")

    def test_patch_rolls_back_when_local_check_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                subprocess.run(["git", "init"], cwd=project_path, check=True, capture_output=True)
                target = project_path / "tracked.txt"
                target.write_text("old\n")
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="ma-user@example.com:30415:/remote/repo",
                    status="running",
                )
                diff = """diff --git a/tracked.txt b/tracked.txt
--- a/tracked.txt
+++ b/tracked.txt
@@ -1 +1 @@
-old
+new
"""
                contract = {
                    "auto_execute": True,
                    "execution_class": "diagnostic-patch",
                    "patch": {
                        "required": True,
                        "files": ["tracked.txt"],
                        "commit_message": "Test rollback",
                        "unified_diff": diff,
                    },
                    "local_checks": ["python3 -c 'import sys; sys.exit(1)'"],
                    "remote": {},
                }

                result = execute_execution_contract(store, "demo", "self-iterate", contract)

                self.assertFalse(result["executed"])
                self.assertEqual(result["reason"], "local_check_failed")
                self.assertTrue(result["rollback"]["rolled_back"])
                self.assertEqual(target.read_text(), "old\n")
                event_types = [event["event_type"] for event in store.list_experiment_events(experiment["id"], limit=10)]
                self.assertIn("code_change_rolled_back", event_types)

    def test_supervisor_executes_safe_fallback_after_one_peer_review_revision(self) -> None:
        contract_text = """```json
{
  "auto_execute": true,
  "execution_class": "monitor-only",
  "patch": {"required": false},
  "remote": {"command": "printf ok\\n", "timeout": 10}
}
```"""
        review_text = """```json
{
  "decision": "revise",
  "reason": "still incomplete",
  "required_changes": ["tighten provenance"],
  "risk_notes": []
}
```"""
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

                with (
                    patch("mycodex.experiment_ops.remote_job_status", return_value={"status": "not_running"}),
                    patch(
                        "mycodex.experiment_ops.start_experiment_control_turn",
                        return_value={"output_text": contract_text, "run_id": 1, "status": "completed"},
                    ) as control_turn,
                    patch(
                        "mycodex.experiment_ops.start_experiment_peer_review_turn",
                        return_value={"output_text": review_text, "thread_id": "review-thread"},
                    ) as peer_review,
                    patch(
                        "mycodex.experiment_ops.run_remote_script",
                        return_value=RemoteExecResult(stdout="ok\n"),
                    ) as remote_command,
                    patch("mycodex.experiment_ops.time.sleep"),
                ):
                    result = supervise_experiment(
                        store,
                        "demo",
                        "self-iterate",
                        remote_ref="ma-user@example.com:30415:/remote/repo",
                        remote_pid="4242",
                        remote_log_path="/remote/log",
                        interval=1,
                        max_cycles=2,
                    )

                self.assertTrue(result["execution"]["executed"])
                self.assertEqual(control_turn.call_count, 2)
                self.assertEqual(peer_review.call_count, 2)
                remote_command.assert_called_once()
                events = store.list_experiment_events(experiment["id"], limit=50)
                event_types = [event["event_type"] for event in events]
                self.assertIn("peer_review_soft_objections_recorded", event_types)
                self.assertEqual(event_types.count("supervisor_terminal_detected"), 1)
                self.assertNotIn("supervisor_retry_scheduled", event_types)
                self.assertNotIn("blocked", event_types)
                tasks = store.list_experiment_tasks(experiment["id"], limit=10)
                self.assertEqual(len(tasks), 1)
                self.assertEqual(tasks[0]["status"], "succeeded")
                self.assertEqual(tasks[0]["attempts"], 1)

    def test_supervisor_repairs_unparseable_control_contract(self) -> None:
        broken_contract_text = """```json
{
  "auto_execute": true,
  "execution_class": "monitor-only",
  "remote": {"command": "true"}
  this prose makes the JSON invalid
}
```"""
        repaired_contract_text = """```json
{
  "auto_execute": true,
  "execution_class": "monitor-only",
  "patch": {"required": false},
  "remote": {"command": "true", "timeout": 10},
  "metrics": [],
  "hard_stop": [],
  "soft_review": []
}
```"""
        review_text = """```json
{
  "decision": "approve",
  "reason": "complete after repair",
  "required_changes": [],
  "risk_notes": []
}
```"""
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

                with (
                    patch("mycodex.experiment_ops.remote_job_status", return_value={"status": "not_running"}),
                    patch(
                        "mycodex.experiment_ops.start_experiment_control_turn",
                        return_value={"output_text": broken_contract_text, "run_id": 1, "status": "completed"},
                    ) as control_turn,
                    patch(
                        "mycodex.experiment_ops.start_experiment_contract_repair_turn",
                        return_value={"output_text": repaired_contract_text, "run_id": 2, "status": "completed"},
                    ) as repair_turn,
                    patch(
                        "mycodex.experiment_ops.start_experiment_peer_review_turn",
                        return_value={"output_text": review_text, "thread_id": "review-thread"},
                    ) as peer_review,
                    patch("mycodex.experiment_ops.run_remote_script", return_value=RemoteExecResult(stdout="ok\n")),
                ):
                    result = supervise_experiment(
                        store,
                        "demo",
                        "self-iterate",
                        remote_ref="ma-user@example.com:30415:/remote/repo",
                        remote_pid="4242",
                        remote_log_path="/remote/log",
                        max_cycles=1,
                    )

                self.assertTrue(result["execution"]["executed"])
                control_turn.assert_called_once()
                repair_turn.assert_called_once()
                peer_review.assert_called_once()
                events = store.list_experiment_events(experiment["id"], limit=30)
                event_types = [event["event_type"] for event in events]
                self.assertIn("control_contract_parse_failed", event_types)
                self.assertIn("contract_repair_scheduled", event_types)
                self.assertIn("control_contract_parsed", event_types)
                self.assertIn("remote_monitor_command_completed", event_types)

    def test_watchdog_skips_fresh_activity(self) -> None:
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
                store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_job_started",
                    payload={"remote_pid": "4242", "log_path": "/remote/log"},
                    status="running",
                )
                calls = []

                def supervisor_runner(*args, **kwargs):
                    calls.append((args, kwargs))
                    return {}

                result = run_experiment_watchdog(
                    store,
                    "demo",
                    experiment_name="self-iterate",
                    stale_seconds=999,
                    supervisor_runner=supervisor_runner,
                )

                self.assertEqual(calls, [])
                self.assertEqual(result["experiments"][0]["action"], "skipped")
                self.assertEqual(result["experiments"][0]["reason"], "fresh")
                events = store.list_experiment_events(experiment["id"], limit=5)
                event_types = [event["event_type"] for event in events]
                self.assertIn("watchdog_heartbeat", event_types)
                self.assertIn("watchdog_skipped", event_types)

    def test_watchdog_skips_recent_active_experiment_codex_run(self) -> None:
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
                store.add_run(
                    project["id"],
                    kind=f"experiment:{experiment['id']}:control",
                    prompt="control",
                    thread_id="thread-1",
                )

                result = run_experiment_watchdog(
                    store,
                    "demo",
                    experiment_name="self-iterate",
                    stale_seconds=0,
                )

                self.assertEqual(result["experiments"][0]["action"], "skipped")
                self.assertEqual(result["experiments"][0]["reason"], "active_control_run")
                event_types = [
                    event["event_type"]
                    for event in store.list_experiment_events(experiment["id"], limit=10)
                ]
                self.assertIn("experiment_watchdog_skipped_active_control", event_types)
                self.assertNotIn("watchdog_supervise_started", event_types)

    def test_watchdog_marks_stale_experiment_codex_run_failed(self) -> None:
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
                run_id = store.add_run(
                    project["id"],
                    kind=f"experiment:{experiment['id']}:control",
                    prompt="control",
                    thread_id="thread-1",
                )
                store.conn.execute(
                    "UPDATE runs SET started_at = started_at - 120 WHERE id = ?",
                    (run_id,),
                )
                store.conn.commit()
                store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_job_started",
                    payload={
                        "remote_pid": "4242",
                        "log_path": "/remote/log",
                        "wandb_entity": "entity",
                        "wandb_project": "project",
                        "wandb_run_id": "run123",
                    },
                    status="running",
                )
                calls = []

                def supervisor_runner(*args, **kwargs):
                    calls.append((args, kwargs))
                    return {"terminal_reason": ""}

                result = run_experiment_watchdog(
                    store,
                    "demo",
                    experiment_name="self-iterate",
                    stale_seconds=0,
                    control_timeout=1,
                    supervisor_runner=supervisor_runner,
                )

                self.assertEqual(result["experiments"][0]["action"], "supervised")
                self.assertEqual(len(calls), 1)
                run = store.get_run(run_id)
                self.assertEqual(run["status"], "failed")
                self.assertIn("stale running experiment Codex run", run["error"])
                event_types = [
                    event["event_type"]
                    for event in store.list_experiment_events(experiment["id"], limit=10)
                ]
                self.assertIn("experiment_control_run_marked_stale", event_types)
                self.assertIn("watchdog_supervise_started", event_types)

    def test_watchdog_marks_stale_peer_review_before_control_timeout(self) -> None:
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
                run_id = store.add_run(
                    project["id"],
                    kind=f"experiment:{experiment['id']}:peer_review",
                    prompt="review",
                    thread_id="review-1",
                )
                store.conn.execute(
                    "UPDATE runs SET started_at = started_at - 1000 WHERE id = ?",
                    (run_id,),
                )
                store.conn.commit()
                store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_job_started",
                    payload={
                        "remote_pid": "4242",
                        "log_path": "/remote/log",
                        "wandb_entity": "entity",
                        "wandb_project": "project",
                        "wandb_run_id": "run123",
                    },
                    status="running",
                )

                result = run_experiment_watchdog(
                    store,
                    "demo",
                    experiment_name="self-iterate",
                    stale_seconds=0,
                    control_timeout=1800,
                    supervisor_runner=lambda *args, **kwargs: {"terminal_reason": ""},
                )

                self.assertEqual(result["experiments"][0]["action"], "supervised")
                run = store.get_run(run_id)
                self.assertEqual(run["status"], "failed")
                self.assertIn("960s", run["error"])

    def test_watchdog_runs_one_supervisor_cycle_when_stale(self) -> None:
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
                store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_job_started",
                    payload={
                        "remote_pid": "4242",
                        "log_path": "/remote/log",
                        "wandb_entity": "entity",
                        "wandb_project": "project",
                        "wandb_run_id": "run123",
                    },
                    status="running",
                )
                calls = []

                def supervisor_runner(*args, **kwargs):
                    calls.append((args, kwargs))
                    return {"terminal_reason": ""}

                result = run_experiment_watchdog(
                    store,
                    "demo",
                    experiment_name="self-iterate",
                    stale_seconds=0,
                    supervisor_interval=7,
                    supervisor_runner=supervisor_runner,
                )

                self.assertEqual(result["experiments"][0]["action"], "supervised")
                self.assertEqual(len(calls), 1)
                self.assertEqual(calls[0][1]["remote_pid"], "4242")
                self.assertEqual(calls[0][1]["remote_log_path"], "/remote/log")
                self.assertEqual(calls[0][1]["run_path"], "entity/project/run123")
                self.assertEqual(calls[0][1]["max_cycles"], 1)
                self.assertEqual(calls[0][1]["interval"], 7)
                events = store.list_experiment_events(experiment["id"], limit=5)
                event_types = [event["event_type"] for event in events]
                self.assertIn("watchdog_supervise_started", event_types)
                self.assertIn("watchdog_supervise_completed", event_types)

    def test_watchdog_keeps_remote_job_after_noisy_control_events(self) -> None:
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
                remote_event_id = store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_job_started",
                    payload={
                        "remote_pid": "4242",
                        "log_path": "/remote/log",
                        "wandb_entity": "entity",
                        "wandb_project": "project",
                        "wandb_run_id": "run123",
                    },
                    status="running",
                )
                for index in range(250):
                    store.add_experiment_event(
                        experiment["id"],
                        event_type="agent_message_delta",
                        message=f"noise {index}",
                        source="codex",
                    )
                calls = []

                def supervisor_runner(*args, **kwargs):
                    calls.append((args, kwargs))
                    return {"terminal_reason": ""}

                result = run_experiment_watchdog(
                    store,
                    "demo",
                    experiment_name="self-iterate",
                    stale_seconds=0,
                    supervisor_runner=supervisor_runner,
                )

                self.assertEqual(result["experiments"][0]["action"], "supervised")
                self.assertEqual(calls[0][1]["remote_pid"], "4242")
                self.assertEqual(calls[0][1]["remote_log_path"], "/remote/log")
                self.assertEqual(calls[0][1]["run_path"], "entity/project/run123")
                self.assertEqual(result["experiments"][0]["remote_job_event_id"], remote_event_id)

    def test_watchdog_ignores_stale_remote_snapshot_for_current_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": "/runs/controller"}},
                )
                remote_event_id = store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_job_started",
                    payload={
                        "run_id": "current-run",
                        "remote_pid": "4242",
                        "outer_log": "/runs/current-run.outer.log",
                    },
                    status="running",
                )
                stale_snapshot = _remote_snapshot(
                    run_id="old-run",
                    train_alive=False,
                    queue_alive=False,
                    supervisor_alive=False,
                    watchdog_alive=False,
                    rows=0,
                    expected_rows=8,
                )

                def runner(target, script, ssh_key, timeout):
                    return RemoteExecResult(stdout=json.dumps(stale_snapshot))

                with patch("mycodex.experiment_ops.start_experiment_control_turn") as control_mock:
                    result = run_experiment_watchdog(
                        store,
                        "demo",
                        experiment_name="self-iterate",
                        stale_seconds=900,
                        remote_runner=runner,
                    )

                self.assertEqual(result["experiments"][0]["action"], "skipped")
                self.assertEqual(result["experiments"][0]["reason"], "fresh")
                self.assertEqual(result["experiments"][0]["remote_job_event_id"], remote_event_id)
                self.assertEqual(result["experiments"][0]["remote_log_path"], "/runs/current-run.outer.log")
                control_mock.assert_not_called()
                events = store.list_experiment_events(experiment["id"], limit=20)
                event_types = [event["event_type"] for event in events]
                self.assertIn("stale_remote_snapshot_ignored", event_types)
                self.assertNotIn("remote_anomaly_control_started", event_types)

    def test_sync_remote_state_records_heartbeat_and_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": "/runs/controller"}},
                )

                def runner(target, script, ssh_key, timeout):
                    return RemoteExecResult(stdout=json.dumps(_remote_snapshot()))

                result = sync_remote_experiment_state(
                    store,
                    "demo",
                    "self-iterate",
                    remote_runner=runner,
                )

                self.assertEqual(result["sync_status"], "ok")
                self.assertEqual(result["remote_events_imported"], 1)
                self.assertEqual(result["control_anomalies"], [])
                event_types = [event["event_type"] for event in store.list_experiment_events(experiment["id"], limit=10)]
                self.assertIn("remote_heartbeat", event_types)
                self.assertIn("remote_event", event_types)
                self.assertNotIn("remote_anomaly", event_types)

    def test_sync_remote_state_detects_remote_boot_id_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": "/runs/controller"}},
                )
                snapshots = [
                    _remote_snapshot(host_boot_id="boot-a"),
                    _remote_snapshot(
                        train_alive=False,
                        queue_alive=False,
                        supervisor_alive=False,
                        watchdog_alive=False,
                        rows=0,
                        expected_rows=8,
                        host_boot_id="boot-b",
                        uptime_seconds=45,
                    ),
                ]

                def runner(target, script, ssh_key, timeout):
                    return RemoteExecResult(stdout=json.dumps(snapshots.pop(0)))

                first = sync_remote_experiment_state(
                    store,
                    "demo",
                    "self-iterate",
                    remote_runner=runner,
                )
                second = sync_remote_experiment_state(
                    store,
                    "demo",
                    "self-iterate",
                    remote_runner=runner,
                )

                self.assertEqual(first["control_anomalies"], [])
                codes = {item["code"] for item in second["control_anomalies"]}
                self.assertIn("remote_host_rebooted", codes)
                events = store.list_experiment_events(experiment["id"], limit=20)
                anomaly_events = [event for event in events if event["event_type"] == "remote_anomaly"]
                self.assertEqual(len(anomaly_events), 1)
                payload = json.loads(anomaly_events[0]["payload_json"])
                anomaly_codes = {item["code"] for item in payload["anomalies"]}
                self.assertIn("remote_host_rebooted", anomaly_codes)

    def test_recent_remote_reboot_without_prior_boot_is_control_anomaly(self) -> None:
        snapshot = _remote_snapshot(
            train_alive=False,
            queue_alive=False,
            supervisor_alive=False,
            watchdog_alive=False,
            rows=0,
            expected_rows=8,
            host_boot_id="fresh-boot",
            uptime_seconds=35,
        )

        anomalies = classify_remote_experiment_snapshot(snapshot, stale_seconds=900)
        recent = [item for item in anomalies if item["code"] == "remote_recent_reboot"]

        self.assertEqual(len(recent), 1)
        self.assertEqual(recent[0]["action"], "control")

    def test_completed_snapshot_boot_change_is_observe_only(self) -> None:
        snapshot = _remote_snapshot(
            train_alive=False,
            queue_alive=False,
            train_done=True,
            queue_done=True,
            rows=8,
            expected_rows=8,
            supervisor_alive=False,
            watchdog_alive=False,
            state_status="supervisor_done",
            host_boot_id="boot-b",
        )
        recent_events = [
            {
                "event_type": "remote_heartbeat",
                "payload_json": json.dumps(
                    {
                        "snapshot": {
                            "remote_ref": snapshot.get("remote_ref"),
                            "root": snapshot["root"],
                            "base": snapshot["base"],
                            "host": {"boot_id": "boot-a"},
                        }
                    }
                ),
            }
        ]

        anomalies = classify_remote_experiment_snapshot(
            snapshot,
            recent_events=recent_events,
            stale_seconds=900,
        )
        reboot = [item for item in anomalies if item["code"] == "remote_host_rebooted"]

        self.assertEqual(len(reboot), 1)
        self.assertEqual(reboot[0]["action"], "observe")

    def test_sync_remote_state_uses_configured_summary_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={
                        "remote_state": {
                            "root": "/runs/controller",
                            "eval_epochs": ["final"],
                            "eval_envs": ["coinrun", "starpilot"],
                        }
                    },
                )
                scripts = []

                def runner(target, script, ssh_key, timeout):
                    scripts.append(script)
                    return RemoteExecResult(stdout=json.dumps(_remote_snapshot()))

                sync_remote_experiment_state(
                    store,
                    "demo",
                    "self-iterate",
                    remote_runner=runner,
                )

                self.assertEqual(len(scripts), 1)
                self.assertIn("REMOTE_SYNC_DEFAULT_EVAL_EPOCHS=final", scripts[0])
                self.assertIn("REMOTE_SYNC_DEFAULT_EVAL_ENVS='coinrun starpilot'", scripts[0])
                self.assertNotIn("bossfight caveflyer", scripts[0])

    def test_sync_remote_state_discovers_latest_root_when_configured_root_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                base = Path(tmp) / "remote-runs"
                bad_root = base / "controller"
                good_root = base / "autonomous-current"
                train_root = base / "train-current"
                eval_root = base / "eval-current"
                for path in (bad_root, good_root, train_root, eval_root):
                    path.mkdir(parents=True)
                (train_root / "train.log").write_text("Done.\n", encoding="utf-8")
                (eval_root / "combined_summary.tsv").write_text(
                    "env\tepoch\tscore\ncoinrun\tfinal\t1.0\n",
                    encoding="utf-8",
                )
                (good_root / "events.jsonl").write_text(
                    json.dumps({"event": "supervisor_done"}) + "\n",
                    encoding="utf-8",
                )
                (good_root / "state.json").write_text(
                    json.dumps(
                        {
                            "current": {
                                "run_id": "train-current",
                                "eval_label": "eval-current",
                                "run_root": str(train_root),
                                "eval_root": str(eval_root),
                                "train_log_path": str(train_root / "train.log"),
                                "eval_epochs": ["final"],
                                "eval_envs": ["coinrun"],
                            }
                        }
                    ),
                    encoding="utf-8",
                )
                project = store.add_project("demo", project_path)
                store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": str(bad_root)}},
                )

                def runner(target, script, ssh_key, timeout):
                    completed = subprocess.run(
                        ["bash", "-lc", script],
                        text=True,
                        capture_output=True,
                        timeout=timeout,
                        check=False,
                    )
                    return RemoteExecResult(
                        stdout=completed.stdout,
                        stderr=completed.stderr,
                        returncode=completed.returncode,
                    )

                result = sync_remote_experiment_state(
                    store,
                    "demo",
                    "self-iterate",
                    remote_runner=runner,
                )

                self.assertEqual(result["snapshot"]["root"], str(good_root))
                self.assertEqual(result["snapshot"]["summary"]["rows"], 1)
                self.assertTrue(result["snapshot"]["summary"]["complete"])

    def test_remote_snapshot_detects_contract_command_mismatch(self) -> None:
        snapshot = _remote_snapshot(command_transform="raw", command_dropout="0.0")
        anomalies = classify_remote_experiment_snapshot(snapshot, stale_seconds=900)
        codes = {item["code"] for item in anomalies}
        self.assertIn("contract_command_mismatch", codes)

    def test_remote_sync_failure_does_not_trigger_anomaly_control(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": "/runs/controller"}},
                )
                store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_job_started",
                    payload={"remote_pid": "4242", "log_path": "/remote/log"},
                    status="running",
                )

                def runner(target, script, ssh_key, timeout):
                    raise RuntimeError("Permission denied (publickey)")

                calls = []

                def supervisor_runner(*args, **kwargs):
                    calls.append((args, kwargs))
                    return {}

                result = run_experiment_watchdog(
                    store,
                    "demo",
                    experiment_name="self-iterate",
                    stale_seconds=999,
                    remote_runner=runner,
                    supervisor_runner=supervisor_runner,
                )

                self.assertEqual(calls, [])
                self.assertEqual(result["experiments"][0]["action"], "skipped")
                event_types = [event["event_type"] for event in store.list_experiment_events(experiment["id"], limit=10)]
                self.assertIn("remote_sync_failed", event_types)
                self.assertNotIn("remote_anomaly_control_started", event_types)

    def test_watchdog_discovers_remote_state_root_from_recent_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                )
                store.add_experiment_event(
                    experiment["id"],
                    event_type="controller_recovered",
                    payload={
                        "supervisor": "/runs/controller",
                        "correct_run_id": "train-blockzscore-vd005",
                        "correct_eval_label": "eval-blockzscore-vd005",
                    },
                    status="running",
                )
                calls = []

                def runner(target, script, ssh_key, timeout):
                    calls.append(script)
                    return RemoteExecResult(stdout=json.dumps(_remote_snapshot()))

                result = run_experiment_watchdog(
                    store,
                    "demo",
                    experiment_name="self-iterate",
                    stale_seconds=900,
                    remote_runner=runner,
                )

                self.assertEqual(len(calls), 1)
                self.assertIn("REMOTE_SYNC_ROOT=/runs/controller", calls[0])
                self.assertEqual(result["experiments"][0]["action"], "skipped")
                self.assertEqual(result["experiments"][0]["reason"], "remote_active")
                self.assertIsNone(result["experiments"][0]["activity_event"])

    def test_watchdog_discovers_remote_state_root_after_noisy_control_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                )
                store.add_experiment_event(
                    experiment["id"],
                    event_type="controller_started",
                    payload={
                        "remote_state_root": "/runs/controller",
                        "run_id": "train-blockzscore-vd005",
                        "eval_label": "eval-blockzscore-vd005",
                    },
                    status="running",
                )
                store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_job_started",
                    payload={"remote_pid": "4242", "log_path": "/runs/train/mycodex_launch.log"},
                    status="running",
                )
                for index in range(1200):
                    store.add_experiment_event(
                        experiment["id"],
                        event_type="agent_message_delta",
                        message=f"noise {index}",
                        payload={"index": index},
                        status="running",
                    )
                calls = []

                def runner(target, script, ssh_key, timeout):
                    calls.append(script)
                    return RemoteExecResult(stdout=json.dumps(_remote_snapshot()))

                result = run_experiment_watchdog(
                    store,
                    "demo",
                    experiment_name="self-iterate",
                    stale_seconds=900,
                    remote_runner=runner,
                )

                self.assertEqual(len(calls), 1)
                self.assertIn("REMOTE_SYNC_ROOT=/runs/controller", calls[0])
                self.assertEqual(result["experiments"][0]["action"], "skipped")

    def test_pid_only_remote_snapshot_is_not_structured_process_failure(self) -> None:
        snapshot = _remote_snapshot(train_alive=False, rows=0, expected_rows=0)
        snapshot["root"] = ""
        snapshot["run_id"] = ""
        snapshot["eval_label"] = ""
        snapshot["state"] = {}
        snapshot["files"]["state"]["exists"] = False
        snapshot["files"]["state"]["path"] = ""
        snapshot["summary"]["path"] = ""

        anomalies = classify_remote_experiment_snapshot(snapshot, stale_seconds=900)
        codes = {item["code"] for item in anomalies}

        self.assertNotIn("process_dead_without_done", codes)

    def test_watchdog_triggers_control_for_remote_anomaly_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": "/runs/controller"}},
                )
                snapshot = _remote_snapshot(train_alive=False, rows=0, expected_rows=8)

                def runner(target, script, ssh_key, timeout):
                    return RemoteExecResult(stdout=json.dumps(snapshot))

                control_output = (
                    "```json\n"
                    "{\"auto_execute\": true, \"execution_class\": \"monitor-only\", "
                    "\"patch\": {\"required\": false}, "
                    "\"remote\": {\"command\": \"printf ok\\\\n\", \"timeout\": 10}}\n"
                    "```"
                )
                review_output = "```json\n{\"decision\": \"approve\"}\n```"
                with patch(
                    "mycodex.experiment_ops.start_experiment_control_turn",
                    return_value={"output_text": control_output, "run_id": "control-1", "status": "completed"},
                ) as control_mock, patch(
                    "mycodex.experiment_ops.start_experiment_peer_review_turn",
                    return_value={"output_text": review_output, "thread_id": "review-1"},
                ), patch(
                    "mycodex.experiment_ops.run_remote_script",
                    return_value=RemoteExecResult(stdout="ok\n"),
                ):
                    first = run_experiment_watchdog(
                        store,
                        "demo",
                        experiment_name="self-iterate",
                        stale_seconds=900,
                        remote_runner=runner,
                    )
                    second = run_experiment_watchdog(
                        store,
                        "demo",
                        experiment_name="self-iterate",
                        stale_seconds=900,
                        remote_runner=runner,
                    )

                self.assertEqual(first["experiments"][0]["action"], "remote_anomaly_control")
                self.assertEqual(second["experiments"][0]["action"], "skipped")
                self.assertEqual(
                    second["experiments"][0]["duplicate_remote_anomaly_control"]["reason"],
                    "duplicate_remote_anomaly_control",
                )
                self.assertEqual(control_mock.call_count, 1)
                event_types = [event["event_type"] for event in store.list_experiment_events(experiment["id"], limit=30)]
                self.assertIn("remote_anomaly", event_types)
                self.assertIn("remote_anomaly_control_started", event_types)
                self.assertIn("remote_anomaly_control_completed", event_types)

    def test_watchdog_retries_remote_anomaly_after_stale_control_run_cleared(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": "/runs/controller"}},
                )
                snapshot = _remote_snapshot(train_alive=False, rows=0, expected_rows=8)

                def runner(target, script, ssh_key, timeout):
                    return RemoteExecResult(stdout=json.dumps(snapshot))

                sync_remote_experiment_state(
                    store,
                    "demo",
                    "self-iterate",
                    remote_runner=runner,
                )
                anomaly = store.get_latest_experiment_event(experiment["id"], "remote_anomaly")
                batch_fingerprint = json.loads(anomaly["payload_json"])["batch_fingerprint"]
                store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_anomaly_control_started",
                    payload={"batch_fingerprint": batch_fingerprint},
                    status="running",
                )
                store.add_experiment_event(
                    experiment["id"],
                    event_type="experiment_control_run_marked_stale",
                    payload={"run_id": 9},
                    status="running",
                )
                control_output = (
                    "```json\n"
                    "{\"auto_execute\": true, \"execution_class\": \"monitor-only\", "
                    "\"patch\": {\"required\": false}, "
                    "\"remote\": {\"command\": \"printf ok\\\\n\", \"timeout\": 10}}\n"
                    "```"
                )
                review_output = "```json\n{\"decision\": \"approve\"}\n```"
                with patch(
                    "mycodex.experiment_ops.start_experiment_control_turn",
                    return_value={"output_text": control_output, "run_id": "control-1", "status": "completed"},
                ) as control_mock, patch(
                    "mycodex.experiment_ops.start_experiment_peer_review_turn",
                    return_value={"output_text": review_output, "thread_id": "review-1"},
                ), patch(
                    "mycodex.experiment_ops.run_remote_script",
                    return_value=RemoteExecResult(stdout="ok\n"),
                ):
                    result = run_experiment_watchdog(
                        store,
                        "demo",
                        experiment_name="self-iterate",
                        stale_seconds=900,
                        remote_runner=runner,
                    )

                self.assertEqual(result["experiments"][0]["action"], "remote_anomaly_control")
                control_mock.assert_called_once()

    def test_remote_anomaly_control_revises_within_same_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": "/runs/controller"}},
                )
                snapshot = _remote_snapshot(train_alive=False, rows=0, expected_rows=8)

                def runner(target, script, ssh_key, timeout):
                    return RemoteExecResult(stdout=json.dumps(snapshot))

                control_output = (
                    "```json\n"
                    "{\"auto_execute\": true, \"execution_class\": \"monitor-only\", "
                    "\"patch\": {\"required\": false}, "
                    "\"remote\": {\"command\": \"printf ok\\\\n\", \"timeout\": 10}}\n"
                    "```"
                )
                review_outputs = [
                    {"output_text": "```json\n{\"decision\": \"revise\", \"required_changes\": [\"tighten\"]}\n```", "thread_id": "review-1"},
                    {"output_text": "```json\n{\"decision\": \"approve\"}\n```", "thread_id": "review-2"},
                ]
                with patch(
                    "mycodex.experiment_ops.start_experiment_control_turn",
                    return_value={"output_text": control_output, "run_id": "control-1", "status": "completed"},
                ) as control_mock, patch(
                    "mycodex.experiment_ops.start_experiment_peer_review_turn",
                    side_effect=review_outputs,
                ) as peer_review_mock, patch(
                    "mycodex.experiment_ops.run_remote_script",
                    return_value=RemoteExecResult(stdout="ok\n"),
                ):
                    result = run_experiment_watchdog(
                        store,
                        "demo",
                        experiment_name="self-iterate",
                        stale_seconds=900,
                        remote_runner=runner,
                    )

                self.assertEqual(result["experiments"][0]["action"], "remote_anomaly_control")
                self.assertEqual(control_mock.call_count, 2)
                self.assertEqual(peer_review_mock.call_count, 2)
                events = store.list_experiment_events(experiment["id"], limit=50)
                messages = [event["message"] for event in events]
                self.assertIn(
                    "Peer review requested revision; retrying remote-anomaly control turn immediately.",
                    messages,
                )
                self.assertIn("Remote anomaly control cycle completed.", messages)

    def test_remote_anomaly_eval_only_skips_peer_review_and_launches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                subprocess.run(["git", "init"], cwd=project_path, check=True, capture_output=True)
                subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=project_path, check=True)
                subprocess.run(["git", "config", "user.name", "Test"], cwd=project_path, check=True)
                (project_path / "tracked.txt").write_text("clean\n")
                subprocess.run(["git", "add", "tracked.txt"], cwd=project_path, check=True)
                subprocess.run(["git", "commit", "-m", "init"], cwd=project_path, check=True, capture_output=True)
                commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=project_path, text=True).strip()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": "/runs/controller"}},
                )
                snapshot = _remote_snapshot(train_alive=False, rows=0, expected_rows=8)

                def runner(target, script, ssh_key, timeout):
                    return RemoteExecResult(stdout=json.dumps(snapshot))

                control_output = (
                    "```json\n"
                    "{"
                    "\"auto_execute\": true, "
                    "\"execution_class\": \"eval-only\", "
                    "\"patch\": {\"required\": false}, "
                    f"\"remote\": {{\"command\": \"printf eval\\\\n\", \"timeout\": 10, \"commit\": \"{commit}\"}}, "
                    "\"wandb\": {\"entity\": \"me\", \"project\": \"evals\", \"run_name\": \"heldout-eval\"}, "
                    "\"metrics\": [\"eval/mean_return\"], "
                    "\"hard_stop\": [\"crash\"]"
                    "}\n"
                    "```"
                )
                with patch(
                    "mycodex.experiment_ops.start_experiment_control_turn",
                    return_value={"output_text": control_output, "run_id": "control-1", "status": "completed"},
                ) as control_mock, patch(
                    "mycodex.experiment_ops.start_experiment_peer_review_turn",
                ) as peer_review_mock, patch(
                    "mycodex.experiment_ops.run_remote_script",
                ) as run_remote:
                    run_remote.side_effect = [
                        RemoteExecResult(
                            stdout=(
                                f"REMOTE_COMMIT={commit}\n"
                                "REMOTE_PID=123\n"
                                "REMOTE_LOG=/remote/eval.log\n"
                                "REMOTE_PID_FILE=/remote/eval.log.pid\n"
                            )
                        ),
                        RemoteExecResult(stdout="REMOTE_STATUS=running\n"),
                    ]
                    result = run_experiment_watchdog(
                        store,
                        "demo",
                        experiment_name="self-iterate",
                        stale_seconds=900,
                        remote_runner=runner,
                    )

                self.assertEqual(result["experiments"][0]["action"], "remote_anomaly_control")
                self.assertTrue(result["experiments"][0]["control_result"]["executed"])
                control_mock.assert_called_once()
                peer_review_mock.assert_not_called()
                self.assertEqual(run_remote.call_count, 2)
                event_types = [
                    event["event_type"]
                    for event in store.list_experiment_events(experiment["id"], limit=50)
                ]
                self.assertIn("peer_review_skipped", event_types)
                self.assertIn("remote_job_started", event_types)
                tasks = store.list_experiment_tasks(experiment["id"], limit=10)
                self.assertEqual(len(tasks), 1)
                self.assertEqual(tasks[0]["status"], "succeeded")
                self.assertEqual(tasks[0]["attempts"], 1)

    def test_remote_anomaly_control_bypasses_revise_after_policy_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": "/runs/controller"}},
                )
                snapshot = _remote_snapshot(train_alive=False, rows=0, expected_rows=8)

                def runner(target, script, ssh_key, timeout):
                    return RemoteExecResult(stdout=json.dumps(snapshot))

                control_output = """```json
{
  "auto_execute": true,
  "execution_class": "monitor-only",
  "patch": {"required": false},
  "remote": {"command": "printf ok\\n", "timeout": 10},
  "metrics": [],
  "hard_stop": [],
  "wandb": {}
}
```"""
                review_output = "```json\n{\"decision\": \"revise\", \"required_changes\": []}\n```"
                with patch(
                    "mycodex.experiment_ops.start_experiment_control_turn",
                    return_value={"output_text": control_output, "run_id": "control-1", "status": "completed"},
                ), patch(
                    "mycodex.experiment_ops.start_experiment_peer_review_turn",
                    return_value={"output_text": review_output, "thread_id": "review-1", "rounds": []},
                ), patch(
                    "mycodex.experiment_ops.run_remote_script",
                    return_value=RemoteExecResult(stdout="ok\n"),
                ) as remote_command:
                    result = run_experiment_watchdog(
                        store,
                        "demo",
                        experiment_name="self-iterate",
                        stale_seconds=900,
                        remote_runner=runner,
                        peer_review_bypass_timeout_seconds=0,
                    )

                self.assertTrue(result["experiments"][0]["control_result"]["executed"])
                remote_command.assert_called_once()
                event_types = [
                    event["event_type"]
                    for event in store.list_experiment_events(experiment["id"], limit=50)
                ]
                self.assertIn("peer_review_bypass_eligible", event_types)
                self.assertIn("peer_review_bypassed", event_types)
                self.assertIn("remote_monitor_command_completed", event_types)

    def test_peer_review_bypass_allows_timed_revise_with_required_changes(self) -> None:
        contract = {
            "auto_execute": True,
            "execution_class": "monitor-only",
            "patch": {"required": False},
            "remote": {"command": "printf ok\\n", "timeout": 10},
            "metrics": [],
            "hard_stop": [],
            "wandb": {},
        }

        assessment = _assess_peer_review_bypass(
            contract=contract,
            peer_review_decision={
                "decision": "revise",
                "required_changes": ["fix command binding before launch"],
            },
            peer_review_result={"status": "completed"},
            decision="revise",
            blocked_since=0,
            now=3600,
            allow_full_train=False,
            timeout_seconds=1800,
            bypass_on="revise,timeout",
            allowed_classes="read-only-preflight,diagnostic-patch,smoke-train,monitor-only",
        )

        self.assertTrue(assessment["eligible"])
        self.assertNotIn("peer_review_required_changes_present", assessment["failures"])
        self.assertIn("peer_review_required_changes_present", assessment["warnings"])

    def test_remote_anomaly_control_does_not_bypass_block_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": "/runs/controller"}},
                )
                snapshot = _remote_snapshot(train_alive=False, rows=0, expected_rows=8)

                def runner(target, script, ssh_key, timeout):
                    return RemoteExecResult(stdout=json.dumps(snapshot))

                control_output = """```json
{
  "auto_execute": true,
  "execution_class": "monitor-only",
  "patch": {"required": false},
  "remote": {"command": "printf ok\\n", "timeout": 10},
  "metrics": [],
  "hard_stop": [],
  "wandb": {}
}
```"""
                review_output = "```json\n{\"decision\": \"block\", \"reason\": \"destructive rm -rf risk\", \"required_changes\": [\"would run rm -rf on unrelated data\"]}\n```"
                with patch(
                    "mycodex.experiment_ops.start_experiment_control_turn",
                    return_value={"output_text": control_output, "run_id": "control-1", "status": "completed"},
                ), patch(
                    "mycodex.experiment_ops.start_experiment_peer_review_turn",
                    return_value={"output_text": review_output, "thread_id": "review-1", "rounds": []},
                ), patch("mycodex.experiment_ops.run_remote_script") as remote_command:
                    result = run_experiment_watchdog(
                        store,
                        "demo",
                        experiment_name="self-iterate",
                        stale_seconds=900,
                        remote_runner=runner,
                        peer_review_bypass_timeout_seconds=0,
                    )

                self.assertFalse(result["experiments"][0]["control_result"]["executed"])
                self.assertEqual(result["experiments"][0]["control_result"]["reason"], "peer_review_hard_blockers")
                self.assertIn("destructive_operation", result["experiments"][0]["control_result"]["hard_blockers"])
                remote_command.assert_not_called()
                event_types = [
                    event["event_type"]
                    for event in store.list_experiment_events(experiment["id"], limit=50)
                ]
                self.assertNotIn("peer_review_bypassed", event_types)
                self.assertIn("remote_anomaly_control_failed", event_types)

    def test_remote_monitor_failure_marks_anomaly_control_failed_and_retries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": "/runs/controller"}},
                )
                snapshot = _remote_snapshot(train_alive=False, rows=0, expected_rows=8)

                def runner(target, script, ssh_key, timeout):
                    return RemoteExecResult(stdout=json.dumps(snapshot))

                control_output = """```json
{
  "auto_execute": true,
  "execution_class": "monitor-only",
  "patch": {"required": false},
  "remote": {"command": "python3 - <<'PY'\\nprint('ok')\\nPY", "timeout": 10},
  "metrics": [],
  "hard_stop": [],
  "wandb": {}
}
```"""
                review_output = "```json\n{\"decision\": \"approve\"}\n```"
                with patch(
                    "mycodex.experiment_ops.start_experiment_control_turn",
                    return_value={"output_text": control_output, "run_id": "control-1", "status": "completed"},
                ) as control_mock, patch(
                    "mycodex.experiment_ops.start_experiment_peer_review_turn",
                    return_value={"output_text": review_output, "thread_id": "review-1"},
                ), patch(
                    "mycodex.experiment_ops.run_remote_script",
                    side_effect=[RuntimeError("remote command failed with exit 127: python3: command not found"), RemoteExecResult(stdout="ok\n")],
                ):
                    first = run_experiment_watchdog(
                        store,
                        "demo",
                        experiment_name="self-iterate",
                        stale_seconds=900,
                        remote_runner=runner,
                    )
                    second = run_experiment_watchdog(
                        store,
                        "demo",
                        experiment_name="self-iterate",
                        stale_seconds=900,
                        remote_runner=runner,
                    )

                self.assertFalse(first["experiments"][0]["control_result"]["executed"])
                self.assertEqual(first["experiments"][0]["control_result"]["reason"], "remote_monitor_command_failed")
                self.assertTrue(second["experiments"][0]["control_result"]["executed"])
                self.assertEqual(control_mock.call_count, 2)
                event_types = [
                    event["event_type"]
                    for event in store.list_experiment_events(experiment["id"], limit=80)
                ]
                self.assertIn("remote_monitor_command_failed", event_types)
                self.assertIn("remote_anomaly_control_failed", event_types)
                self.assertIn("remote_anomaly_control_completed", event_types)

    def test_duplicate_remote_anomaly_falls_through_to_blocked_result_gate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": "/runs/controller"}},
                )
                snapshot = _remote_snapshot(
                    train_alive=False,
                    queue_alive=False,
                    train_done=True,
                    queue_done=True,
                    rows=8,
                    expected_rows=8,
                    supervisor_alive=False,
                    watchdog_alive=False,
                    state_status="supervisor_done",
                )

                def runner(target, script, ssh_key, timeout):
                    return RemoteExecResult(stdout=json.dumps(snapshot))

                sync_remote_experiment_state(store, "demo", "self-iterate", remote_runner=runner)
                anomaly = store.get_latest_experiment_event(experiment["id"], "remote_anomaly")
                batch_fingerprint = json.loads(anomaly["payload_json"])["batch_fingerprint"]
                store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_anomaly_control_started",
                    payload={"trigger": "remote_anomaly", "batch_fingerprint": batch_fingerprint},
                    status="running",
                )
                store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_anomaly_control_completed",
                    payload={"trigger": "remote_anomaly", "batch_fingerprint": batch_fingerprint},
                    status="running",
                )
                store.add_experiment_event(
                    experiment["id"],
                    event_type="result_gate_parsed",
                    level="warning",
                    message="Result gate parsed; artifact manifest repair is required before training.",
                    payload={
                        "manual_review_required": True,
                        "allowed_next_contract_class": "blocked",
                        "training_allowed_by_this_gate": False,
                        "gate_blockers": ["manifest_repair_needed"],
                    },
                    status="running",
                    phase="waiting_for_decision",
                )
                control_output = """```json
{
  "auto_execute": true,
  "execution_class": "monitor-only",
  "patch": {"required": false},
  "remote": {"command": "printf repaired\\n", "timeout": 10},
  "metrics": [],
  "hard_stop": [],
  "wandb": {}
}
```"""
                review_output = "```json\n{\"decision\": \"approve\"}\n```"
                with patch(
                    "mycodex.experiment_ops.start_experiment_control_turn",
                    return_value={"output_text": control_output, "run_id": "control-1", "status": "completed"},
                ) as control_mock, patch(
                    "mycodex.experiment_ops.start_experiment_peer_review_turn",
                    return_value={"output_text": review_output, "thread_id": "review-1"},
                ), patch(
                    "mycodex.experiment_ops.run_remote_script",
                    return_value=RemoteExecResult(stdout="repaired\n"),
                ):
                    result = run_experiment_watchdog(
                        store,
                        "demo",
                        experiment_name="self-iterate",
                        stale_seconds=900,
                        remote_runner=runner,
                    )

                self.assertEqual(result["experiments"][0]["action"], "local_gate_control")
                self.assertTrue(result["experiments"][0]["control_result"]["executed"])
                control_mock.assert_called_once()
                self.assertEqual(
                    result["experiments"][0]["control_result"]["contract"]["execution_class"],
                    "monitor-only",
                )

    def test_peer_review_blocked_since_resets_after_successful_control(self) -> None:
        events = [
            {
                "event_type": "remote_anomaly_control_completed",
                "created_at": 2000,
                "payload_json": json.dumps({"trigger": "remote_anomaly", "batch_fingerprint": "abc"}),
            },
            {
                "event_type": "peer_review_decision",
                "created_at": 1000,
                "payload_json": json.dumps(
                    {
                        "trigger": "remote_anomaly",
                        "batch_fingerprint": "abc",
                        "decision": {"decision": "revise"},
                    }
                ),
            },
        ]
        self.assertIsNone(
            _peer_review_blocked_since(
                events,
                trigger="remote_anomaly",
                key_field="batch_fingerprint",
                key_value="abc",
            )
        )

    def test_peer_review_decision_failure_returns_partial_result(self) -> None:
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
                first = {
                    "run_id": 1,
                    "thread_id": "review-thread",
                    "turn_id": "turn-1",
                    "status": "completed",
                    "output_text": "critique only",
                }

                with patch(
                    "mycodex.service.send_prompt",
                    side_effect=[first, RuntimeError("decision turn failed")],
                ) as send_prompt_mock:
                    result = start_experiment_peer_review_turn(
                        store,
                        "demo",
                        "self-iterate",
                        control_output="control",
                        contract={"auto_execute": True, "execution_class": "monitor-only"},
                        timeout=900,
                    )

                self.assertEqual(result["thread_id"], "review-thread")
                self.assertEqual(result["rounds"][0]["status"], "completed")
                self.assertEqual(result["rounds"][1]["status"], "failed")
                self.assertEqual(send_prompt_mock.call_args_list[0].kwargs["timeout"], 600)
                self.assertEqual(send_prompt_mock.call_args_list[1].kwargs["timeout"], 180)
                event_types = [
                    event["event_type"]
                    for event in store.list_experiment_events(experiment["id"], limit=10)
                ]
                self.assertIn("peer_review_failed", event_types)
                self.assertIn("peer_review_completed", event_types)

    def test_process_dead_with_done_marker_is_not_crash_anomaly(self) -> None:
        snapshot = _remote_snapshot(
            train_alive=False,
            queue_alive=False,
            train_done=True,
            rows=8,
            expected_rows=8,
        )
        anomalies = classify_remote_experiment_snapshot(snapshot, stale_seconds=900)
        codes = {item["code"] for item in anomalies}
        self.assertNotIn("process_dead_without_done", codes)
        self.assertNotIn("eval_queue_dead_incomplete", codes)

    def test_completed_remote_snapshot_does_not_become_stale_heartbeat_anomaly(self) -> None:
        snapshot = _remote_snapshot(
            train_alive=False,
            queue_alive=False,
            train_done=True,
            queue_done=True,
            rows=8,
            expected_rows=8,
            supervisor_alive=False,
            watchdog_alive=False,
            state_status="supervisor_done",
        )
        old_mtime = time.time() - 7200
        snapshot["remote_now"] = time.time()
        snapshot["files"]["state"]["mtime"] = old_mtime
        snapshot["files"]["events"]["mtime"] = old_mtime

        anomalies = classify_remote_experiment_snapshot(snapshot, stale_seconds=900)
        codes = {item["code"] for item in anomalies}

        self.assertNotIn("remote_heartbeat_stale", codes)
        self.assertIn("remote_terminal_complete_ready_for_next", codes)

    def test_terminal_complete_remote_control_event_is_info_level(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": "/runs/controller"}},
                )
                snapshot = _remote_snapshot(
                    train_alive=False,
                    queue_alive=False,
                    train_done=True,
                    queue_done=True,
                    rows=8,
                    expected_rows=8,
                    supervisor_alive=False,
                    watchdog_alive=False,
                    state_status="supervisor_done",
                )

                def runner(target, script, ssh_key, timeout):
                    return RemoteExecResult(stdout=json.dumps(snapshot))

                sync_remote_experiment_state(
                    store,
                    "demo",
                    "self-iterate",
                    remote_runner=runner,
                )

                event = store.get_latest_experiment_event(experiment["id"], "remote_anomaly")
                self.assertIsNotNone(event)
                self.assertEqual(event["level"], "info")
                self.assertIn("remote_terminal_complete_ready_for_next", event["message"])

    def test_terminal_complete_anomaly_fingerprint_ignores_log_tail_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"remote_state": {"root": "/runs/controller"}},
                )
                snapshot = _remote_snapshot(
                    train_alive=False,
                    queue_alive=False,
                    train_done=True,
                    queue_done=True,
                    rows=8,
                    expected_rows=8,
                    supervisor_alive=False,
                    watchdog_alive=False,
                    state_status="supervisor_done",
                )
                stable_mtime = 12345.0
                snapshot["remote_now"] = stable_mtime + 100.0
                snapshot["files"]["state"]["mtime"] = stable_mtime
                snapshot["files"]["events"]["mtime"] = stable_mtime
                snapshot["logs"]["train"]["mtime"] = stable_mtime

                calls = 0

                def runner(target, script, ssh_key, timeout):
                    nonlocal calls
                    calls += 1
                    current = json.loads(json.dumps(snapshot))
                    current["logs"]["train"]["tail_sha256"] = f"trainhash-{calls}"
                    return RemoteExecResult(stdout=json.dumps(current))

                sync_remote_experiment_state(
                    store,
                    "demo",
                    "self-iterate",
                    remote_runner=runner,
                    stale_seconds=900,
                )
                sync_remote_experiment_state(
                    store,
                    "demo",
                    "self-iterate",
                    remote_runner=runner,
                    stale_seconds=900,
                )

                anomaly_events = [
                    event
                    for event in store.list_experiment_events(experiment["id"], limit=20)
                    if event["event_type"] == "remote_anomaly"
                ]
                self.assertEqual(len(anomaly_events), 1)

    def test_remote_clock_skew_is_observe_only(self) -> None:
        snapshot = _remote_snapshot()
        snapshot["remote_now"] = time.time() + 10000
        anomalies = classify_remote_experiment_snapshot(snapshot, stale_seconds=900)
        skew = [item for item in anomalies if item["code"] == "remote_clock_skew"]
        self.assertEqual(len(skew), 1)
        self.assertEqual(skew[0]["action"], "observe")

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

    def test_training_contract_requires_research_block(self) -> None:
        contract = {
            "auto_execute": True,
            "execution_class": "smoke-train",
            "remote": {"command": "python train.py", "timeout": 60},
            "metrics": ["heldout/mean_reward"],
            "hard_stop": ["nonfinite"],
            "wandb": {"project": "proj", "run_id": "run-1"},
        }

        failures = _contract_execution_failures(contract, allow_full_train=True)

        self.assertIn("missing_research_block", failures)

    def test_training_contract_accepts_hypothesis_driven_research_block(self) -> None:
        contract = {
            "auto_execute": True,
            "execution_class": "smoke-train",
            "remote": {"command": "python train.py", "timeout": 60},
            "metrics": ["heldout/mean_reward"],
            "hard_stop": ["nonfinite"],
            "wandb": {"project": "proj", "run_id": "run-1"},
            "research": {
                "hypothesis": "Task-local competence percentiles improve held-out policy selection.",
                "mechanism": "The value token carries policy stage information rather than raw return.",
                "baseline": "auto_relabel raw return on the same train/eval split.",
                "intervention": "Replace raw value targets with task-local competence percentile targets.",
                "falsifiable_predictions": ["Masking value tokens degrades raw return more than percentile targets."],
                "controls": ["Same seed, env split, eval horizon, and checkpoint epochs."],
                "decision_criteria": {
                    "supports": "Held-out mean and worst-env reward improve with stable diagnostics.",
                    "falsifies": "No improvement and the same value-token sensitivity remains.",
                    "ambiguous": "Only caveflyer improves while bossfight collapses.",
                },
                "analysis_plan": ["Compare held-out summary by env and run value-token perturbation eval."],
                "anti_sweep_rationale": "This tests the competence-stage mechanism, not a generic parameter sweep.",
            },
        }

        failures = _contract_execution_failures(contract, allow_full_train=True)

        self.assertNotIn("missing_research_block", failures)
        self.assertEqual(failures, [])


if __name__ == "__main__":
    unittest.main()
