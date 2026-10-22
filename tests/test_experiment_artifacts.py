from __future__ import annotations

import base64
import hashlib
import json
import io
import os
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from mycodex.db import Store
from mycodex.experiment_artifacts import (
    REMOTE_BOOTSTRAP_SOURCE, REMOTE_SUPERVISOR_SOURCE, assess_result_evidence,
    REMOTE_PROCESS_IDENTITY_SOURCE, REMOTE_RECEIPT_OBSERVATION_SOURCE, receipt_observation_status,
    build_execution_manifest, canonical_hash, execution_policy_failures,
    list_execution_manifests, patch_scope_failures, persist_execution_manifest,
    read_execution_manifest, render_remote_launch_script, resolve_time_budget,
)
from mycodex.experiment_ops import RemoteCommandError, RemoteExecResult, launch_remote_job, record_execution_result


def manifest(execution_id="test-1", execution_class="eval-only"):
    return build_execution_manifest(execution_id=execution_id, project_id=1, experiment_id=1,
        remote_ref="u@host:/repo", commit="abc", command="python eval.py", env={"TOKEN": "secret"},
        contract={"execution_class": execution_class, "seed": 42, "dataset": "heldout-v2"},
        time_budget_seconds=10)


def decode(script):
    return json.loads(base64.b64decode(shlex.split(script)[-1]))


class ExperimentArtifactTests(unittest.TestCase):
    def test_receipt_observation_requires_both_supervisor_and_child_identities(self):
        receipt = {"status": "running"}
        observed = {"REMOTE_SUPERVISOR_STATUS": "running", "REMOTE_CHILD_STATUS": "running"}
        self.assertEqual(receipt_observation_status(receipt, observed), "running")
        for status in ("absent", "identity_unverified", "identity_changed", "unknown", "not_running"):
            for key in observed:
                self.assertEqual(receipt_observation_status(receipt, {**observed, key: status}), "needs_reconciliation")
        self.assertEqual(receipt_observation_status(receipt, {}), "needs_reconciliation")
        self.assertEqual(receipt_observation_status({"status": "completed", "exit_code": 0}, {}), "completed")
        self.assertEqual(receipt_observation_status({"status": "completed", "exit_code": False}, {}), "needs_reconciliation")

    def test_shared_process_observation_handles_pid_reuse_and_unknown_state(self):
        namespace = {"os": os, "pathlib": __import__("pathlib")}
        exec(REMOTE_PROCESS_IDENTITY_SOURCE, namespace)
        observe = namespace["observe_process_identity"]
        with patch("os.kill"), patch("pathlib.Path.read_text", return_value="123 (child name) S " + "0 " * 18 + "42"):
            self.assertEqual(observe(123, "42"), "running")
            self.assertEqual(observe(123, "old"), "identity_changed")
            self.assertEqual(observe(123, None), "identity_unverified")
        with patch("os.kill", side_effect=PermissionError):
            self.assertEqual(observe(123, "42"), "unknown")
        with patch("os.kill", side_effect=ProcessLookupError):
            self.assertEqual(observe(123, "42"), "absent")
        compile(REMOTE_RECEIPT_OBSERVATION_SOURCE, "receipt-observation", "exec")

    def test_manifest_retry_is_immutable_and_does_not_persist_env_secret(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = persist_execution_manifest(root, manifest())
            self.assertEqual(persist_execution_manifest(root, manifest()), first)
            self.assertNotIn("secret", first.read_text())
            self.assertEqual(read_execution_manifest(root, "test-1")["metadata"]["seed"], 42)
            changed = manifest(); changed["code_commit"] = "changed"
            with self.assertRaisesRegex(ValueError, "immutable"):
                persist_execution_manifest(root, changed)
            self.assertEqual(len(list_execution_manifests(root, project_id=1, experiment_id=1)), 1)
            self.assertEqual(list_execution_manifests(root, project_id=2), [])

    def test_manifest_rejects_traversal_identifier(self):
        with self.assertRaises(ValueError):
            manifest("../escape")

    def test_numeric_budgets_and_legacy_prose_are_explicit(self):
        self.assertEqual(resolve_time_budget({"budget": "2 minutes"}), 120)
        self.assertEqual(resolve_time_budget({"budget": {"seconds": 1.5}}), 1.5)
        self.assertIsNone(resolve_time_budget({"budget": "small smoke run"}))
        for value in (0, -1, True, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                resolve_time_budget({"timeout": value})

    def test_patch_cannot_hide_an_undeclared_target(self):
        patch_info = {"required": True, "files": ["allowed.py"], "unified_diff":
                      "--- a/allowed.py\n+++ b/elsewhere.py\n@@ -1 +1 @@\n-a\n+b\n"}
        failures = patch_scope_failures(Path.cwd(), patch_info)
        self.assertIn("patch_outside_declared_files:elsewhere.py", failures)

    def test_patch_rejects_escape_git_metadata_and_policy_roots(self):
        for name in ("../outside", "/absolute", "C:\\outside", ".GIT/config"):
            with self.subTest(name=name):
                self.assertTrue(patch_scope_failures(Path.cwd(), {"required": True, "files": [name],
                    "unified_diff": f"--- a/{name}\n+++ b/{name}\n"}))
        self.assertIn("patch_path_outside_policy:other/file.py", patch_scope_failures(Path.cwd(),
            {"required": True, "files": ["other/file.py"], "unified_diff": "--- a/other/file.py\n+++ b/other/file.py\n"},
            {"allowed_patch_roots": ["src"]}))

    def test_remote_policy_bounds_even_when_model_requests_execution(self):
        contract = {"execution_class": "smoke-train", "remote": {"command": "python train.py", "budget": "small"}}
        failures = execution_policy_failures(contract, {"allow_remote": False, "max_runtime_seconds": 60,
                                                       "allowed_command_sha256": []})
        self.assertIn("policy_remote_execution_disabled", failures)
        self.assertIn("policy_time_budget_exceeded_or_unbounded", failures)
        self.assertIn("policy_remote_command_not_allowlisted", failures)
        self.assertTrue(execution_policy_failures({"remote": {"command": "rm -rf /user/data"}}))

    def test_exit_zero_and_claim_metadata_do_not_promote_smoke_run(self):
        data = manifest(execution_class="smoke-eval")
        supplied = {"status": "completed", "exit_code": 0, "receipt_verified": True,
                    "artifacts_verified": True, "artifact_refs": ["results.json"],
                    "evaluation": {"protocol": "heldout", "dataset": "v2", "baseline": "base", "metrics": {"score": 1}},
                    "independent_review": {"verdict": "supports", "review_ref": "review-1"}, "claim_refs": ["claim-1"],
                    "provenance": {key: data[key] for key in ("execution_id", "code_commit", "command_sha256")}}
        gate = assess_result_evidence(data, supplied)
        self.assertFalse(gate["method_claim_allowed"])
        self.assertEqual(gate["evidence_level"], "smoke")
        self.assertIn("execution_class_is_diagnostic", gate["reasons"])

    def test_missing_coverage_or_changed_provenance_cannot_support_claim(self):
        gate = assess_result_evidence(manifest(), {"status": "completed", "exit_code": 0,
                                                 "receipt_verified": True, "artifacts_verified": True})
        self.assertFalse(gate["method_claim_allowed"])
        self.assertIn("result_provenance_unbound", gate["reasons"])
        self.assertIn("missing_evaluation_dataset", gate["reasons"])

    def test_remote_sources_compile_and_payload_quoting_roundtrips(self):
        compile(REMOTE_BOOTSTRAP_SOURCE, "remote-bootstrap", "exec")
        compile(REMOTE_SUPERVISOR_SOURCE, "remote-supervisor", "exec")
        command = "printf '%s' \"$(touch /never-executed)\"\n"
        script = render_remote_launch_script(workdir="/repo with spaces", execution_id="quoted", command=command,
                    commit="abc", branch=None, fetch=False, log_path="log", pid_path="pid", env={},
                    manifest=manifest("quoted"), time_budget_seconds=2, termination_grace_seconds=1)
        self.assertEqual(decode(script)["command"], command)
        self.assertEqual(decode(script)["workdir"], "/repo with spaces")

    def test_result_recording_ignores_supplied_verification_flags(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "db.sqlite") as store:
            repo = Path(tmp) / "repo"; repo.mkdir()
            project = store.add_project("demo", repo)
            store.add_experiment(project["id"], name="eval", thread_id="thread", remote_ref="u@host:/repo")
            persist_execution_manifest(store.path.parent / "experiment_artifacts", manifest())
            result = record_execution_result(store, "demo", "eval", "test-1", {
                "status": "completed", "exit_code": 0, "receipt_verified": True,
                "artifacts_verified": True, "artifact_refs": [{"path": str(repo / "missing"), "sha256": "abc"}]})
            self.assertFalse(result["method_claim_allowed"])
            self.assertIn("remote_receipt_unverified", result["reasons"])
            self.assertFalse(result["artifact_verification"][0]["verified"])

    def test_production_missing_receipt_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "db.sqlite") as store:
            repo = Path(tmp) / "repo"; repo.mkdir()
            project = store.add_project("demo", repo)
            store.add_experiment(project["id"], name="eval", thread_id="thread", remote_ref="u@host:/repo")
            with patch("mycodex.experiment_ops.run_remote_script", return_value=RemoteExecResult(stdout="REMOTE_PID=12\n")):
                with self.assertRaisesRegex(RemoteCommandError, "receipt"):
                    launch_remote_job(store, "demo", "eval", command="python eval.py", commit="abc",
                                      require_clean=False, post_launch_check_delay=-1)

    def test_result_candidate_requires_bound_receipt_and_hashed_real_artifact(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "db.sqlite") as store:
            repo = Path(tmp) / "repo"; repo.mkdir()
            project = store.add_project("demo", repo)
            experiment = store.add_experiment(project["id"], name="eval", thread_id="thread", remote_ref="u@host:/repo")
            data = manifest()
            persist_execution_manifest(store.path.parent / "experiment_artifacts", data)
            store.add_experiment_event(experiment["id"], event_type="remote_job_status", payload={
                "execution_id": data["execution_id"], "receipt_verified": True,
                "receipt": {"execution_id": data["execution_id"], "manifest_sha256": canonical_hash(data),
                            "status": "completed", "exit_code": 0}})
            artifact = repo / "metrics.json"; artifact.write_text('{"score": 1}')
            supplied = {"artifact_refs": [{"path": str(artifact), "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest()}],
                        "evaluation": {"protocol": "heldout-v2", "dataset": "test", "baseline": "base", "metrics": ["score"]},
                        "provenance": {key: data[key] for key in ("execution_id", "code_commit", "command_sha256")},
                        "independent_review": {"verdict": "supports", "review_ref": "review-1"}, "claim_refs": ["claim-1"]}
            gate = record_execution_result(store, "demo", "eval", "test-1", supplied)
            self.assertTrue(gate["method_claim_allowed"])
            self.assertEqual(gate["evidence_level"], "claim_candidate")
            self.assertEqual(gate["result_path"], record_execution_result(store, "demo", "eval", "test-1", supplied)["result_path"])

    def test_string_false_and_v1_unbounded_budget_cannot_authorize_execution(self):
        from mycodex.experiment_ops import execute_execution_contract, extract_execution_contract
        self.assertIsNone(extract_execution_contract('{"auto_execute":"false","execution_class":"monitor-only"}'))
        self.assertIsNone(extract_execution_contract(json.dumps({"schema_version": "1", "auto_execute": True,
            "execution_class": "monitor-only", "reason": "test", "remote": {"command": "echo ok", "budget": "small"}})))
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "db.sqlite") as store:
            repo = Path(tmp) / "repo"; repo.mkdir()
            project = store.add_project("demo", repo)
            store.add_experiment(project["id"], name="eval", thread_id="thread", remote_ref="u@host:/repo")
            with patch("mycodex.experiment_ops.run_remote_script") as runner:
                result = execute_execution_contract(store, "demo", "eval", {"auto_execute": "false",
                    "execution_class": "monitor-only", "remote": {"command": "echo ok", "timeout": 10}})
            self.assertEqual(result["reason"], "contract_invalid")
            runner.assert_not_called()

    def test_local_check_expiration_kills_tree_and_returns_failure_without_hanging(self):
        from unittest.mock import Mock
        from mycodex.experiment_ops import _run_local_check_command
        process = Mock(pid=98765)
        process.communicate.side_effect = [subprocess.TimeoutExpired("check", 0.01), ("partial", "")]
        with patch("subprocess.Popen", return_value=process), \
                patch("subprocess.run", return_value=Mock(returncode=0)) as terminate, \
                patch("os.killpg", create=True) as terminate_group:
            completed = _run_local_check_command(Path.cwd(), "unused", 0.01)
        self.assertEqual(completed.returncode, 124)
        self.assertIn("exceeded 0.01s", completed.stderr)
        self.assertEqual(process.communicate.call_args_list[0].kwargs["timeout"], 0.01)
        if os.name == "nt":
            self.assertEqual(terminate.call_args.args[0], ["taskkill", "/PID", "98765", "/T", "/F"])
        else:
            terminate_group.assert_called_once()

    def test_local_check_budget_is_capped_by_policy_before_execution(self):
        from mycodex.experiment_ops import execute_execution_contract
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "db.sqlite") as store:
            repo = Path(tmp) / "repo"; repo.mkdir()
            project = store.add_project("demo", repo)
            store.add_experiment(project["id"], name="eval", thread_id="thread", remote_ref="u@host:/repo",
                                 config={"execution_policy": {"max_local_check_seconds": 5}})
            contract = {"auto_execute": True, "execution_class": "monitor-only", "local_checks": ["unused"],
                        "local_check_timeout_seconds": 10, "remote": {"command": "echo done", "timeout": 1}}
            with patch("mycodex.experiment_ops._run_local_check_command", return_value=subprocess.CompletedProcess("unused", 0, "", "")) as check, \
                    patch("mycodex.experiment_ops.run_remote_script", return_value=RemoteExecResult(stdout="done")):
                result = execute_execution_contract(store, "demo", "eval", contract)
            self.assertTrue(result["executed"])
            self.assertEqual(check.call_args.args[2], 5)

    def test_dry_run_has_stable_identity_and_no_manifest_write(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "db.sqlite") as store:
            repo = Path(tmp) / "repo"; repo.mkdir()
            project = store.add_project("demo", repo)
            store.add_experiment(project["id"], name="eval", thread_id="thread", remote_ref="u@host:/repo")
            a = launch_remote_job(store, "demo", "eval", command="python eval.py", commit="abc", require_clean=False, dry_run=True)
            b = launch_remote_job(store, "demo", "eval", command="python eval.py", commit="abc", require_clean=False, dry_run=True)
            self.assertEqual(a["execution_id"], b["execution_id"])
            self.assertEqual(a["log_path"], b["log_path"])
            self.assertFalse((store.path.parent / "experiment_artifacts").exists())

    def test_existing_failed_remote_claim_is_not_relaunched_cross_platform(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / ".mycodex" / "executions" / "failed"
            root.mkdir(parents=True)
            (root / "receipt.json").write_text(json.dumps({"execution_id": "failed", "manifest_sha256": "hash", "status": "failed"}))
            payload = {"workdir": tmp, "execution_id": "failed", "manifest_sha256": "hash"}
            encoded = base64.b64encode(json.dumps(payload).encode()).decode()
            result = subprocess.run([sys.executable, "-c", REMOTE_BOOTSTRAP_SOURCE, encoded], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("previous execution is failed", result.stderr)
            self.assertEqual(len(list(root.iterdir())), 1)

    def test_existing_completed_remote_claim_reuses_receipt_cross_platform(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / ".mycodex" / "executions" / "done"; root.mkdir(parents=True)
            receipt = {"execution_id": "done", "manifest_sha256": "hash", "status": "completed", "exit_code": 0,
                       "remote_commit": "abc", "pid": 123, "log_path": "run.log", "pid_path": "run.pid"}
            (root / "receipt.json").write_text(json.dumps(receipt))
            encoded = base64.b64encode(json.dumps({"workdir": tmp, "execution_id": "done", "manifest_sha256": "hash"}).encode()).decode()
            result = subprocess.run([sys.executable, "-c", REMOTE_BOOTSTRAP_SOURCE, encoded], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("REMOTE_REUSED=1", result.stdout)
            self.assertIn("REMOTE_EXECUTION_STATUS=completed", result.stdout)
            self.assertEqual(len(list(root.iterdir())), 1)

    def test_new_claim_and_lost_response_retry_spawn_once_with_synthetic_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = manifest("synthetic")
            script = render_remote_launch_script(workdir=tmp, execution_id="synthetic", command="echo unused",
                      commit="abc", branch=None, fetch=False, log_path="run.log", pid_path="run.pid", env={},
                      manifest=data, time_budget_seconds=2, termination_grace_seconds=1)
            payload = decode(script)
            encoded = base64.b64encode(json.dumps(payload).encode()).decode()
            root = Path(tmp) / ".mycodex/executions/synthetic"

            def spawn(*args, **kwargs):
                (root / "receipt.json").write_text(json.dumps({"execution_id": "synthetic",
                    "manifest_sha256": canonical_hash(data), "status": "completed", "exit_code": 0,
                    "remote_commit": "abc", "pid": 456, "log_path": "run.log", "pid_path": "run.pid"}))

            with patch("sys.argv", ["bootstrap", encoded]), patch("subprocess.run"), \
                    patch("subprocess.check_output", return_value="abc\n"), \
                    patch("subprocess.Popen", side_effect=spawn) as process, patch("sys.stdout", new_callable=io.StringIO):
                exec(compile(REMOTE_BOOTSTRAP_SOURCE, "remote-bootstrap", "exec"), {})
                # First response is deliberately discarded; second call must only read the receipt.
                exec(compile(REMOTE_BOOTSTRAP_SOURCE, "remote-bootstrap", "exec"), {})
                self.assertEqual(process.call_count, 1)

    def test_distinct_execution_cannot_change_shared_checkout_while_prior_job_active(self):
        with tempfile.TemporaryDirectory() as tmp:
            prior = Path(tmp) / ".mycodex/executions/active"; prior.mkdir(parents=True)
            (prior / "receipt.json").write_text(json.dumps({"status": "running", "pid": 123}))
            payload = decode(render_remote_launch_script(workdir=tmp, execution_id="next", command="echo unused",
                commit="different", branch=None, fetch=True, log_path="log", pid_path="pid", env={},
                manifest=manifest("next"), time_budget_seconds=1, termination_grace_seconds=0))
            encoded = base64.b64encode(json.dumps(payload).encode()).decode()
            with patch("sys.argv", ["bootstrap", encoded]), patch("subprocess.run") as git, \
                    patch("subprocess.Popen") as process, self.assertRaisesRegex(RuntimeError, "active or ambiguous"):
                exec(compile(REMOTE_BOOTSTRAP_SOURCE, "bootstrap", "exec"), {})
            git.assert_not_called()
            process.assert_not_called()
            self.assertFalse((Path(tmp) / ".mycodex/checkout.lock").exists())

    def test_numeric_timeout_supervisor_sends_term_then_kill_with_synthetic_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = {"execution_id": "bounded", "manifest_sha256": "hash", "remote_commit": "abc",
                       "log_path": str(root / "run.log"), "pid_path": str(root / "run.pid"),
                       "env": {}, "command": "unused", "workdir": tmp,
                       "time_budget_seconds": 0.1, "termination_grace_seconds": 0.2}
            (root / "launch.json").write_text(json.dumps(payload))
            from unittest.mock import Mock
            child = Mock(pid=9876, returncode=-9)
            child.wait.side_effect = [subprocess.TimeoutExpired("child", 0.1),
                                     subprocess.TimeoutExpired("child", 0.2), -9]
            with patch("sys.argv", ["supervisor", tmp]), patch("subprocess.Popen", return_value=child), \
                    patch("signal.SIGHUP", 1, create=True), patch("signal.SIGKILL", 9, create=True), \
                    patch("signal.signal"), patch("os.killpg", create=True) as kill_group:
                exec(compile(REMOTE_SUPERVISOR_SOURCE, "remote-supervisor", "exec"), {})
            receipt = json.loads((root / "receipt.json").read_text())
            self.assertEqual(receipt["status"], "timed_out")
            self.assertEqual(receipt["exit_code"], 124)
            self.assertGreaterEqual(kill_group.call_count, 2)
            self.assertEqual(kill_group.call_args_list[0].args[1], 15)
            self.assertEqual(kill_group.call_args_list[-1].args[1], 9)
            self.assertEqual(child.wait.call_args_list[0].kwargs["timeout"], 0.1)

    def test_restarted_supervisor_preserves_receipt_and_never_spawns_another_workload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "launch.json").write_text(json.dumps({"execution_id": "already-owned"}))
            (root / "supervisor.claim").mkdir()
            previous = {"status": "running", "pid": 123, "supervisor_pid": 456}
            (root / "receipt.json").write_text(json.dumps(previous))
            with patch("sys.argv", ["supervisor", tmp]), patch("subprocess.Popen") as child, \
                    patch("signal.SIGHUP", 1, create=True), patch("signal.signal"), \
                    self.assertRaisesRegex(RuntimeError, "duplicate workload refused"):
                exec(compile(REMOTE_SUPERVISOR_SOURCE, "remote-supervisor", "exec"), {})
            child.assert_not_called()
            self.assertEqual(json.loads((root / "receipt.json").read_text()), previous)

    def test_dead_supervisor_refuses_receipt_reuse_without_launching_or_killing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / ".mycodex/executions/owned"; root.mkdir(parents=True)
            previous = {"execution_id": "owned", "manifest_sha256": "hash", "status": "running",
                        "pid": 123, "supervisor_pid": 456, "supervisor_start_ticks": "old"}
            (root / "receipt.json").write_text(json.dumps(previous))
            encoded = base64.b64encode(json.dumps({"workdir": tmp, "execution_id": "owned", "manifest_sha256": "hash"}).encode()).decode()
            with patch("sys.argv", ["bootstrap", encoded]), patch("subprocess.Popen") as child, \
                    patch("os.kill", side_effect=ProcessLookupError) as probe, \
                    self.assertRaisesRegex(RuntimeError, "budget enforcement unknown"):
                exec(compile(REMOTE_BOOTSTRAP_SOURCE, "remote-bootstrap", "exec"), {})
            child.assert_not_called()
            probe.assert_called_once_with(456, 0)
            self.assertEqual(json.loads((root / "receipt.json").read_text()), previous)

    def test_exited_child_waits_for_durable_terminal_receipt_without_duplicate_spawn(self):
        # A supervisor can reap its child before atomically saving the terminal
        # receipt. A retry during that window must still refuse the ambiguity.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / ".mycodex/executions/owned"; root.mkdir(parents=True)
            receipt_path = root / "receipt.json"
            running = {"execution_id": "owned", "manifest_sha256": "hash", "status": "running",
                       "pid": 123, "pid_start_ticks": "43", "supervisor_pid": 456,
                       "supervisor_start_ticks": "42", "remote_commit": "abc",
                       "log_path": "run.log", "pid_path": "run.pid"}
            receipt_path.write_text(json.dumps(running))
            encoded = base64.b64encode(json.dumps({"workdir": tmp, "execution_id": "owned",
                                                  "manifest_sha256": "hash"}).encode()).decode()
            original_read = Path.read_text

            def read_receipt_or_supervisor_stat(path, *args, **kwargs):
                if path == Path("/proc/456/stat"):
                    return "456 (supervisor) S " + "0 " * 18 + "42"
                return original_read(path, *args, **kwargs)

            with patch("sys.argv", ["bootstrap", encoded]), patch("subprocess.Popen") as child, \
                    patch("os.kill", side_effect=[None, ProcessLookupError]) as probe, \
                    patch.object(Path, "read_text", autospec=True, side_effect=read_receipt_or_supervisor_stat), \
                    self.assertRaisesRegex(RuntimeError, "receipt says running but original process is absent"):
                exec(compile(REMOTE_BOOTSTRAP_SOURCE, "remote-bootstrap", "exec"), {})
            child.assert_not_called()
            self.assertEqual([call.args for call in probe.call_args_list], [(456, 0), (123, 0)])
            self.assertEqual(json.loads(receipt_path.read_text()), running)

            # Only the original supervisor's durable terminal state resolves the
            # ambiguity; the retry then returns the receipt without any spawn.
            completed = {**running, "status": "completed", "exit_code": 0}
            receipt_path.write_text(json.dumps(completed))
            with patch("sys.argv", ["bootstrap", encoded]), patch("subprocess.Popen") as child, \
                    patch("sys.stdout", new_callable=io.StringIO) as output:
                exec(compile(REMOTE_BOOTSTRAP_SOURCE, "remote-bootstrap", "exec"), {})
            child.assert_not_called()
            self.assertIn("REMOTE_REUSED=1", output.getvalue())
            self.assertIn("REMOTE_EXECUTION_STATUS=completed", output.getvalue())
            self.assertEqual(json.loads(receipt_path.read_text()), completed)
            self.assertEqual(list(root.iterdir()), [receipt_path])


class ProtocolLeaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temporary.name) / "db.sqlite")
        repo = Path(self.temporary.name) / "repo"; repo.mkdir()
        project = self.store.add_project("demo", repo)
        self.experiment = self.store.add_experiment(project["id"], name="eval", thread_id="thread", remote_ref="u@host:/repo")
        self.contract = {"auto_execute": True, "execution_class": "monitor-only", "patch": {"required": False},
                         "remote": {"command": "echo diagnostic", "timeout": 10}}

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def record(self, context="lease"):
        from mycodex.experiment_ops import record_execution_contract_task
        return record_execution_contract_task(self.store, "demo", "eval", self.contract,
                                              trigger="manual", context_key=context)

    def test_start_renew_complete_reject_expired_and_reassigned_owner(self):
        task_id = self.record()["task"]["id"]
        experiment_id = self.experiment["id"]
        self.store.lease_experiment_task(experiment_id, task_id, owner="old", lease_seconds=10, now=100)
        self.assertIsNone(self.store.start_experiment_task_if_owned(experiment_id, task_id, owner="wrong", now=101))
        self.assertIsNotNone(self.store.start_experiment_task_if_owned(experiment_id, task_id, owner="old", now=101))
        self.assertTrue(self.store.renew_experiment_task_lease(experiment_id, task_id, owner="old", lease_seconds=10, now=102))
        self.assertFalse(self.store.renew_experiment_task_lease(experiment_id, task_id, owner="old", now=113))
        self.assertIsNone(self.store.complete_experiment_task_if_owned(experiment_id, task_id, "succeeded", owner="old", now=113))
        self.store.update_experiment_task_status(experiment_id, task_id, "ready")
        self.store.lease_experiment_task(experiment_id, task_id, owner="new", lease_seconds=10, now=114)
        self.assertIsNone(self.store.start_experiment_task_if_owned(experiment_id, task_id, owner="old", now=115))
        self.assertIsNotNone(self.store.start_experiment_task_if_owned(experiment_id, task_id, owner="new", now=115))
        self.assertFalse(self.store.renew_experiment_task_lease(experiment_id, task_id, owner="old", now=115))
        self.assertIsNone(self.store.complete_experiment_task_if_owned(experiment_id, task_id, "failed", owner="old", now=115))
        current = self.store.get_experiment_task(experiment_id, task_id)
        self.assertEqual((current["status"], current["lease_owner"]), ("running", "new"))
        self.assertIsNotNone(self.store.complete_experiment_task_if_owned(experiment_id, task_id, "succeeded", owner="new", now=116))

    def test_recording_same_live_task_cannot_reset_lease_or_replace_contract(self):
        from mycodex.experiment_ops import _lease_recorded_protocol_task
        record = self.record(); task_id = record["task"]["id"]; experiment_id = self.experiment["id"]
        self.store.lease_experiment_task(experiment_id, task_id, owner="live", lease_seconds=60)
        self.store.start_experiment_task_if_owned(experiment_id, task_id, owner="live")
        repeated = self.record()["task"]
        self.assertEqual((repeated["status"], repeated["lease_owner"]), ("running", "live"))
        self.store.upsert_experiment_task(experiment_id, idempotency_key=repeated["idempotency_key"],
            status="ready", contract={"unexpected": "replacement"})
        current = self.store.get_experiment_task(experiment_id, task_id)
        self.assertEqual(current["contract_json"], repeated["contract_json"])
        self.assertIsNone(_lease_recorded_protocol_task(self.store, self.experiment, task_id,
                                                      contract=self.contract, owner="competitor"))
        self.assertEqual(self.store.get_experiment_task(experiment_id, task_id)["lease_owner"], "live")

    def test_slow_execution_renews_from_separate_connection(self):
        from mycodex.experiment_ops import run_ready_experiment_tasks
        self.record()
        enough_pulses = threading.Event()
        pulse_calls = []
        original_renew = Store.renew_experiment_task_lease
        main_thread = threading.current_thread()

        def observed_renew(instance, *args, **kwargs):
            success = original_renew(instance, *args, **kwargs)
            if threading.current_thread() is not main_thread:
                self.assertIsNot(instance.conn, self.store.conn)
                pulse_calls.append(success)
                if len(pulse_calls) >= 2:
                    enough_pulses.set()
            return success

        def slow_execute(*args, **kwargs):
            self.assertTrue(enough_pulses.wait(3), "protocol heartbeat did not renew during execution")
            self.assertTrue(kwargs["execution_guard"]())
            return {"executed": True}

        with patch.object(Store, "renew_experiment_task_lease", observed_renew), \
                patch("mycodex.experiment_ops.execute_execution_contract", side_effect=slow_execute):
            result = run_ready_experiment_tasks(self.store, "demo", "eval", lease_seconds=3)
        self.assertTrue(all(pulse_calls))
        self.assertEqual(result["tasks"][0]["task"]["status"], "succeeded")
        self.assertFalse(any(t.name.startswith("experiment-lease-") for t in threading.enumerate()))

    def test_lost_owner_cannot_complete_reassigned_task(self):
        from mycodex.experiment_ops import run_ready_experiment_tasks
        task_id = self.record()["task"]["id"]
        experiment_id = self.experiment["id"]

        def reassigned_during_execution(*args, **kwargs):
            self.store.update_experiment_task_status(experiment_id, task_id, "ready")
            self.store.lease_experiment_task(experiment_id, task_id, owner="replacement", lease_seconds=60)
            self.store.start_experiment_task_if_owned(experiment_id, task_id, owner="replacement")
            return {"executed": True}

        with patch("mycodex.experiment_ops.execute_execution_contract", side_effect=reassigned_during_execution):
            result = run_ready_experiment_tasks(self.store, "demo", "eval")
        self.assertFalse(result["tasks"][0]["execution"]["executed"])
        current = self.store.get_experiment_task(experiment_id, task_id)
        self.assertEqual((current["status"], current["lease_owner"]), ("running", "replacement"))
        self.assertIn("experiment_task_lease_lost", [event["event_type"] for event in self.store.list_experiment_events(experiment_id)])

    def test_recovery_respects_unexpired_lease_even_with_old_activity_timestamp(self):
        from mycodex.experiment_ops import recover_stale_experiment_tasks
        task_id = self.record()["task"]["id"]; experiment_id = self.experiment["id"]
        self.store.lease_experiment_task(experiment_id, task_id, owner="live", lease_seconds=60)
        self.store.start_experiment_task_if_owned(experiment_id, task_id, owner="live")
        self.store.conn.execute("UPDATE experiment_tasks SET updated_at=? WHERE id=?", (int(time.time()) - 100, task_id))
        self.store.conn.commit()
        result = recover_stale_experiment_tasks(self.store, project_name="demo", experiment_name="eval",
                                                running_timeout_seconds=1)
        self.assertEqual(result["recovered_tasks"], 0)
        self.assertEqual(self.store.get_experiment_task(experiment_id, task_id)["status"], "running")

    def test_monitor_retries_have_distinct_lineage_and_three_attempt_cap(self):
        ids = []
        for attempt in range(1, 4):
            record = self.record("retry")
            task = record["task"]; ids.append(task["id"])
            if attempt > 1:
                lineage = json.loads(task["contract_json"])["_protocol_retry"]
                self.assertEqual(lineage, {"root_task_id": ids[0], "retry_of_task_id": ids[-2],
                                           "attempt": attempt, "max_attempts": 3})
            self.store.update_experiment_task_status(self.experiment["id"], task["id"], "failed",
                                                    error="remote_monitor_command_failed")
        exhausted = self.record("retry")
        self.assertEqual(len(set(ids)), 3)
        self.assertEqual(exhausted["task"]["id"], ids[-1])
        self.assertEqual(exhausted["readiness"]["reason"], "monitor_retry_limit_reached")
        self.assertEqual(len(self.store.list_experiment_tasks(self.experiment["id"])), 3)
        self.assertTrue(all(self.store.get_experiment_task(self.experiment["id"], task_id)["status"] == "failed" for task_id in ids))


@unittest.skipUnless(sys.platform.startswith("linux") and shutil.which("bash") and shutil.which("git"),
                     "Detached remote supervisor integration requires native Linux, bash and git")
class RemoteSupervisorIntegrationTests(unittest.TestCase):
    def run_payload(self, payload):
        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        return subprocess.run([sys.executable, "-c", REMOTE_BOOTSTRAP_SOURCE, encoded],
                              capture_output=True, text=True, timeout=15)

    def prepare(self, root, command, budget=3):
        subprocess.run(["git", "init"], cwd=root, check=True, capture_output=True)
        (root / "tracked").write_text("initial")
        subprocess.run(["git", "add", "tracked"], cwd=root, check=True, capture_output=True)
        subprocess.run(["git", "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "initial"],
                       cwd=root, check=True, capture_output=True)
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
        return decode(render_remote_launch_script(workdir=str(root), execution_id="integration", command=command,
                    commit=commit, branch=None, fetch=False, log_path="run.log", pid_path="run.pid", env={},
                    manifest=manifest("integration"), time_budget_seconds=budget, termination_grace_seconds=0.2))

    def wait_receipt(self, root, statuses, timeout=5):
        path = root / ".mycodex/executions/integration/receipt.json"
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                receipt = json.loads(path.read_text())
                if receipt.get("status") in statuses:
                    return receipt
            except (FileNotFoundError, json.JSONDecodeError):
                pass
            time.sleep(0.02)
        self.fail(f"receipt never reached {statuses}")

    @staticmethod
    def process_matches(pid, ticks):
        try:
            stat = Path(f"/proc/{int(pid)}/stat").read_text().rsplit(")", 1)[1].split()
            return stat[0] != "Z" and stat[19] == ticks
        except (OSError, ValueError, IndexError, TypeError):
            return False

    def cleanup_owned(self, root):
        """Only terminate processes whose identity came from this temporary run."""
        try:
            receipt = json.loads((root / ".mycodex/executions/integration/receipt.json").read_text())
        except (OSError, ValueError):
            return
        for key, tick_key, group in (("supervisor_pid", "supervisor_start_ticks", False),
                                     ("pid", "pid_start_ticks", True)):
            pid = receipt.get(key)
            if self.process_matches(pid, receipt.get(tick_key)):
                try:
                    (os.killpg if group else os.kill)(int(pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def test_retry_after_lost_response_spawns_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = self.prepare(root, "echo launched >> count; sleep 0.4")
            try:
                first = self.run_payload(payload)
                self.assertEqual(first.returncode, 0, first.stderr)
                # Discard the response as if the SSH connection was lost. Child
                # exit and receipt persistence are separate events, so observe
                # the durable terminal receipt instead of assuming a sleep is
                # long enough for the supervisor to finish its wait/save cycle.
                receipt = self.wait_receipt(root, {"completed"})
                self.assertEqual(receipt["exit_code"], 0)
                second = self.run_payload(payload)
                self.assertEqual(second.returncode, 0, second.stderr)
                self.assertIn("REMOTE_REUSED=1", second.stdout)
                self.assertEqual((root / "count").read_text().splitlines(), ["launched"])
            finally:
                self.cleanup_owned(root)

    def test_numeric_budget_terminates_and_failed_receipt_blocks_relaunch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = self.prepare(root, "echo launched >> count; sleep 30", budget=0.2)
            first = self.run_payload(payload)
            self.assertEqual(first.returncode, 0, first.stderr)
            receipt_path = root / ".mycodex/executions/integration/receipt.json"
            deadline = time.monotonic() + 4
            while time.monotonic() < deadline:
                receipt = json.loads(receipt_path.read_text())
                if receipt["status"] == "timed_out": break
                time.sleep(0.05)
            self.assertEqual(receipt["status"], "timed_out")
            self.assertEqual(receipt["exit_code"], 124)
            second = self.run_payload(payload)
            self.assertNotEqual(second.returncode, 0)
            self.assertEqual((root / "count").read_text().splitlines(), ["launched"])

    def test_observer_process_death_does_not_duplicate_detached_workload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = self.prepare(root, "echo launched >> count; sleep 1", budget=4)
            encoded = base64.b64encode(json.dumps(payload).encode()).decode()
            # Keep the observer alive after bootstrap, then kill it like a lost
            # SSH connection. The production supervisor is unchanged and detached.
            source = REMOTE_BOOTSTRAP_SOURCE + "\nimport time; time.sleep(30)\n"
            observer = subprocess.Popen([sys.executable, "-c", source, encoded],
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                self.wait_receipt(root, {"running"})
                self.assertIsNone(observer.poll())
                observer.kill()
                observer.communicate(timeout=5)
                second = self.run_payload(payload)
                self.assertEqual(second.returncode, 0, second.stderr)
                self.assertIn("REMOTE_REUSED=1", second.stdout)
                self.wait_receipt(root, {"completed"})
                self.assertEqual((root / "count").read_text().splitlines(), ["launched"])
            finally:
                if observer.poll() is None:
                    observer.kill(); observer.communicate(timeout=5)
                self.cleanup_owned(root)

    def test_dead_supervisor_and_direct_restart_refuse_duplicate_workload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = self.prepare(root, "echo launched >> count; sleep 30", budget=5)
            try:
                first = self.run_payload(payload)
                self.assertEqual(first.returncode, 0, first.stderr)
                receipt = self.wait_receipt(root, {"running"})
                deadline = time.monotonic() + 2
                while not (root / "count").exists() and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue((root / "count").exists())
                self.assertTrue(self.process_matches(receipt["supervisor_pid"], receipt["supervisor_start_ticks"]))
                os.kill(receipt["supervisor_pid"], signal.SIGKILL)
                deadline = time.monotonic() + 3
                while self.process_matches(receipt["supervisor_pid"], receipt["supervisor_start_ticks"]) and time.monotonic() < deadline:
                    time.sleep(0.02)
                second = self.run_payload(payload)
                self.assertNotEqual(second.returncode, 0)
                self.assertIn("supervisor is absent", second.stderr)
                execution_root = root / ".mycodex/executions/integration"
                restarted = subprocess.run([sys.executable, str(execution_root / "supervisor.py"), str(execution_root)],
                                           capture_output=True, text=True, timeout=5)
                self.assertNotEqual(restarted.returncode, 0)
                self.assertIn("duplicate workload refused", restarted.stderr)
                self.assertEqual((root / "count").read_text().splitlines(), ["launched"])
            finally:
                self.cleanup_owned(root)

    def test_concurrent_retries_claim_one_workload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = self.prepare(root, "echo launched >> count; sleep 0.8", budget=4)
            encoded = base64.b64encode(json.dumps(payload).encode()).decode()
            observers = [subprocess.Popen([sys.executable, "-c", REMOTE_BOOTSTRAP_SOURCE, encoded],
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(2)]
            try:
                for observer in observers:
                    _stdout, stderr = observer.communicate(timeout=10)
                    self.assertEqual(observer.returncode, 0, stderr)
                self.wait_receipt(root, {"completed"})
                self.assertEqual((root / "count").read_text().splitlines(), ["launched"])
            finally:
                for observer in observers:
                    if observer.poll() is None:
                        observer.kill(); observer.communicate(timeout=5)
                self.cleanup_owned(root)


if __name__ == "__main__":
    unittest.main()
