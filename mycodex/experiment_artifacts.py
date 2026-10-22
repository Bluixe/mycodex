"""Immutable experiment provenance and conservative result evidence gates.

These helpers deliberately do not infer scientific support from a successful exit.
The execution policy is an outer-control-plane allowlist, not a shell sandbox.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shlex
import base64
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":"), default=str).encode()).hexdigest()


def validate_execution_id(execution_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", execution_id):
        raise ValueError("execution_id must be a safe identifier of at most 128 characters")
    return execution_id


def resolve_time_budget(remote: dict[str, Any]) -> float | None:
    """Accept numeric seconds and explicit durations; retain unbounded legacy prose."""
    value = next((remote[key] for key in ("time_budget_seconds", "max_runtime_seconds",
                 "timeout_seconds", "timeout", "budget") if remote.get(key) is not None), None)
    if isinstance(value, dict):
        value = value.get("seconds", value.get("max_seconds"))
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("remote time budget must be positive numeric seconds")
    if isinstance(value, str):
        match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(s|sec|seconds?|m|min|minutes?|h|hours?)?\s*", value, re.I)
        if not match:
            # Historical contracts use prose budgets. They remain explicit in the manifest.
            return None
        unit = (match.group(2) or "s").lower()
        value = float(match.group(1)) * (3600 if unit.startswith("h") else 60 if unit.startswith("m") else 1)
    try:
        seconds = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("remote time budget must be positive numeric seconds") from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("remote time budget must be positive numeric seconds")
    return seconds


def _safe_patch_path(value: str) -> str:
    normalized = value.replace("\\", "/")
    path = PurePosixPath(normalized)
    if (not normalized or normalized.startswith(("/", "~")) or ":" in normalized
            or ".." in path.parts or {".git", ".mycodex"} & {part.lower() for part in path.parts}
            or path == PurePosixPath(".")):
        raise ValueError(f"unsafe patch path: {value}")
    return str(path)


def patch_scope_failures(repo: Path, patch: dict[str, Any], policy: dict[str, Any] | None = None) -> list[str]:
    if not patch.get("required"):
        return []
    failures: list[str] = []
    try:
        declared = {_safe_patch_path(str(value)) for value in patch.get("files") or []}
        actual: set[str] = set()
        diff = str(patch.get("unified_diff") or patch.get("diff") or "")
        for line in diff.splitlines():
            if line.startswith(("--- ", "+++ ")):
                raw = line[4:].split("\t", 1)[0]
                if raw == "/dev/null":
                    continue
                if raw.startswith('"'):
                    # Git's quoted filenames require C-style decoding; fail closed instead.
                    raise ValueError("quoted patch paths are unsupported; use simple repository-relative paths")
                actual.add(_safe_patch_path(raw[2:] if raw.startswith(("a/", "b/")) else raw))
            elif line.startswith(("rename from ", "rename to ", "copy from ", "copy to ")):
                actual.add(_safe_patch_path(line.split(" ", 2)[2]))
        if not actual:
            failures.append("patch_scope_unverifiable")
        if actual - declared:
            failures.append("patch_outside_declared_files:" + ",".join(sorted(actual - declared)))
        root = repo.resolve()
        allowed_roots = (policy or {}).get("allowed_patch_roots")
        for name in declared | actual:
            resolved = (repo / name).resolve()
            if not resolved.is_relative_to(root):
                failures.append(f"patch_path_escapes_repository:{name}")
            if allowed_roots is not None and not any(
                    name == str(prefix).rstrip("/") or name.startswith(str(prefix).rstrip("/") + "/")
                    for prefix in allowed_roots):
                failures.append(f"patch_path_outside_policy:{name}")
    except (ValueError, TypeError) as exc:
        failures.append(str(exc))
    return failures


_DESTRUCTIVE = re.compile(r"(?:\brm\s+|\bgit\s+(?:reset\s+--hard|clean\s+-|push\s+.*--force)|"
                          r"\b(?:mkfs|shutdown|reboot)\b|\b(?:Remove-Item|rd|rmdir)\s+.*(?:-Recurse|/s)|"
                          r"\b(?:curl|wget)\b[^\n]*\|\s*(?:bash|sh)\b)", re.I)


def execution_policy_failures(contract: dict[str, Any], policy: dict[str, Any] | None = None) -> list[str]:
    if policy is not None and not isinstance(policy, dict):
        return ["policy_must_be_object"]
    policy = policy or {}
    failures: list[str] = []
    for key in ("allow_remote", "allow_local_checks", "allow_patch", "allow_destructive"):
        if key in policy and not isinstance(policy[key], bool):
            failures.append(f"policy_{key}_must_be_boolean")
    for key in ("allowed_execution_classes", "allowed_command_sha256", "allowed_local_check_sha256",
                "allowed_remote_refs", "allowed_patch_roots"):
        if key in policy and (not isinstance(policy[key], list) or not all(isinstance(x, str) for x in policy[key])):
            failures.append(f"policy_{key}_must_be_string_list")
    if failures:
        return failures
    remote = contract.get("remote") if isinstance(contract.get("remote"), dict) else {}
    command = str(remote.get("command") or "")
    checks = contract.get("local_checks") or []
    if command and policy.get("allow_remote") is False:
        failures.append("policy_remote_execution_disabled")
    if checks and policy.get("allow_local_checks") is False:
        failures.append("policy_local_checks_disabled")
    if (contract.get("patch") or {}).get("required") and policy.get("allow_patch") is False:
        failures.append("policy_patch_disabled")
    classes = policy.get("allowed_execution_classes")
    if classes is not None and contract.get("execution_class") not in classes:
        failures.append("policy_execution_class_not_allowed")
    for label, value in [("remote", command), *[("local_check", str(c)) for c in checks]]:
        if value and _DESTRUCTIVE.search(value) and not policy.get("allow_destructive"):
            failures.append(f"policy_destructive_{label}_command")
    allowed = policy.get("allowed_command_sha256")
    if command and allowed is not None and hashlib.sha256(command.encode()).hexdigest() not in allowed:
        failures.append("policy_remote_command_not_allowlisted")
    allowed_checks = policy.get("allowed_local_check_sha256")
    if allowed_checks is not None:
        for check in checks:
            if hashlib.sha256(str(check).encode()).hexdigest() not in allowed_checks:
                failures.append("policy_local_check_not_allowlisted")
    max_budget = policy.get("max_runtime_seconds")
    try:
        budget = resolve_time_budget(remote)
        if max_budget is not None and (budget is None or budget > float(max_budget)):
            failures.append("policy_time_budget_exceeded_or_unbounded")
    except ValueError:
        failures.append("invalid_remote_time_budget")
    return failures


def build_execution_manifest(*, execution_id: str, project_id: int, experiment_id: int,
                             remote_ref: str, commit: str | None, command: str,
                             env: dict[str, str], contract: dict[str, Any] | None = None,
                             protocol_task_id: int | None = None, time_budget_seconds: float | None = None,
                             termination_grace_seconds: float = 30,
                             artifact_refs: dict[str, Any] | None = None) -> dict[str, Any]:
    contract = contract or {}
    remote = contract.get("remote") if isinstance(contract.get("remote"), dict) else {}
    metadata = {key: contract.get(key, remote.get(key)) for key in
                ("seed", "seeds", "dataset", "data_version", "eval", "evaluation", "environment", "config")}
    # Keep identifiers and hashes; environment values and arbitrary config may contain credentials.
    return {
        "schema_version": 1, "execution_id": validate_execution_id(execution_id),
        "project_id": project_id, "experiment_id": experiment_id,
        "protocol_task_id": protocol_task_id, "remote_ref": remote_ref, "code_commit": commit,
        "execution_class": contract.get("execution_class", "manual"),
        "command_sha256": hashlib.sha256(command.encode()).hexdigest(),
        "environment_sha256": canonical_hash(env), "environment_keys": sorted(env),
        "config_sha256": canonical_hash(metadata.get("config")),
        "declared_environment_sha256": canonical_hash(metadata.get("environment")),
        "contract_sha256": canonical_hash(contract),
        "time_budget_seconds": time_budget_seconds, "termination_grace_seconds": termination_grace_seconds,
        "budget_enforced": time_budget_seconds is not None,
        "wandb": {key: value for key, value in (contract.get("wandb") or {}).items()
                  if key in {"entity", "project", "run_id", "run_name"}},
        "metadata": {key: value for key, value in metadata.items() if value is not None and key not in {"config", "environment"}},
        "artifact_refs": {"declared": contract.get("artifacts") or {}, **(artifact_refs or {})},
        "evidence_status": "execution_planned", "method_claim_allowed": False,
    }


def _write_immutable(path: Path, value: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(dir=path.parent, prefix=".artifact-", suffix=".tmp")
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
            stream.write(content); stream.flush(); os.fsync(stream.fileno())
        try:
            # Hard-link publication is atomic and never replaces an existing artifact.
            os.link(temporary, path)
        except FileExistsError:
            if path.read_text(encoding="utf-8") != content:
                raise ValueError(f"immutable experiment artifact conflicts with existing file: {path}")
    finally:
        temporary.unlink(missing_ok=True)
    return path


def persist_execution_manifest(artifact_root: Path, manifest: dict[str, Any]) -> Path:
    return _write_immutable(Path(artifact_root) / validate_execution_id(manifest["execution_id"]) / "manifest.json", manifest)


def read_execution_manifest(artifact_root: Path, execution_id: str) -> dict[str, Any]:
    return json.loads((Path(artifact_root) / validate_execution_id(execution_id) / "manifest.json").read_text(encoding="utf-8"))


def list_execution_manifests(artifact_root: Path, *, project_id: int | None = None,
                             experiment_id: int | None = None) -> list[dict[str, Any]]:
    results = []
    for path in sorted(Path(artifact_root).glob("*/manifest.json")):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if project_id is not None and manifest.get("project_id") != project_id:
            continue
        if experiment_id is not None and manifest.get("experiment_id") != experiment_id:
            continue
        results.append({**manifest, "manifest_path": str(path.resolve())})
    return results


def assess_result_evidence(manifest: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    reasons = []
    execution_class = manifest.get("execution_class", "manual")
    status = str(result.get("status") or "unknown")
    if status not in {"completed", "succeeded"} or result.get("exit_code") != 0:
        reasons.append("execution_not_verified_successful")
    if not result.get("receipt_verified"):
        reasons.append("remote_receipt_unverified")
    if not result.get("artifacts_verified") or not result.get("artifact_refs"):
        reasons.append("result_artifacts_not_verified")
    provenance = result.get("provenance") if isinstance(result.get("provenance"), dict) else {}
    if any(provenance.get(key) != expected for key, expected in {
            "execution_id": manifest["execution_id"], "code_commit": manifest.get("code_commit"),
            "command_sha256": manifest.get("command_sha256")}.items()):
        reasons.append("result_provenance_unbound")
    if execution_class.startswith("smoke") or execution_class in {"read-only-preflight", "monitor-only", "diagnostic-patch"}:
        reasons.append("execution_class_is_diagnostic")
    evaluation = result.get("evaluation") if isinstance(result.get("evaluation"), dict) else {}
    for key in ("protocol", "dataset", "baseline", "metrics"):
        if not evaluation.get(key):
            reasons.append(f"missing_evaluation_{key}")
    review = result.get("independent_review")
    if not isinstance(review, dict) or review.get("verdict") != "supports" or not review.get("review_ref") or not result.get("claim_refs"):
        reasons.append("method_claim_requires_independent_review_and_claim_refs")
    # A gate is a traceable assessment of supplied evidence, never an automatic novelty proof.
    allowed = not reasons
    return {"schema_version": 1, "execution_id": manifest["execution_id"],
            "evidence_level": "claim_candidate" if allowed else "smoke" if execution_class.startswith("smoke") else "triage",
            "method_claim_allowed": allowed, "reasons": reasons,
            "assessment_basis": "supplied_result_metadata; artifacts must be checked by the caller"}


def persist_execution_result(artifact_root: Path, manifest: dict[str, Any], result: dict[str, Any],
                             gate: dict[str, Any]) -> Path:
    digest = canonical_hash(result)[:16]
    return _write_immutable(Path(artifact_root) / validate_execution_id(manifest["execution_id"]) /
                            f"result-{digest}.json", {"result": result, "gate": gate})


# Executed only on the remote Linux host. Kept as plain source so it can also be
# compiled and exercised locally without an SSH connection.
REMOTE_PROCESS_IDENTITY_SOURCE = r'''
def observe_process_identity(pid, expected_start_ticks):
    try:
        if isinstance(pid, bool) or int(pid) <= 0: return "identity_unverified"
        pid = int(pid)
    except (TypeError, ValueError): return "identity_unverified"
    try:
        os.kill(pid, 0)
        stat = pathlib.Path("/proc/%s/stat" % pid).read_text().rsplit(")", 1)[1].split()
    except ProcessLookupError: return "absent"
    except FileNotFoundError: return "absent"
    except (OSError, IndexError): return "unknown"
    if len(stat) <= 19: return "unknown"
    if stat[0] == "Z": return "not_running"
    if expected_start_ticks is None: return "identity_unverified"
    if stat[19] != str(expected_start_ticks): return "identity_changed"
    return "running"
'''


REMOTE_RECEIPT_OBSERVATION_SOURCE = r'''
import base64, json, os, pathlib, sys
''' + REMOTE_PROCESS_IDENTITY_SOURCE + r'''
content = pathlib.Path(sys.argv[1]).read_bytes()
receipt = json.loads(content)
if not isinstance(receipt, dict): raise ValueError("receipt must be an object")
print("REMOTE_RECEIPT=" + base64.b64encode(content).decode())
print("REMOTE_SUPERVISOR_STATUS=" + observe_process_identity(receipt.get("supervisor_pid"), receipt.get("supervisor_start_ticks")))
print("REMOTE_CHILD_STATUS=" + observe_process_identity(receipt.get("pid"), receipt.get("pid_start_ticks")))
'''


def receipt_observation_status(receipt: dict[str, Any], observations: dict[str, str]) -> str:
    """Classify a receipt only after the caller verifies its manifest binding."""
    status = receipt.get("status")
    if status in {"failed", "timed_out"}:
        return status
    if status == "completed":
        return "completed" if type(receipt.get("exit_code")) is int and receipt["exit_code"] == 0 else "needs_reconciliation"
    if status == "running" and all(observations.get(key) == "running" for key in (
            "REMOTE_SUPERVISOR_STATUS", "REMOTE_CHILD_STATUS")):
        return "running"
    return "needs_reconciliation"


REMOTE_SUPERVISOR_SOURCE = r'''
import hashlib, json, os, pathlib, signal, subprocess, sys, time
signal.signal(signal.SIGHUP, signal.SIG_IGN)
root = pathlib.Path(sys.argv[1])
payload = json.loads((root / "launch.json").read_text())
# A restarted supervisor cannot safely adopt another process' child/wait state.
# Keep the claim after both success and failure; the execution id is immutable.
try: (root / "supervisor.claim").mkdir()
except FileExistsError:
    raise RuntimeError("supervisor already claimed; explicit reconciliation required, duplicate workload refused")
receipt = {"schema_version": 1, "execution_id": payload["execution_id"],
    "manifest_sha256": payload["manifest_sha256"], "remote_commit": payload["remote_commit"],
    "log_path": payload["log_path"], "pid_path": payload["pid_path"],
    "remote_worktree_status_sha256": payload.get("remote_worktree_status_sha256"),
    "remote_python_version": sys.version,
    "status": "starting", "supervisor_pid": os.getpid(), "started_at": time.time()}
def save():
    tmp = root / ("receipt." + str(os.getpid()) + ".tmp")
    with tmp.open("w") as stream:
        json.dump(receipt, stream, sort_keys=True); stream.flush(); os.fsync(stream.fileno())
    os.replace(tmp, root / "receipt.json")
def start_ticks(pid):
    try: return pathlib.Path("/proc/%s/stat" % pid).read_text().rsplit(")", 1)[1].split()[19]
    except (OSError, IndexError): return None
receipt["supervisor_start_ticks"] = start_ticks(os.getpid())
save()
try:
    env = os.environ.copy(); env.update(payload["env"])
    receipt["observed_environment_sha256"] = hashlib.sha256(json.dumps(env, sort_keys=True).encode()).hexdigest()
    with open(payload["log_path"], "wb") as log:
        child = subprocess.Popen(["bash", "-lc", payload["command"]], cwd=payload["workdir"],
            env=env, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, start_new_session=True)
        receipt.update(status="running", pid=child.pid, pid_start_ticks=start_ticks(child.pid))
        pathlib.Path(payload["pid_path"]).write_text(str(child.pid) + "\n")
        save()
        budget = payload.get("time_budget_seconds")
        grace = payload.get("termination_grace_seconds", 30)
        try:
            code = child.wait(timeout=budget)
            receipt.update(status="completed" if code == 0 else "failed", exit_code=code)
        except subprocess.TimeoutExpired:
            receipt.update(status="stopping", stop_reason="time_budget_exceeded"); save()
            # The workload receives TERM first so its own handler can save a checkpoint.
            # No checkpoint is claimed unless the workload produces and verifies one.
            try: os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError: pass
            stop_deadline = time.monotonic() + grace
            try: child.wait(timeout=grace)
            except subprocess.TimeoutExpired: pass
            # A shell may exit before its descendants. Keep the grace period for
            # remaining members, then kill the original process group as well.
            while time.monotonic() < stop_deadline:
                try: os.killpg(child.pid, 0)
                except ProcessLookupError: break
                time.sleep(min(0.05, max(0, stop_deadline - time.monotonic())))
            try: os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError: pass
            child.wait()
            receipt.update(status="timed_out", exit_code=124, child_exit_code=child.returncode)
    receipt["finished_at"] = time.time(); save()
except BaseException as exc:
    receipt.update(status="failed", error=str(exc), finished_at=time.time()); save(); raise
'''


REMOTE_BOOTSTRAP_SOURCE = r'''
import base64, hashlib, json, os, pathlib, subprocess, sys, time
''' + REMOTE_PROCESS_IDENTITY_SOURCE + r'''
payload = json.loads(base64.b64decode(sys.argv[1]))
os.umask(0o077)
workdir = pathlib.Path(payload["workdir"]).resolve()
root = workdir / ".mycodex" / "executions" / payload["execution_id"]
if root.is_symlink() or os.path.commonpath([str(workdir), str(root.resolve())]) != str(workdir):
    raise RuntimeError("execution claim path escapes the remote repository")
root.parent.mkdir(parents=True, exist_ok=True)
reused = False
try: root.mkdir()
except FileExistsError: reused = True
guard = root.parent.parent / "checkout.lock"
guard_owned = False
def release_guard():
    global guard_owned
    if guard_owned:
        guard.rmdir(); guard_owned = False
def guard_shared_checkout():
    global guard_owned
    try: guard.mkdir(); guard_owned = True
    except FileExistsError:
        raise RuntimeError("repository checkout lock is active or stale; reconcile before a different execution")
    for other in root.parent.iterdir():
        if other == root or not other.is_dir(): continue
        try: previous = json.loads((other / "receipt.json").read_text())
        except (OSError, ValueError):
            raise RuntimeError("another execution claim has no reliable receipt: " + other.name)
        if previous.get("status") not in ("completed", "failed", "timed_out"):
            raise RuntimeError("shared repository has an active or ambiguous execution: " + other.name)
        # A failed supervisor must not allow checkout while its original child lives.
        if previous.get("pid"):
            identity = observe_process_identity(previous["pid"], previous.get("pid_start_ticks"))
            if identity not in ("absent", "not_running", "identity_changed"):
                raise RuntimeError("previous execution still owns a live process: " + other.name)
def emit(receipt):
    if receipt.get("manifest_sha256") != payload["manifest_sha256"]:
        raise RuntimeError("execution id already belongs to a different immutable manifest")
    status = receipt.get("status")
    if status in ("failed", "timed_out", "stopping", "unknown"):
        raise RuntimeError("previous execution is " + str(status) + "; inspect receipt, never retry with a new spawn implicitly")
    if status == "running":
        # A live workload without its supervisor has lost budget enforcement.
        # Observe the ambiguity without restarting or killing user work here.
        if not receipt.get("supervisor_pid") or not receipt.get("supervisor_start_ticks"):
            raise RuntimeError("running receipt lacks a verified supervisor identity; reconciliation required")
        if observe_process_identity(receipt["supervisor_pid"], receipt["supervisor_start_ticks"]) != "running":
            raise RuntimeError("original supervisor is absent; budget enforcement unknown, reconciliation required")
        if observe_process_identity(receipt.get("pid"), receipt.get("pid_start_ticks")) != "running":
            raise RuntimeError("receipt says running but original process is absent; reconciliation required")
    if status not in ("running", "completed"):
        return False
    if status == "completed" and receipt.get("exit_code") != 0:
        raise RuntimeError("completed receipt lacks a successful exit code")
    for key, value in {"REMOTE_COMMIT": receipt["remote_commit"], "REMOTE_PID": receipt["pid"],
        "REMOTE_LOG": receipt["log_path"], "REMOTE_PID_FILE": receipt["pid_path"],
        "REMOTE_EXECUTION_ID": payload["execution_id"], "REMOTE_EXECUTION_STATUS": status,
        "REMOTE_RECEIPT_PATH": str(root / "receipt.json"), "REMOTE_REUSED": "1" if reused else "0",
        "REMOTE_RECEIPT": base64.b64encode(json.dumps(receipt).encode()).decode()}.items():
        print(str(key) + "=" + str(value))
    return True
if not reused:
    try:
        guard_shared_checkout()
        (root / "manifest.json").write_text(json.dumps(payload["manifest"], sort_keys=True))
        # Existing server edits are user-owned and must not silently change a run's code.
        subprocess.run(["git", "diff", "--quiet"], cwd=workdir, check=True)
        subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=workdir, check=True)
        if payload["fetch"]:
            subprocess.run(["git", "fetch", "--all", "--prune"], cwd=workdir, check=True, stdout=sys.stderr)
        if payload["commit"]:
            resolved_commit = subprocess.check_output(["git", "rev-parse", "--verify", "--end-of-options",
                payload["commit"] + "^{commit}"], cwd=workdir, text=True).strip()
            subprocess.run(["git", "checkout", "--detach", resolved_commit], cwd=workdir, check=True, stdout=sys.stderr)
        elif payload["branch"]:
            subprocess.run(["git", "check-ref-format", "--branch", payload["branch"]], cwd=workdir, check=True, stdout=sys.stderr)
            subprocess.run(["git", "checkout", payload["branch"]], cwd=workdir, check=True, stdout=sys.stderr)
            subprocess.run(["git", "pull", "--ff-only"], cwd=workdir, check=True, stdout=sys.stderr)
        payload["remote_commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=workdir, text=True).strip()
        worktree_status = subprocess.check_output(["git", "status", "--porcelain"], cwd=workdir, text=True)
        payload["remote_worktree_status_sha256"] = hashlib.sha256(worktree_status.encode()).hexdigest()
        for key in ("log_path", "pid_path"):
            path = pathlib.Path(payload[key])
            if not path.is_absolute(): path = workdir / path
            path.parent.mkdir(parents=True, exist_ok=True); payload[key] = str(path)
        (root / "launch.json").write_text(json.dumps(payload))
        os.chmod(root / "launch.json", 0o600)
        (root / "supervisor.py").write_text(base64.b64decode(payload.pop("supervisor_source")).decode())
        with (root / "supervisor.log").open("ab") as log:
            subprocess.Popen([sys.executable, str(root / "supervisor.py"), str(root)], cwd=workdir,
                stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
    except BaseException as exc:
        # A failed claim stays claimed. Explicit investigation is required before another id.
        (root / "receipt.json").write_text(json.dumps({"execution_id": payload["execution_id"],
            "manifest_sha256": payload["manifest_sha256"], "status": "failed", "error": str(exc)}))
        release_guard()
        raise
try:
    for _ in range(100):
        try:
            receipt = json.loads((root / "receipt.json").read_text())
            if emit(receipt): break
        except FileNotFoundError: pass
        time.sleep(0.05)
    else:
        raise RuntimeError("execution claim exists without a launch receipt; state is ambiguous, duplicate launch refused")
finally:
    release_guard()
'''


def render_remote_launch_script(*, workdir: str, execution_id: str, command: str,
                                commit: str | None, branch: str | None, fetch: bool,
                                log_path: str, pid_path: str, env: dict[str, str],
                                manifest: dict[str, Any], time_budget_seconds: float | None,
                                termination_grace_seconds: float) -> str:
    payload = {"workdir": workdir, "execution_id": validate_execution_id(execution_id),
               "command": command, "commit": commit, "branch": branch, "fetch": fetch,
               "log_path": log_path, "pid_path": pid_path, "env": env, "manifest": manifest,
               "manifest_sha256": canonical_hash(manifest), "time_budget_seconds": time_budget_seconds,
               "termination_grace_seconds": termination_grace_seconds,
               "supervisor_source": base64.b64encode(REMOTE_SUPERVISOR_SOURCE.encode()).decode()}
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    # Command stays in JSON, never interpolated into the bootstrap shell.
    return "set -euo pipefail\numask 077\n" + "python3 -c " + shlex.quote(REMOTE_BOOTSTRAP_SOURCE) + " " + shlex.quote(encoded) + "\n"


def render_bounded_command(command: str, seconds: float, grace_seconds: float = 30,
                           env: dict[str, str] | None = None) -> str:
    """Remote monitor/preflight bound independent of the local SSH connection timeout."""
    source = r'''
import base64, json, os, signal, subprocess, sys, time
signal.signal(signal.SIGHUP, signal.SIG_IGN)
payload = json.loads(base64.b64decode(sys.argv[1]))
env = os.environ.copy(); env.update(payload["env"])
child = subprocess.Popen(["bash", "-lc", payload["command"]], start_new_session=True, env=env)
try: sys.exit(child.wait(timeout=payload["seconds"]))
except subprocess.TimeoutExpired:
    try: os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError: pass
    deadline = time.monotonic() + payload["grace"]
    try: child.wait(timeout=payload["grace"])
    except subprocess.TimeoutExpired: pass
    while time.monotonic() < deadline:
        try: os.killpg(child.pid, 0)
        except ProcessLookupError: break
        time.sleep(min(0.05, max(0, deadline - time.monotonic())))
    try: os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError: pass
    child.wait()
    sys.exit(124)
'''
    encoded = base64.b64encode(json.dumps({"command": command, "seconds": seconds, "grace": grace_seconds,
                                         "env": env or {}}).encode()).decode()
    return "python3 -c " + shlex.quote(source) + " " + shlex.quote(encoded)
