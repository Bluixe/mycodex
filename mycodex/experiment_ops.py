from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import re
import shlex
import shutil
import signal
import subprocess
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable

from .db import EXPERIMENT_TASK_TERMINAL_STATUSES, Store
from .contracts import (
    ContractValidationError, PEER_REVIEW_CONTRACT_V1,
    parse_contract, validate_contract, validate_execution_contract,
)
from .experiment_artifacts import (
    assess_result_evidence, build_execution_manifest, canonical_hash,
    execution_policy_failures, patch_scope_failures, persist_execution_manifest,
    persist_execution_result, read_execution_manifest, render_bounded_command, render_remote_launch_script,
    resolve_time_budget, validate_execution_id, REMOTE_RECEIPT_OBSERVATION_SOURCE, receipt_observation_status,
)
from . import experiment_protocol as _protocol
from .experiment_policy import (
    DEFAULT_PEER_REVIEW_BYPASS_ALLOWED_CLASSES,
    DEFAULT_PEER_REVIEW_BYPASS_ON,
    EVAL_EXECUTION_CLASSES,
    LOW_RISK_EXECUTION_CLASSES,
    PEER_REVIEW_EXEMPT_EXECUTION_CLASSES,
    PLACEHOLDER_WANDB_RUN_IDS,
    REMOTE_COMMAND_EXECUTION_CLASSES,
    RESEARCH_REQUIRED_DECISION_KEYS,
    RESEARCH_REQUIRED_EXECUTION_CLASSES,
    RESEARCH_REQUIRED_LIST_FIELDS,
    RESEARCH_REQUIRED_SCALAR_FIELDS,
    WANDB_PRODUCING_EXECUTION_CLASSES,
    _JSON_BLOCK_RE,
    extract_execution_contract,
    _execution_contract_parse_failure,
    extract_peer_review_decision,
    _normalize_policy_tokens,
    _peer_review_result_status,
    _review_decision_text,
    _peer_review_has_required_changes,
    _execution_class,
    _contract_skips_peer_review,
    _phase_for_contract_execution,
    _review_text,
    _peer_review_hard_blockers,
    _peer_review_soft_objections,
    _annotate_contract_with_reviewer_objections,
    _remote_eval_command_looks_destructive,
    _contract_bypass_failures,
    _validate_experiment_contract_types,
    _has_contract_content,
    _contract_research_failures,
    _peer_review_blocked_since,
    _assess_peer_review_bypass,
    _contract_execution_failures,
    _contract_summary,
    _contract_produces_wandb_run,
    _contract_monitors_wandb_run,
    _is_real_wandb_run_id,
    _contract_execution_fingerprint,
    _event_payload,
)
from .experiment_protocol import (
    _contract_task_idempotency_key,
    _contract_protocol_readiness,
    record_execution_contract_task,
    _protocol_task_outcome,
    _lease_recorded_protocol_task,
    _record_protocol_lease_loss,
    _complete_recorded_protocol_task,
    recover_stale_experiment_tasks,
    _task_has_remote_launch_event,
    _payload_mentions_protocol_task,
    _require_experiment,
)

from .service import (
    require_project,
    start_experiment_contract_repair_turn,
    start_experiment_control_turn,
    start_experiment_peer_review_turn,
)


@dataclass(frozen=True)
class RemoteTarget:
    login: str
    path: str
    port: int | None = None

    @property
    def ref(self) -> str:
        port = f":{self.port}" if self.port is not None else ""
        return f"{self.login}{port}:{self.path}"


@dataclass(frozen=True)
class RemoteExecResult:
    stdout: str
    stderr: str = ""
    returncode: int = 0


@dataclass(frozen=True)
class WandbRunRef:
    entity: str
    project: str
    run_id: str

    @property
    def path(self) -> str:
        return f"{self.entity}/{self.project}/{self.run_id}"


class RemoteCommandError(RuntimeError):
    def __init__(self, message: str, result: RemoteExecResult | None = None) -> None:
        super().__init__(message)
        self.result = result


RemoteRunner = Callable[[RemoteTarget, str, str | None, int], RemoteExecResult]
WandbFetcher = Callable[[WandbRunRef, list[str], int], dict[str, Any]]


_REMOTE_REF_RE = re.compile(r"^(?P<login>[^:]+?)(?::(?P<port>\d+))?:(?P<path>/.*)$")
_COND_RE = re.compile(
    r"^\s*(?P<metric>[A-Za-z0-9_./:-]+)\s*"
    r"(?P<op>>=|<=|==|!=|>|<)\s*"
    r"(?P<value>-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*$"
)
_PASSWORD_PROMPT_RE = re.compile(r"^[^\r\n]*password:\s*[\r\n]*", re.IGNORECASE | re.MULTILINE)
_REMOTE_FAILURE_HINT_RE = re.compile(
    r"(Traceback|SyntaxError|unexpected EOF|CUDA out of memory|RuntimeError|"
    r"command not found|No such file or directory|Killed)",
    re.IGNORECASE,
)

SAFE_FALLBACK_EXECUTION_CLASSES = {
    "read-only-preflight",
    "monitor-only",
    "smoke-eval",
    "eval-only",
}
MAX_CONTRACT_PARSE_REPAIR_ATTEMPTS = 2
WATCHDOG_ACTIVITY_EVENTS = {
    "agent_message_delta",
    "codex_event",
    "codex_tool",
    "control_contract_parsed",
    "control_contract_parse_failed",
    "control_turn_completed",
    "control_turn_failed",
    "control_turn_started",
    "contract_repair_scheduled",
    "contract_repair_completed",
    "contract_repair_failed",
    "contract_repair_started",
    "decision",
    "failed",
    "metric",
    "peer_review_decision",
    "peer_review_skipped",
    "peer_review_soft_objections_recorded",
    "paused_with_reviewer_objections",
    "dirty_worktree_ignored_for_eval",
    "remote_job_exited_early",
    "remote_job_started",
    "remote_job_status",
    "remote_monitor_command_completed",
    "remote_monitor_command_failed",
    "remote_monitor_command_started",
    "supervisor_action_completed",
    "supervisor_heartbeat",
    "supervisor_retry_scheduled",
    "supervisor_started",
    "supervisor_stopped",
    "supervisor_terminal_detected",
    "supervisor_warning",
    "wandb_poll_started",
    "wandb_run_finished",
}
REMOTE_STATE_EVENT_TYPES = {
    "remote_heartbeat",
    "remote_event",
    "remote_anomaly",
    "remote_anomaly_control_completed",
    "remote_anomaly_control_failed",
    "remote_anomaly_control_started",
    "remote_sync_failed",
}
LOCAL_GATE_CONTROL_EVENT_TYPES = {
    "result_gate_parsed",
}
LOCAL_GATE_FOLLOWUP_EVENT_TYPES = {
    "control_contract_parsed",
    "remote_anomaly_control_started",
    "remote_job_started",
    "remote_monitor_command_started",
    "watchdog_supervise_started",
}
REMOTE_STATE_HINT_EVENT_TYPES = (
    "controller_started",
    "controller_recovered",
    "heldout_eval_started",
    "heldout_eval_restarted",
    "remote_job_started",
    "remote_heartbeat",
)
REMOTE_FAILURE_HINT_RE = re.compile(
    r"(Traceback|SyntaxError|RuntimeError|CUDA out of memory|out of memory|"
    r"wandb\.errors|CommError|command not found|No such file or directory|"
    r"unexpected EOF|Killed|\bnan\b|\binf\b)",
    re.IGNORECASE,
)


def parse_remote_ref(remote_ref: str) -> RemoteTarget:
    remote_ref = remote_ref.strip()
    match = _REMOTE_REF_RE.match(remote_ref)
    if not match:
        raise ValueError(
            "remote ref must look like user@host:/abs/path or user@host:port:/abs/path"
        )
    port = match.group("port")
    return RemoteTarget(
        login=match.group("login"),
        port=int(port) if port else None,
        path=match.group("path"),
    )


def parse_wandb_run_path(run_path: str) -> WandbRunRef:
    parts = [part for part in run_path.strip().split("/") if part]
    if len(parts) != 3:
        raise ValueError("W&B run path must look like entity/project/run_id")
    return WandbRunRef(entity=parts[0], project=parts[1], run_id=parts[2])


def git_commit_push(
    store: Store,
    project_name: str,
    experiment_name: str,
    *,
    commit_message: str | None = None,
    all_changes: bool = False,
    paths: list[str] | None = None,
    remote: str = "origin",
    branch: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    project = require_project(store, project_name)
    experiment = _require_experiment(store, project["id"], experiment_name)
    repo = Path(project["path"])
    _ensure_git_repo(repo)

    current_branch = branch or _git_text(repo, ["rev-parse", "--abbrev-ref", "HEAD"])
    if current_branch == "HEAD":
        raise ValueError("local repository is detached; provide --branch explicitly")
    head_before = _git_text(repo, ["rev-parse", "HEAD"])
    status_lines = _git_text(repo, ["status", "--short"]).splitlines()
    store.add_experiment_event(
        experiment["id"],
        event_type="git_checked",
        level="info",
        message=f"Local git checked at {head_before[:12]}; dirty={bool(status_lines)}.",
        payload={"head": head_before, "branch": current_branch, "dirty": bool(status_lines), "status": status_lines},
        source="mycodex",
        status="running",
    )

    selected_paths = paths or []
    if status_lines:
        if not commit_message:
            store.add_experiment_event(
                experiment["id"],
                event_type="blocked",
                level="warning",
                message="Local repository has uncommitted changes; provide a commit message before pushing.",
                payload={"status": status_lines},
                source="mycodex",
                phase="waiting_for_commit",
            )
            raise ValueError("local repository is dirty; provide --message to commit before push")
        if not all_changes and not selected_paths:
            store.add_experiment_event(
                experiment["id"],
                event_type="blocked",
                level="warning",
                message="Dirty local repository needs --all or at least one --path to define the commit scope.",
                payload={"status": status_lines},
                source="mycodex",
                phase="waiting_for_commit",
            )
            raise ValueError("refusing to commit without --all or --path")
        if not all_changes:
            staged_paths = set(_git_text(repo, ["diff", "--cached", "--name-only", "-z"]).split("\0")) - {""}
            unrelated_staged = sorted(staged_paths - set(selected_paths))
            if unrelated_staged:
                raise ValueError("refusing to commit unrelated staged files: " + ", ".join(unrelated_staged))

        store.add_experiment_event(
            experiment["id"],
            event_type="code_change_started",
            level="info",
            message="Preparing local code commit for experiment launch.",
            payload={"all": all_changes, "paths": selected_paths},
            source="mycodex",
        )
        if not dry_run:
            if all_changes:
                _git_text(repo, ["add", "-A"])
            else:
                _git_text(repo, ["add", "--", *selected_paths])
            _git_text(repo, ["commit", "-m", commit_message])
        head_after = head_before if dry_run else _git_text(repo, ["rev-parse", "HEAD"])
        store.add_experiment_event(
            experiment["id"],
            event_type="code_committed",
            level="info",
            message=f"Local experiment code committed at {head_after[:12]}.",
            payload={"commit": head_after, "dry_run": dry_run},
            source="mycodex",
        )
    else:
        head_after = head_before

    if dry_run:
        store.add_experiment_event(
            experiment["id"],
            event_type="code_push_dry_run",
            level="info",
            message=f"Would push {head_after[:12]} to {remote}/{current_branch}.",
            payload={"commit": head_after, "remote": remote, "branch": current_branch},
            source="mycodex",
        )
        return {"commit": head_after, "branch": current_branch, "remote": remote, "dry_run": True}

    store.add_experiment_event(
        experiment["id"],
        event_type="code_push_started",
        level="info",
        message=f"Pushing {head_after[:12]} to {remote}/{current_branch}.",
        payload={"commit": head_after, "remote": remote, "branch": current_branch},
        source="mycodex",
    )
    _git_text(repo, ["push", remote, f"HEAD:{current_branch}"])
    store.add_experiment_event(
        experiment["id"],
        event_type="code_pushed",
        level="info",
        message=f"Local commit {head_after[:12]} pushed to {remote}/{current_branch}.",
        payload={"commit": head_after, "remote": remote, "branch": current_branch},
        source="mycodex",
    )
    return {"commit": head_after, "branch": current_branch, "remote": remote, "dry_run": False}


def launch_remote_job(
    store: Store,
    project_name: str,
    experiment_name: str,
    *,
    command: str,
    remote_ref: str | None = None,
    ssh_key: str | None = None,
    ssh_password_file: str | None = None,
    commit: str | None = None,
    branch: str | None = None,
    require_clean: bool = True,
    fetch: bool = True,
    log_path: str | None = None,
    pid_path: str | None = None,
    env: dict[str, Any] | None = None,
    wandb_entity: str | None = None,
    wandb_project: str | None = None,
    wandb_run_id: str | None = None,
    wandb_run_name: str | None = None,
    timeout: int = 120,
    dry_run: bool = False,
    post_launch_check_delay: float = 2.0,
    post_launch_check_lines: int = 80,
    remote_runner: RemoteRunner | None = None,
    execution_id: str | None = None,
    execution_contract: dict[str, Any] | None = None,
    protocol_task_id: int | None = None,
    time_budget_seconds: float | None = None,
    termination_grace_seconds: float = 30,
) -> dict[str, Any]:
    command = command.strip()
    if not command:
        raise ValueError("remote launch command is required")
    project = require_project(store, project_name)
    experiment = _require_experiment(store, project["id"], experiment_name)
    config = _experiment_config(experiment)
    resolved_remote_ref = remote_ref or experiment["remote_ref"] or config.get("remote_ref")
    if not resolved_remote_ref:
        raise ValueError("remote_ref is not configured for this experiment")
    target = parse_remote_ref(str(resolved_remote_ref))
    resolved_ssh_key = ssh_key or config.get("ssh_key") or os.environ.get("MYCODEX_SSH_KEY")
    resolved_ssh_password_file = (
        ssh_password_file
        or config.get("ssh_password_file")
        or os.environ.get("MYCODEX_SSH_PASSWORD_FILE")
    )
    repo = Path(project["path"])

    local_status: list[str] = []
    local_head: str | None = None
    if commit is None:
        _ensure_git_repo(repo)
        local_head = _git_text(repo, ["rev-parse", "HEAD"])
        commit = local_head
    if require_clean:
        _ensure_git_repo(repo)
        local_status = _git_text(repo, ["status", "--short"]).splitlines()
        if local_status:
            store.add_experiment_event(
                experiment["id"],
                event_type="blocked",
                level="warning",
                message="Local repository is dirty; commit and push before launching the remote job.",
                payload={"status": local_status},
                source="mycodex",
                phase="waiting_for_commit",
            )
            raise ValueError("local repository is dirty; commit and push before launching remote job")

    resolved_env = _validate_env(env or {})
    contract = execution_contract or {"execution_class": "manual", "remote": {"command": command}, "wandb": {
        "entity": wandb_entity, "project": wandb_project, "run_id": wandb_run_id, "run_name": wandb_run_name}}
    policy = config.get("execution_policy", {})
    if policy is None:
        policy = {}
    policy_contract = {**contract, "remote": {**(contract.get("remote") or {}), "command": command}}
    if time_budget_seconds is not None:
        policy_contract["remote"]["time_budget_seconds"] = time_budget_seconds
    policy_failures = execution_policy_failures(policy_contract, policy)
    if policy_failures:
        raise ValueError("external execution policy refused launch: " + ", ".join(policy_failures))
    if policy.get("allowed_remote_refs") is not None and target.ref not in policy["allowed_remote_refs"]:
        raise ValueError("external execution policy refused remote target")
    if time_budget_seconds is None:
        time_budget_seconds = resolve_time_budget(policy_contract["remote"])
    if not math.isfinite(float(termination_grace_seconds)) or termination_grace_seconds < 0:
        raise ValueError("termination_grace_seconds must be finite and nonnegative")
    identity = {"project_id": project["id"], "experiment_id": experiment["id"], "remote_ref": target.ref,
                "commit": commit, "command": command, "env": resolved_env, "protocol_task_id": protocol_task_id,
                "contract": contract, "budget": time_budget_seconds, "grace": termination_grace_seconds}
    resolved_execution_id = validate_execution_id(execution_id or "exec-" + canonical_hash(identity)[:32])
    resolved_log_path = log_path or f"logs/mycodex/{resolved_execution_id}.log"
    resolved_pid_path = pid_path or f"{resolved_log_path}.pid"
    manifest = build_execution_manifest(
        execution_id=resolved_execution_id, project_id=project["id"], experiment_id=experiment["id"],
        remote_ref=target.ref, commit=commit, command=command, env=resolved_env, contract=contract,
        protocol_task_id=protocol_task_id, time_budget_seconds=time_budget_seconds,
        termination_grace_seconds=termination_grace_seconds,
        artifact_refs={"log_path": resolved_log_path, "pid_path": resolved_pid_path})
    script = _remote_launch_script(
        target=target,
        command=command,
        commit=commit,
        branch=branch,
        fetch=fetch,
        log_path=resolved_log_path,
        pid_path=resolved_pid_path,
        env=resolved_env,
        execution_id=resolved_execution_id,
        manifest=manifest,
        time_budget_seconds=time_budget_seconds,
        termination_grace_seconds=termination_grace_seconds,
    )

    payload_base = {
        "remote_ref": target.ref,
        "command": command,
        "commit": commit,
        "branch": branch,
        "fetch": fetch,
        "log_path": resolved_log_path,
        "pid_path": resolved_pid_path,
        "local_head": local_head,
        "local_status": local_status,
        "wandb_entity": wandb_entity,
        "wandb_project": wandb_project,
        "wandb_run_id": wandb_run_id,
        "wandb_run_name": wandb_run_name,
        "ssh_auth": "password_file" if resolved_ssh_password_file else ("key" if resolved_ssh_key else "default"),
        "execution_id": resolved_execution_id,
        "protocol_task_id": protocol_task_id,
        "manifest_sha256": canonical_hash(manifest),
        "time_budget_seconds": time_budget_seconds,
        "budget_enforced": time_budget_seconds is not None,
    }

    if dry_run:
        store.add_experiment_event(
            experiment["id"],
            event_type="remote_launch_dry_run",
            level="info",
            message=f"Would launch remote job on {target.ref}.",
            payload={**payload_base, "script_sha256": hashlib.sha256(script.encode()).hexdigest(), "manifest": manifest},
            source="mycodex",
            status="running",
            phase="waiting_for_experiment_start",
        )
        return {**payload_base, "script": script, "manifest": manifest, "dry_run": True}

    manifest_path = persist_execution_manifest(store.path.parent / "experiment_artifacts", manifest)
    payload_base["manifest_path"] = str(manifest_path.resolve())
    store.add_experiment_event(
        experiment["id"], event_type="execution_manifest_saved", level="info",
        message=f"Saved immutable execution manifest for {resolved_execution_id}.",
        payload={**payload_base, "manifest": manifest}, source="mycodex")

    store.add_experiment_event(
        experiment["id"],
        event_type="remote_pull_started",
        level="info",
        message=f"Syncing remote repository to {commit[:12] if commit else branch or 'current checkout'}.",
        payload=payload_base,
        source="mycodex",
        status="running",
    )
    runner = remote_runner or run_remote_script
    try:
        if remote_runner:
            result = runner(target, script, str(resolved_ssh_key) if resolved_ssh_key else None, timeout)
        else:
            result = run_remote_script(
                target,
                script,
                str(resolved_ssh_key) if resolved_ssh_key else None,
                timeout,
                ssh_password_file=str(resolved_ssh_password_file) if resolved_ssh_password_file else None,
            )
    except Exception as exc:
        store.add_experiment_event(
            experiment["id"],
            event_type="failed",
            level="error",
            message=f"Remote job launch failed: {exc}",
            payload=payload_base,
            source="mycodex",
            status="failed",
        )
        raise

    parsed = _parse_key_values(result.stdout)
    receipt: dict[str, Any] = {}
    if result.returncode != 0:
        raise RemoteCommandError("remote launcher returned a failure exit status", result)
    receipt_verified = False
    if parsed.get("REMOTE_RECEIPT"):
        try:
            receipt = json.loads(base64.b64decode(parsed["REMOTE_RECEIPT"]))
            if not isinstance(receipt, dict):
                raise ValueError("receipt must be an object")
            receipt_verified = (receipt.get("execution_id") == resolved_execution_id
                                and receipt.get("manifest_sha256") == canonical_hash(manifest)
                                and receipt.get("status") in {"running", "completed"}
                                and bool(receipt.get("pid"))
                                and str(receipt.get("pid")) == parsed.get("REMOTE_PID")
                                and (receipt.get("status") != "completed" or
                                     (type(receipt.get("exit_code")) is int and receipt["exit_code"] == 0)))
        except (ValueError, TypeError, KeyError):
            receipt_verified = False
    if not receipt_verified and (remote_runner is None or parsed.get("REMOTE_RECEIPT")):
        store.add_experiment_event(
            experiment["id"], event_type="remote_launch_receipt_invalid", level="error",
            message="Remote launch receipt is missing, invalid, or failed; reconcile before retry.",
            payload={**payload_base, "receipt": receipt, "stdout": result.stdout, "stderr": result.stderr},
            source="mycodex", status="failed")
        raise RemoteCommandError("remote launch lacks a valid durable receipt", result)
    remote_commit = parsed.get("REMOTE_COMMIT") or commit or ""
    remote_pid = parsed.get("REMOTE_PID")
    remote_log = parsed.get("REMOTE_LOG") or resolved_log_path
    remote_pid_path = parsed.get("REMOTE_PID_FILE") or resolved_pid_path
    if not remote_pid:
        store.add_experiment_event(
            experiment["id"],
            event_type="failed",
            level="error",
            message="Remote job launch did not return a PID.",
            payload={**payload_base, "stdout": result.stdout, "stderr": result.stderr},
            source="mycodex",
            status="failed",
        )
        raise RemoteCommandError("remote job launch did not return REMOTE_PID", result)

    store.add_experiment_event(
        experiment["id"],
        event_type="remote_pull_completed",
        level="info",
        message=f"Remote repository synced at {remote_commit[:12] if remote_commit else 'unknown commit'}.",
        payload={**payload_base, "remote_commit": remote_commit, "stdout": result.stdout},
        source="mycodex",
    )
    job_payload = {
        **payload_base,
        "remote_commit": remote_commit,
        "remote_pid": remote_pid,
        "log_path": remote_log,
        "pid_path": remote_pid_path,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "receipt": receipt,
        "receipt_verified": receipt_verified,
        "receipt_path": parsed.get("REMOTE_RECEIPT_PATH"),
        "execution_status": receipt.get("status") or "unverified",
        "reused": parsed.get("REMOTE_REUSED") == "1",
    }
    store.add_experiment_event(
        experiment["id"],
        event_type="remote_job_started",
        level="info",
        message=f"Remote job started: pid={remote_pid}, log={remote_log}.",
        payload=job_payload,
        source="mycodex",
        status="running",
    )
    post_launch_check: dict[str, Any] | None = None
    if post_launch_check_delay >= 0 and receipt.get("status") != "completed":
        post_launch_check = _post_launch_status_check(
            target,
            remote_pid,
            remote_log,
            ssh_key=str(resolved_ssh_key) if resolved_ssh_key else None,
            ssh_password_file=str(resolved_ssh_password_file) if resolved_ssh_password_file else None,
            timeout=timeout,
            delay=post_launch_check_delay,
            lines=post_launch_check_lines,
            runner=remote_runner,
            receipt_path=str(job_payload.get("receipt_path") or ""),
            execution_id=resolved_execution_id,
            manifest_sha256=canonical_hash(manifest),
        )
        if post_launch_check.get("check_failed"):
            store.add_experiment_event(
                experiment["id"],
                event_type="remote_post_launch_check_failed",
                level="warning",
                message=f"Post-launch remote status check failed: {post_launch_check.get('error')}",
                payload={**job_payload, "post_launch_check": post_launch_check},
                source="mycodex",
                phase="experiment_running",
                phase_message="Remote job started, but immediate status verification failed.",
            )
        elif post_launch_check.get("status") == "needs_reconciliation":
            store.add_experiment_event(
                experiment["id"], event_type="remote_job_needs_reconciliation", level="warning",
                message="Remote process ownership or budget supervisor could not be verified; inspect before continuing.",
                payload={**job_payload, **post_launch_check}, source="mycodex", phase="waiting_for_decision")
        elif post_launch_check.get("status") == "completed":
            store.add_experiment_event(
                experiment["id"], event_type="remote_job_completed", level="info",
                message="Remote execution completed successfully; result artifacts still require a gate.",
                payload={**job_payload, **post_launch_check}, source="mycodex", phase="waiting_for_decision")
        elif post_launch_check.get("status") != "running":
            failure_hint = _remote_output_has_failure_hint(post_launch_check) or post_launch_check.get("status") in {"failed", "timed_out"}
            store.add_experiment_event(
                experiment["id"],
                event_type="remote_job_exited_early",
                level="error" if failure_hint else "warning",
                message=(
                    f"Remote job pid={remote_pid} was not running "
                    f"{post_launch_check_delay:g}s after launch."
                ),
                payload={**job_payload, "post_launch_check": post_launch_check,
                         "receipt": post_launch_check.get("receipt") if post_launch_check.get("receipt_verified") else job_payload.get("receipt"),
                         "receipt_verified": post_launch_check.get("receipt_verified", False)},
                source="mycodex",
                status="failed" if failure_hint else None,
                phase="failed" if failure_hint else "waiting_for_decision",
                phase_message=(
                    "Remote job exited immediately; inspect log tail for launch errors."
                    if failure_hint
                    else "Remote job is no longer running after launch; inspect artifacts before deciding next action."
                ),
            )
    return {**job_payload, "post_launch_check": post_launch_check, "dry_run": False}


def _post_launch_status_check(
    target: RemoteTarget,
    remote_pid: str,
    remote_log: str,
    *,
    ssh_key: str | None,
    ssh_password_file: str | None,
    timeout: int,
    delay: float,
    lines: int,
    runner: RemoteRunner | None,
    receipt_path: str = "",
    execution_id: str = "",
    manifest_sha256: str = "",
) -> dict[str, Any]:
    if delay > 0:
        time.sleep(delay)
    script = _remote_status_script(remote_pid, remote_log, max(1, lines), receipt_path=receipt_path)
    try:
        if runner:
            result = runner(target, script, ssh_key, timeout)
        else:
            result = run_remote_script(
                target,
                script,
                ssh_key,
                timeout,
                ssh_password_file=ssh_password_file,
            )
    except Exception as exc:
        return {"check_failed": True, "error": str(exc)}
    parsed = _parse_key_values(result.stdout)
    receipt = {}
    verified = False
    status = parsed.get("REMOTE_STATUS") or "unknown"
    try:
        if parsed.get("REMOTE_RECEIPT"):
            receipt = json.loads(base64.b64decode(parsed["REMOTE_RECEIPT"]))
            if not isinstance(receipt, dict):
                raise ValueError("receipt must be an object")
            verified = receipt.get("execution_id") == execution_id and receipt.get("manifest_sha256") == manifest_sha256
            if verified:
                status = receipt_observation_status(receipt, parsed)
    except (ValueError, TypeError):
        receipt = {}
    if receipt_path and not verified:
        status = "needs_reconciliation"
    return {
        "status": status,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "check_failed": False,
        "receipt": receipt,
        "receipt_verified": verified,
    }


def _remote_output_has_failure_hint(check: dict[str, Any]) -> bool:
    text = f"{check.get('stdout') or ''}\n{check.get('stderr') or ''}"
    return bool(_REMOTE_FAILURE_HINT_RE.search(text))


def remote_job_status(
    store: Store,
    project_name: str,
    experiment_name: str,
    *,
    remote_ref: str | None = None,
    ssh_key: str | None = None,
    ssh_password_file: str | None = None,
    pid: str | None = None,
    log_path: str | None = None,
    lines: int = 40,
    timeout: int = 30,
    remote_runner: RemoteRunner | None = None,
) -> dict[str, Any]:
    project = require_project(store, project_name)
    experiment = _require_experiment(store, project["id"], experiment_name)
    config = _experiment_config(experiment)
    latest_job = _latest_event_payload(store, experiment["id"], "remote_job_started")
    resolved_remote_ref = remote_ref or latest_job.get("remote_ref") or experiment["remote_ref"] or config.get("remote_ref")
    if not resolved_remote_ref:
        raise ValueError("remote_ref is not configured for this experiment")
    target = parse_remote_ref(str(resolved_remote_ref))
    resolved_ssh_key = ssh_key or config.get("ssh_key") or os.environ.get("MYCODEX_SSH_KEY")
    resolved_ssh_password_file = (
        ssh_password_file
        or config.get("ssh_password_file")
        or os.environ.get("MYCODEX_SSH_PASSWORD_FILE")
    )
    resolved_pid = str(pid or latest_job.get("remote_pid") or "")
    resolved_log = str(log_path or latest_job.get("log_path") or "")
    if not resolved_pid:
        raise ValueError("remote PID is required; pass --pid or launch a remote job first")
    script = _remote_status_script(resolved_pid, resolved_log, max(1, lines),
                                   receipt_path=str(latest_job.get("receipt_path") or "") if not pid else "")
    runner = remote_runner or run_remote_script
    if remote_runner:
        result = runner(target, script, str(resolved_ssh_key) if resolved_ssh_key else None, timeout)
    else:
        result = run_remote_script(
            target,
            script,
            str(resolved_ssh_key) if resolved_ssh_key else None,
            timeout,
            ssh_password_file=str(resolved_ssh_password_file) if resolved_ssh_password_file else None,
        )
    parsed = _parse_key_values(result.stdout)
    status = parsed.get("REMOTE_STATUS") or "unknown"
    receipt = {}
    receipt_verified = False
    try:
        if parsed.get("REMOTE_RECEIPT"):
            receipt = json.loads(base64.b64decode(parsed["REMOTE_RECEIPT"]))
            if not isinstance(receipt, dict):
                raise ValueError("receipt must be an object")
            receipt_verified = (receipt.get("execution_id") == latest_job.get("execution_id")
                                and receipt.get("manifest_sha256") == latest_job.get("manifest_sha256"))
            if receipt_verified:
                status = receipt_observation_status(receipt, parsed)
    except (ValueError, TypeError):
        receipt = {}
    if latest_job.get("receipt_path") and not pid and not receipt_verified:
        status = "needs_reconciliation"
    level = "info" if status == "running" else "warning"
    store.add_experiment_event(
        experiment["id"],
        event_type="remote_job_status",
        level=level,
        message=f"Remote job pid={resolved_pid} status={status}.",
        payload={
            "remote_ref": target.ref,
            "remote_pid": resolved_pid,
            "log_path": resolved_log,
            "status": status,
            "ssh_auth": "password_file" if resolved_ssh_password_file else ("key" if resolved_ssh_key else "default"),
            "stdout": result.stdout,
            "stderr": result.stderr,
            "execution_id": latest_job.get("execution_id"),
            "receipt": receipt,
            "receipt_verified": receipt_verified,
        },
        source="mycodex",
        phase="experiment_running" if status == "running" else "waiting_for_decision",
    )
    return {"status": status, "stdout": result.stdout, "stderr": result.stderr,
            "receipt": receipt, "receipt_verified": receipt_verified,
            "execution_id": latest_job.get("execution_id")}


def poll_wandb_run(
    store: Store,
    project_name: str,
    experiment_name: str,
    *,
    run_path: str | None = None,
    wandb_entity: str | None = None,
    wandb_project: str | None = None,
    wandb_run_id: str | None = None,
    metrics: list[str] | None = None,
    history_limit: int = 5,
    stop_conditions: list[str] | None = None,
    fail_on_nonfinite: bool = False,
    complete_on_finish: bool = False,
    interval: int = 0,
    max_polls: int = 1,
    trigger_next_control: bool = False,
    control_timeout: int = 1800,
    control_sandbox: str | None = None,
    control_approval_policy: str | None = None,
    wandb_fetcher: WandbFetcher | None = None,
) -> dict[str, Any]:
    if max_polls < 1:
        raise ValueError("max_polls must be positive")
    if max_polls > 1 and interval < 1:
        raise ValueError("interval must be positive when max_polls > 1")

    last_result: dict[str, Any] = {}
    for poll_index in range(max_polls):
        last_result = poll_wandb_once(
            store,
            project_name,
            experiment_name,
            run_path=run_path,
            wandb_entity=wandb_entity,
            wandb_project=wandb_project,
            wandb_run_id=wandb_run_id,
            metrics=metrics or [],
            history_limit=history_limit,
            stop_conditions=stop_conditions or [],
            fail_on_nonfinite=fail_on_nonfinite,
            complete_on_finish=complete_on_finish,
            wandb_fetcher=wandb_fetcher,
        )
        if last_result.get("should_stop"):
            break
        if poll_index < max_polls - 1:
            time.sleep(interval)

    if trigger_next_control and last_result.get("should_trigger_control"):
        control_result = start_experiment_control_turn(
            store,
            project_name,
            experiment_name,
            timeout=control_timeout,
            sandbox=control_sandbox,
            approval_policy=control_approval_policy,
        )
        last_result = {**last_result, "control_result": control_result}
    return last_result


def poll_wandb_once(
    store: Store,
    project_name: str,
    experiment_name: str,
    *,
    run_path: str | None = None,
    wandb_entity: str | None = None,
    wandb_project: str | None = None,
    wandb_run_id: str | None = None,
    metrics: list[str],
    history_limit: int,
    stop_conditions: list[str],
    fail_on_nonfinite: bool,
    complete_on_finish: bool,
    wandb_fetcher: WandbFetcher | None = None,
) -> dict[str, Any]:
    project = require_project(store, project_name)
    experiment = _require_experiment(store, project["id"], experiment_name)
    run_ref = _resolve_wandb_run_ref(
        store,
        experiment,
        run_path=run_path,
        wandb_entity=wandb_entity,
        wandb_project=wandb_project,
        wandb_run_id=wandb_run_id,
    )
    store.add_experiment_event(
        experiment["id"],
        event_type="wandb_poll_started",
        level="info",
        message=f"Polling W&B run {run_ref.path}.",
        payload={"run_path": run_ref.path, "metrics": metrics},
        source="mycodex",
        status="running",
    )

    fetcher = wandb_fetcher or fetch_wandb_run
    snapshot = fetcher(run_ref, metrics, max(0, history_limit))
    latest_metrics = snapshot.get("latest_metrics") or {}
    state = str(snapshot.get("state") or "unknown").lower()
    store.add_experiment_event(
        experiment["id"],
        event_type="metric",
        level="info",
        message=_wandb_metric_message(run_ref, state, snapshot),
        payload=snapshot,
        source="mycodex",
        status="running",
    )

    matched_conditions = evaluate_stop_conditions(latest_metrics, stop_conditions)
    nonfinite = _nonfinite_metrics(latest_metrics)
    should_stop = False
    should_trigger_control = False
    outcome = "running"

    if state in {"failed", "crashed", "killed"}:
        outcome = "failed"
        should_stop = True
        should_trigger_control = True
        store.add_experiment_event(
            experiment["id"],
            event_type="failed",
            level="error",
            message=f"W&B run {run_ref.path} ended with state={state}.",
            payload=snapshot,
            source="mycodex",
            status="failed",
        )
    elif fail_on_nonfinite and nonfinite:
        outcome = "nonfinite"
        should_stop = True
        should_trigger_control = True
        store.add_experiment_event(
            experiment["id"],
            event_type="failed",
            level="error",
            message=f"W&B run {run_ref.path} reported non-finite metric(s): {', '.join(nonfinite)}.",
            payload={**snapshot, "nonfinite_metrics": nonfinite},
            source="mycodex",
            status="failed",
        )
    elif matched_conditions:
        outcome = "stop_condition_met"
        should_stop = True
        should_trigger_control = True
        store.add_experiment_event(
            experiment["id"],
            event_type="decision",
            level="info",
            message=f"Stop condition met for W&B run {run_ref.path}: {', '.join(matched_conditions)}.",
            payload={**snapshot, "matched_stop_conditions": matched_conditions},
            source="mycodex",
            phase="waiting_for_decision",
        )
    elif state in {"finished", "completed"}:
        outcome = "finished"
        should_stop = True
        should_trigger_control = True
        event_type = "completed" if complete_on_finish else "wandb_run_finished"
        store.add_experiment_event(
            experiment["id"],
            event_type=event_type,
            level="info",
            message=f"W&B run {run_ref.path} finished.",
            payload=snapshot,
            source="mycodex",
            status="completed" if complete_on_finish else "running",
            phase="completed" if complete_on_finish else "waiting_for_decision",
        )

    return {
        "run_path": run_ref.path,
        "state": state,
        "outcome": outcome,
        "snapshot": snapshot,
        "matched_stop_conditions": matched_conditions,
        "nonfinite_metrics": nonfinite,
        "should_stop": should_stop,
        "should_trigger_control": should_trigger_control,
    }


def sync_remote_experiment_state(
    store: Store,
    project_name: str,
    experiment_name: str,
    *,
    remote_ref: str | None = None,
    ssh_key: str | None = None,
    ssh_password_file: str | None = None,
    remote_state_root: str | None = None,
    remote_state_path: str | None = None,
    remote_events_path: str | None = None,
    remote_base: str | None = None,
    remote_python: str | None = None,
    event_tail: int = 200,
    log_tail_lines: int = 120,
    stale_seconds: int = 900,
    timeout: int = 90,
    remote_runner: RemoteRunner | None = None,
) -> dict[str, Any]:
    """Import a structured remote experiment snapshot into local experiment events.

    A failed SSH/Python sync is intentionally observation-only: it records
    `remote_sync_failed` and returns no anomaly so local sleep/network outages
    cannot be misclassified as remote experiment failure.
    """

    project = require_project(store, project_name)
    experiment = _require_experiment(store, project["id"], experiment_name)
    config = _experiment_config(experiment)
    latest_job_event = store.get_latest_experiment_event(experiment["id"], "remote_job_started")
    latest_job = _event_payload(latest_job_event) if latest_job_event else {}
    latest_state_hint = _latest_remote_state_hint(store, experiment["id"])
    latest_job = _merge_latest_job_with_remote_state_hint(latest_job, latest_state_hint)
    request = _resolve_remote_state_request(
        experiment,
        latest_job,
        remote_ref=remote_ref,
        remote_state_root=remote_state_root,
        remote_state_path=remote_state_path,
        remote_events_path=remote_events_path,
        remote_base=remote_base,
        remote_python=remote_python,
    )
    resolved_remote_ref = request.get("remote_ref")
    if not resolved_remote_ref:
        return {
            "sync_status": "skipped",
            "reason": "remote_ref_missing",
            "experiment": experiment_name,
        }
    if not any(
        request.get(key)
        for key in (
            "root",
            "state_path",
            "events_path",
            "base",
            "latest_log_path",
            "latest_pid",
        )
    ):
        return {
            "sync_status": "skipped",
            "reason": "remote_state_target_missing",
            "experiment": experiment_name,
            "remote_ref": resolved_remote_ref,
        }

    target = parse_remote_ref(str(resolved_remote_ref))
    script = _remote_state_sync_script(
        root=str(request.get("root") or ""),
        state_path=str(request.get("state_path") or ""),
        events_path=str(request.get("events_path") or ""),
        base=str(request.get("base") or ""),
        remote_python=str(request.get("python") or ""),
        latest_log_path=str(request.get("latest_log_path") or ""),
        latest_pid=str(request.get("latest_pid") or ""),
        default_eval_epochs=request.get("default_eval_epochs") or [],
        default_eval_envs=request.get("default_eval_envs") or [],
        event_tail=max(1, event_tail),
        log_tail_lines=max(1, log_tail_lines),
    )

    observed_at_local = int(time.time())
    try:
        result = _call_remote_runner(
            remote_runner,
            target,
            script,
            str(Path(ssh_key).expanduser()) if ssh_key else None,
            timeout,
            ssh_password_file=ssh_password_file,
        )
        snapshot = json.loads(result.stdout.strip() or "{}")
        if not isinstance(snapshot, dict):
            raise ValueError("remote sync did not return a JSON object")
    except Exception as exc:
        payload = {
            "sync_status": "failed",
            "remote_ref": target.ref,
            "error": str(exc),
            "observed_at_local": observed_at_local,
        }
        store.add_experiment_event(
            experiment["id"],
            event_type="remote_sync_failed",
            level="warning",
            message=f"Remote state sync failed: {exc}",
            payload=payload,
            source="mycodex",
            phase=experiment["phase"],
            phase_message=experiment["phase_message"],
        )
        return payload

    snapshot["sync_status"] = str(snapshot.get("sync_status") or "ok")
    snapshot["remote_ref"] = target.ref
    snapshot["observed_at_local"] = observed_at_local
    snapshot["latest_remote_job_event_id"] = latest_job_event["id"] if latest_job_event else None
    snapshot["request"] = {
        key: value
        for key, value in request.items()
        if key not in {"remote_ref"} and value
    }
    remote_now = snapshot.get("remote_now")
    if isinstance(remote_now, (int, float)):
        snapshot["clock_skew_seconds"] = float(remote_now) - float(observed_at_local)
    if snapshot["sync_status"] != "ok":
        payload = {
            "sync_status": snapshot["sync_status"],
            "remote_ref": target.ref,
            "error": snapshot.get("error") or "remote sync script failed",
            "snapshot": _compact_remote_snapshot(snapshot),
            "observed_at_local": observed_at_local,
        }
        store.add_experiment_event(
            experiment["id"],
            event_type="remote_sync_failed",
            level="warning",
            message=f"Remote state sync failed: {payload['error']}",
            payload=payload,
            source="mycodex",
            phase=experiment["phase"],
            phase_message=experiment["phase_message"],
        )
        return payload

    imported_events = _record_remote_event_tail(store, experiment, snapshot)
    recent_events = store.list_experiment_events(experiment["id"], limit=1000)
    binding_mismatch = _remote_snapshot_binding_mismatch(snapshot, latest_job, latest_job_event)
    if binding_mismatch:
        anomalies = []
    else:
        anomalies = classify_remote_experiment_snapshot(
            snapshot,
            recent_events=recent_events,
            stale_seconds=stale_seconds,
        )
    for anomaly in anomalies:
        anomaly["fingerprint"] = _remote_anomaly_fingerprint(snapshot, anomaly)
    control_anomalies = [item for item in anomalies if item.get("action") == "control"]
    batch_fingerprint = _remote_anomaly_batch_fingerprint(control_anomalies)
    compact = _compact_remote_snapshot(snapshot)
    heartbeat_payload = {
        "snapshot": compact,
        "remote_events_imported": imported_events,
        "anomaly_count": len(anomalies),
        "control_anomaly_count": len(control_anomalies),
        "batch_fingerprint": batch_fingerprint,
    }
    if binding_mismatch:
        heartbeat_payload["ignored_stale_snapshot"] = binding_mismatch
    store.add_experiment_event(
        experiment["id"],
        event_type="remote_heartbeat",
        level="info" if not control_anomalies else "warning",
        message=_remote_heartbeat_message(compact, anomalies),
        payload=heartbeat_payload,
        source="mycodex",
        status="running",
        phase=experiment["phase"],
        phase_message=experiment["phase_message"],
    )
    anomaly_event_id = None
    if control_anomalies and batch_fingerprint:
        if not _recent_remote_fingerprint_event(
            recent_events,
            "remote_anomaly",
            batch_fingerprint,
            cooldown_seconds=stale_seconds,
            now=observed_at_local,
        ):
            anomaly_event_id = store.add_experiment_event(
                experiment["id"],
                event_type="remote_anomaly",
                level=_remote_control_event_level(control_anomalies),
                message="Remote experiment control condition detected: "
                + ", ".join(item["code"] for item in control_anomalies),
                payload={
                    "batch_fingerprint": batch_fingerprint,
                    "anomalies": control_anomalies,
                    "snapshot": _compact_remote_snapshot(snapshot, include_hints=True),
                },
                source="mycodex",
                status="running",
                phase="analyzing",
                phase_message="Remote anomaly detected; watchdog will request a control decision.",
            )

    if binding_mismatch:
        store.add_experiment_event(
            experiment["id"],
            event_type="stale_remote_snapshot_ignored",
            level="warning",
            message=(
                "Ignored remote snapshot for "
                f"{binding_mismatch['snapshot_run_id']} because latest local job is "
                f"{binding_mismatch['latest_job_run_id']}."
            ),
            payload={
                "binding": binding_mismatch,
                "snapshot": _compact_remote_snapshot(snapshot, include_hints=True),
            },
            source="mycodex",
            status="running",
            phase=experiment["phase"],
            phase_message=experiment["phase_message"],
        )

    return {
        "sync_status": snapshot["sync_status"],
        "snapshot": snapshot,
        "compact_snapshot": compact,
        "anomalies": anomalies,
        "control_anomalies": control_anomalies,
        "batch_fingerprint": batch_fingerprint,
        "anomaly_event_id": anomaly_event_id,
        "remote_events_imported": imported_events,
        "remote_ref": target.ref,
        "ignored_stale_snapshot": binding_mismatch,
    }


def classify_remote_experiment_snapshot(
    snapshot: dict[str, Any],
    *,
    recent_events: list[Any] | None = None,
    stale_seconds: int = 900,
) -> list[dict[str, Any]]:
    """Pure anomaly classifier for a successful remote snapshot."""

    if snapshot.get("sync_status") not in {None, "ok"}:
        return []
    anomalies: list[dict[str, Any]] = []
    remote_now = _number(snapshot.get("remote_now"))
    local_now = _number(snapshot.get("observed_at_local")) or time.time()
    now_for_age = remote_now or local_now
    clock_skew = _number(snapshot.get("clock_skew_seconds"))
    if clock_skew is None and remote_now is not None:
        clock_skew = remote_now - local_now
    if clock_skew is not None and abs(clock_skew) > 300:
        anomalies.append(
            {
                "code": "remote_clock_skew",
                "severity": "warning",
                "action": "observe",
                "message": f"Remote clock skew is {clock_skew:.1f}s; stale judgments are low-confidence.",
            }
        )

    files = snapshot.get("files") if isinstance(snapshot.get("files"), dict) else {}
    state_info = files.get("state") if isinstance(files.get("state"), dict) else {}
    events_info = files.get("events") if isinstance(files.get("events"), dict) else {}
    state_age = _age_seconds(state_info.get("mtime"), now_for_age)
    events_age = _age_seconds(events_info.get("mtime"), now_for_age)
    heartbeat_age = min(
        [age for age in (state_age, events_age) if age is not None],
        default=None,
    )

    pids = snapshot.get("pids") if isinstance(snapshot.get("pids"), dict) else {}
    train_pid = pids.get("train") if isinstance(pids.get("train"), dict) else {}
    queue_pid = pids.get("eval_queue") if isinstance(pids.get("eval_queue"), dict) else {}
    supervisor_pid = pids.get("supervisor") if isinstance(pids.get("supervisor"), dict) else {}
    watchdog_pid = pids.get("watchdog") if isinstance(pids.get("watchdog"), dict) else {}
    summary = snapshot.get("summary") if isinstance(snapshot.get("summary"), dict) else {}
    expected_rows = int(_number(summary.get("expected_rows")) or 0)
    rows = int(_number(summary.get("rows")) or 0)
    summary_complete = bool(summary.get("complete"))
    has_structured_state = bool(
        snapshot.get("root")
        or snapshot.get("run_id")
        or snapshot.get("eval_label")
        or state_info.get("exists")
        or (summary.get("path") and summary.get("path") != "")
    )
    train_done = bool(snapshot.get("train_done_marker"))
    queue_done = bool(snapshot.get("queue_done_marker"))
    train_alive = bool(train_pid.get("alive"))
    queue_alive = bool(queue_pid.get("alive"))
    supervisor_alive = bool(supervisor_pid.get("alive"))
    watchdog_alive = bool(watchdog_pid.get("alive"))
    has_train_pid = bool(train_pid.get("pid"))
    has_queue_pid = bool(queue_pid.get("pid"))

    state = snapshot.get("state") if isinstance(snapshot.get("state"), dict) else {}
    state_status = str(state.get("status") or state.get("phase") or "").lower()
    terminal_success_state = state_status in {
        "completed",
        "complete",
        "done",
        "supervisor_done",
        "max_next_launches_reached",
    }
    terminal_failure_state = state_status in {"failed", "stopped", "error"}
    terminal_state = terminal_success_state or terminal_failure_state
    current = snapshot.get("current") if isinstance(snapshot.get("current"), dict) else {}
    expected_next = bool(current.get("expected_next_action") or current.get("next_command"))
    terminal_success = bool(summary_complete or terminal_success_state or (train_done and queue_done))
    any_remote_process_alive = bool(train_alive or queue_alive or supervisor_alive or watchdog_alive)

    host = snapshot.get("host") if isinstance(snapshot.get("host"), dict) else {}
    current_boot_id = str(host.get("boot_id") or "").strip()
    previous_boot_id = _latest_remote_boot_id_from_events(recent_events or [], snapshot)
    uptime_seconds = _number(host.get("uptime_seconds"))
    if current_boot_id and previous_boot_id and current_boot_id != previous_boot_id:
        anomalies.append(
            {
                "code": "remote_host_rebooted",
                "severity": "warning",
                "action": "observe" if terminal_success else "control",
                "message": "Remote host boot id changed since the previous heartbeat.",
                "previous_boot_id": previous_boot_id,
                "boot_id": current_boot_id,
                "uptime_seconds": int(uptime_seconds) if uptime_seconds is not None else None,
            }
        )
    elif (
        current_boot_id
        and uptime_seconds is not None
        and uptime_seconds < min(max(stale_seconds, 1), 600)
        and has_structured_state
        and not any_remote_process_alive
        and not terminal_success
    ):
        anomalies.append(
            {
                "code": "remote_recent_reboot",
                "severity": "warning",
                "action": "control",
                "message": "Remote host appears to have rebooted recently and no controller processes are alive.",
                "boot_id": current_boot_id,
                "uptime_seconds": int(uptime_seconds),
            }
        )

    if heartbeat_age is not None and heartbeat_age > stale_seconds and not train_alive and not terminal_success:
        anomalies.append(
            {
                "code": "remote_heartbeat_stale",
                "severity": "warning",
                "action": "control" if not supervisor_alive and not watchdog_alive else "observe",
                "message": f"Remote state/events heartbeat is {int(heartbeat_age)}s old.",
                "age_seconds": int(heartbeat_age),
            }
        )

    if has_structured_state and has_train_pid and not train_alive and not train_done and not summary_complete:
        anomalies.append(
            {
                "code": "process_dead_without_done",
                "severity": "error",
                "action": "control",
                "message": "Training process is not alive, no Done marker was found, and eval summary is incomplete.",
            }
        )

    if (train_done or queue_done) and expected_rows and rows < expected_rows and has_queue_pid and not queue_alive:
        anomalies.append(
            {
                "code": "eval_queue_dead_incomplete",
                "severity": "error",
                "action": "control",
                "message": f"Eval queue is dead with only {rows}/{expected_rows} expected rows.",
                "rows": rows,
                "expected_rows": expected_rows,
            }
        )

    if (
        terminal_success
        and not train_alive
        and not queue_alive
        and not supervisor_alive
        and not watchdog_alive
        and not expected_next
    ):
        anomalies.append(
            {
                "code": "remote_terminal_complete_ready_for_next",
                "severity": "info",
                "action": "control",
                "message": "Remote train/eval controller reached a complete terminal state and no next action is running.",
            }
        )

    if summary_complete and not queue_alive and not terminal_state and (expected_next or not supervisor_alive):
        anomalies.append(
            {
                "code": "summary_complete_no_next",
                "severity": "error",
                "action": "control",
                "message": "Eval summary is complete, but the remote controller has not recorded a terminal or next action.",
            }
        )

    mismatch = _detect_contract_command_mismatch(snapshot)
    if mismatch:
        anomalies.append(
            {
                "code": "contract_command_mismatch",
                "severity": "error",
                "action": "control",
                "message": "Remote command does not match contract/run identity: " + "; ".join(mismatch),
                "mismatches": mismatch,
            }
        )

    log_hints = _remote_log_failure_hints(snapshot)
    if log_hints:
        anomalies.append(
            {
                "code": "remote_log_failure_hint",
                "severity": "error",
                "action": "control",
                "message": "Remote log tail contains failure hints.",
                "hints": log_hints[:5],
            }
        )

    if train_alive and _active_train_looks_stalled(snapshot, stale_seconds=stale_seconds, now_for_age=now_for_age):
        anomalies.append(
            {
                "code": "active_train_no_progress",
                "severity": "warning",
                "action": "control",
                "message": "Training process is alive, but log mtime is stale and GPU utilization is low.",
            }
        )

    return anomalies


def _latest_remote_boot_id_from_events(events: list[Any], snapshot: dict[str, Any]) -> str:
    current_boot_id = str((snapshot.get("host") or {}).get("boot_id") or "") if isinstance(snapshot.get("host"), dict) else ""
    for event in events:
        try:
            event_type = str(event["event_type"])
        except Exception:
            continue
        if event_type != "remote_heartbeat":
            continue
        payload = _event_payload(event)
        event_snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
        if not _remote_heartbeat_matches_snapshot(event_snapshot, snapshot):
            continue
        host = event_snapshot.get("host") if isinstance(event_snapshot.get("host"), dict) else {}
        boot_id = str(host.get("boot_id") or event_snapshot.get("boot_id") or "").strip()
        if boot_id and boot_id != current_boot_id:
            return boot_id
        if boot_id:
            return boot_id
    return ""


def _remote_heartbeat_matches_snapshot(event_snapshot: dict[str, Any], snapshot: dict[str, Any]) -> bool:
    for key in ("remote_ref", "root", "base"):
        expected = str(snapshot.get(key) or "")
        observed = str(event_snapshot.get(key) or "")
        if expected and observed and expected != observed:
            return False
    return True


def _run_remote_anomaly_control_cycle(
    store: Store,
    project_name: str,
    experiment_name: str,
    *,
    sync_result: dict[str, Any],
    ssh_key: str | None,
    ssh_password_file: str | None,
    remote_ref: str | None,
    remote_python: str | None,
    auto_execute: bool,
    allow_full_train: bool,
    control_timeout: int,
    control_sandbox: str | None,
    control_approval_policy: str | None,
    control_model: str | None,
    control_reasoning_effort: str | None,
    peer_review_bypass_timeout_seconds: int,
    peer_review_bypass_on: Any,
    peer_review_bypass_allowed_classes: Any,
    trigger: str = "remote_anomaly",
) -> dict[str, Any]:
    project = require_project(store, project_name)
    experiment = _require_experiment(store, project["id"], experiment_name)
    batch_fingerprint = str(sync_result.get("batch_fingerprint") or "")
    if not batch_fingerprint:
        return {"executed": False, "reason": "missing_remote_anomaly_fingerprint"}
    recent_events = store.list_experiment_events(experiment["id"], limit=1000)
    if _recent_remote_control_started_without_failure(
        recent_events,
        batch_fingerprint,
        cooldown_seconds=max(300, min(control_timeout, max(0, peer_review_bypass_timeout_seconds))),
        now=int(time.time()),
    ):
        return {"executed": False, "reason": "duplicate_remote_anomaly_control", "batch_fingerprint": batch_fingerprint}
    blocked_since = _peer_review_blocked_since(
        recent_events,
        trigger=trigger,
        key_field="batch_fingerprint",
        key_value=batch_fingerprint,
    )
    if blocked_since is None:
        blocked_since = int(time.time())

    store.add_experiment_event(
        experiment["id"],
        event_type="remote_anomaly_control_started",
        level="warning",
        message=f"Starting control session for {trigger.replace('_', ' ')}.",
        payload={
            "trigger": trigger,
            "batch_fingerprint": batch_fingerprint,
            "anomalies": sync_result.get("control_anomalies") or [],
            "snapshot": sync_result.get("compact_snapshot") or {},
        },
        source="mycodex",
        status="running",
        phase="analyzing",
        phase_message="Remote anomaly detected; asking bound control session for the next executable contract.",
    )
    try:
        revision_attempts = 0
        max_revision_attempts = 1
        while True:
            control_result, contract = _obtain_execution_contract_with_repair(
                store,
                project_name,
                experiment_name,
                control_timeout=control_timeout,
                control_sandbox=control_sandbox,
                control_approval_policy=control_approval_policy,
                control_model=control_model,
                control_reasoning_effort=control_reasoning_effort,
                context_payload={
                    "trigger": trigger,
                    "batch_fingerprint": batch_fingerprint,
                    "revision_attempt": revision_attempts,
                    "anomalies": sync_result.get("control_anomalies") or [],
                    "snapshot": sync_result.get("compact_snapshot") or {},
                },
            )
            if contract is None or control_result is None:
                store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_anomaly_control_failed",
                    level="warning",
                    message="Remote anomaly control did not produce a parseable execution contract.",
                    payload={"batch_fingerprint": batch_fingerprint, "revision_attempt": revision_attempts},
                    source="mycodex",
                    phase="analyzing",
                    phase_message="Remote anomaly control contract was unparseable; watchdog will retry after cooldown.",
                )
                return {"executed": False, "reason": "contract_missing", "batch_fingerprint": batch_fingerprint}

            parsed_event_id = store.add_experiment_event(
                experiment["id"],
                event_type="control_contract_parsed",
                level="info",
                message=_contract_summary(contract),
                payload={
                    "contract": contract,
                    "trigger": trigger,
                    "batch_fingerprint": batch_fingerprint,
                    "revision_attempt": revision_attempts,
                },
                source="mycodex",
                phase=_phase_for_contract_execution(contract) if auto_execute else "waiting_for_decision",
            )
            task_record = record_execution_contract_task(
                store,
                project_name,
                experiment_name,
                contract,
                trigger=trigger,
                auto_execute_policy=auto_execute and _contract_skips_peer_review(contract),
                allow_full_train=allow_full_train,
                source_event_id=parsed_event_id,
                run_id=control_result.get("run_id") if isinstance(control_result.get("run_id"), int) else None,
                context_key=batch_fingerprint,
            )
            if not auto_execute:
                return {"executed": False, "reason": "auto_execute_disabled", "contract": contract}

            if _contract_skips_peer_review(contract):
                _record_peer_review_skipped(
                    store,
                    experiment["id"],
                    contract=contract,
                    trigger=trigger,
                    payload={
                        "batch_fingerprint": batch_fingerprint,
                        "revision_attempt": revision_attempts,
                    },
                )
                break

            peer_review_result = start_experiment_peer_review_turn(
                store,
                project_name,
                experiment_name,
                control_output=control_result.get("output_text") or "",
                contract=contract,
                timeout=min(control_timeout, 900),
                model=control_model,
                reasoning_effort=control_reasoning_effort,
            )
            peer_review_decision = extract_peer_review_decision(peer_review_result.get("output_text") or "")
            decision = str((peer_review_decision or {}).get("decision") or "").strip().lower()
            store.add_experiment_event(
                experiment["id"],
                event_type="peer_review_decision",
                level="info" if decision == "approve" else "warning",
                message=f"Peer review gate decision: {decision or '<missing>'}.",
                payload={
                    "decision": peer_review_decision,
                    "thread_id": peer_review_result.get("thread_id"),
                    "trigger": trigger,
                    "batch_fingerprint": batch_fingerprint,
                    "revision_attempt": revision_attempts,
                },
                source="mycodex",
                phase="checking_git" if decision == "approve" else "waiting_for_decision",
            )
            if decision == "approve":
                break
            hard_blockers = _peer_review_hard_blockers(
                peer_review_decision,
                contract=contract,
                allow_full_train=allow_full_train,
            )
            if hard_blockers:
                store.add_experiment_event(
                    experiment["id"],
                    event_type="remote_anomaly_control_failed",
                    level="warning",
                    message="Peer review found hard safety blockers for the remote-anomaly execution contract.",
                    payload={
                        "trigger": trigger,
                        "batch_fingerprint": batch_fingerprint,
                        "decision": peer_review_decision,
                        "hard_blockers": hard_blockers,
                        "revision_attempt": revision_attempts,
                        "max_revision_attempts": max_revision_attempts,
                    },
                    source="mycodex",
                    phase="waiting_for_decision",
                    phase_message="Peer review found hard safety blockers.",
                )
                return {
                    "executed": False,
                    "reason": "peer_review_hard_blockers",
                    "decision": peer_review_decision,
                    "hard_blockers": hard_blockers,
                }
            bypass_assessment = _assess_peer_review_bypass(
                contract=contract,
                peer_review_decision=peer_review_decision,
                peer_review_result=peer_review_result,
                decision=decision,
                blocked_since=blocked_since or int(time.time()),
                now=int(time.time()),
                allow_full_train=allow_full_train,
                timeout_seconds=peer_review_bypass_timeout_seconds,
                bypass_on=peer_review_bypass_on,
                allowed_classes=peer_review_bypass_allowed_classes,
            )
            if bypass_assessment["eligible"]:
                store.add_experiment_event(
                    experiment["id"],
                    event_type="peer_review_bypass_eligible",
                    level="warning",
                    message="Peer review has blocked long enough and contract passed bypass eligibility checks.",
                    payload={
                        "assessment": bypass_assessment,
                        "trigger": trigger,
                        "batch_fingerprint": batch_fingerprint,
                        "revision_attempt": revision_attempts,
                    },
                    source="mycodex",
                    status="running",
                    phase="checking_git",
                    phase_message="Peer-review bypass eligible; executing contract under policy.",
                )
                store.add_experiment_event(
                    experiment["id"],
                    event_type="peer_review_bypassed",
                    level="warning",
                    message="Bypassing peer review after configured timeout for a complete non-block contract.",
                    payload={
                        "assessment": bypass_assessment,
                        "decision": peer_review_decision,
                        "trigger": trigger,
                        "batch_fingerprint": batch_fingerprint,
                    },
                    source="mycodex",
                    status="running",
                    phase="checking_git",
                )
                break
            if bypass_assessment["decision"] in set(bypass_assessment["bypass_on"]):
                store.add_experiment_event(
                    experiment["id"],
                    event_type="peer_review_bypass_denied",
                    level="info",
                    message="Peer-review bypass was considered but denied by policy.",
                    payload={
                        "assessment": bypass_assessment,
                        "trigger": trigger,
                        "batch_fingerprint": batch_fingerprint,
                        "revision_attempt": revision_attempts,
                    },
                    source="mycodex",
                    phase="waiting_for_decision",
                )
            if decision == "revise" and revision_attempts < max_revision_attempts:
                revision_attempts += 1
                store.add_experiment_event(
                    experiment["id"],
                    event_type="decision",
                    level="info",
                    message="Peer review requested revision; retrying remote-anomaly control turn immediately.",
                    payload={
                        "decision": peer_review_decision,
                        "revision_attempt": revision_attempts,
                        "max_revision_attempts": max_revision_attempts,
                        "trigger": trigger,
                        "batch_fingerprint": batch_fingerprint,
                    },
                    source="mycodex",
                    status="running",
                    phase="analyzing",
                    phase_message="Revising remote-anomaly execution contract after peer review.",
                )
                continue

            if decision in {"block", "revise"} and _execution_class(contract) in SAFE_FALLBACK_EXECUTION_CLASSES:
                contract = _record_peer_review_soft_objections(
                    store,
                    experiment["id"],
                    contract=contract,
                    peer_review_decision=peer_review_decision,
                    trigger=trigger,
                    payload={
                        "batch_fingerprint": batch_fingerprint,
                        "revision_attempt": revision_attempts,
                        "max_revision_attempts": max_revision_attempts,
                    },
                )
                break

            if decision == "revise":
                return _pause_with_reviewer_objections(
                    store,
                    experiment["id"],
                    peer_review_decision=peer_review_decision,
                    revision_attempts=revision_attempts,
                    max_revision_attempts=max_revision_attempts,
                    trigger=trigger,
                    payload={"batch_fingerprint": batch_fingerprint},
                )

            store.add_experiment_event(
                experiment["id"],
                event_type="remote_anomaly_control_failed",
                level="warning",
                message="Peer review did not approve the remote-anomaly execution contract.",
                payload={
                    "trigger": trigger,
                    "batch_fingerprint": batch_fingerprint,
                    "decision": peer_review_decision,
                    "revision_attempt": revision_attempts,
                    "max_revision_attempts": max_revision_attempts,
                },
                source="mycodex",
                phase="waiting_for_decision",
                phase_message="Peer review blocked automatic remote-anomaly repair.",
            )
            return {"executed": False, "reason": "peer_review_not_approved", "decision": peer_review_decision}

        task_id = int(task_record["task"]["id"])
        lease_owner = _lease_recorded_protocol_task(
            store,
            experiment,
            task_id,
            contract=contract,
            owner=f"mycodex-{trigger}",
            lease_seconds=max(60, control_timeout),
        )
        execution = _execute_with_protocol_lease(
            store,
            project_name,
            experiment_name,
            contract,
            ssh_key=ssh_key,
            ssh_password_file=ssh_password_file,
            remote_ref=remote_ref,
            remote_python=remote_python,
            allow_full_train=allow_full_train,
            protocol_task_id=task_id,
            owner=lease_owner,
            lease_seconds=max(60, control_timeout),
        )
        _complete_recorded_protocol_task(
            store,
            experiment,
            task_id,
            execution=execution,
            contract=contract,
            owner=lease_owner or "",
        )
        if not execution.get("executed"):
            store.add_experiment_event(
                experiment["id"],
                event_type="remote_anomaly_control_failed",
                level="warning",
                message="Remote anomaly control contract did not execute successfully.",
                payload={
                    "trigger": trigger,
                    "batch_fingerprint": batch_fingerprint,
                    "control": control_result,
                    "contract": contract,
                    "execution": execution,
                },
                source="mycodex",
                phase="waiting_for_decision",
                phase_message="Control execution failed; watchdog will retry or request a revised contract.",
            )
            return {
                "executed": False,
                "reason": str(execution.get("reason") or "execution_failed"),
                "batch_fingerprint": batch_fingerprint,
                "control": control_result,
                "contract": contract,
                "execution": execution,
            }
        store.add_experiment_event(
            experiment["id"],
            event_type="remote_anomaly_control_completed",
            level="info",
            message="Remote anomaly control cycle completed.",
            payload={
                "trigger": trigger,
                "batch_fingerprint": batch_fingerprint,
                "control": control_result,
                "contract": contract,
                "execution": execution,
            },
            source="mycodex",
            status="running",
            phase="experiment_running" if execution.get("launch") else "diagnosis_ready",
        )
        return {
            "executed": True,
            "batch_fingerprint": batch_fingerprint,
            "control": control_result,
            "contract": contract,
            "execution": execution,
        }
    except Exception as exc:
        store.add_experiment_event(
            experiment["id"],
            event_type="remote_anomaly_control_failed",
            level="error",
            message=f"Remote anomaly control cycle failed: {exc}",
            payload={"batch_fingerprint": batch_fingerprint, "error": str(exc)},
            source="mycodex",
            phase="waiting_for_decision",
            phase_message="Remote anomaly control failed; inspect latest event payload.",
        )
        raise


def _obtain_execution_contract_with_repair(
    store: Store,
    project_name: str,
    experiment_name: str,
    *,
    control_timeout: int,
    control_sandbox: str | None,
    control_approval_policy: str | None,
    control_model: str | None,
    control_reasoning_effort: str | None,
    context_payload: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    project = require_project(store, project_name)
    experiment = _require_experiment(store, project["id"], experiment_name)
    control_result = start_experiment_control_turn(
        store,
        project_name,
        experiment_name,
        timeout=control_timeout,
        sandbox=control_sandbox,
        approval_policy=control_approval_policy,
        model=control_model,
        reasoning_effort=control_reasoning_effort,
    )
    contract = extract_execution_contract(control_result.get("output_text") or "")
    if contract is not None:
        return control_result, contract

    parse_error = _execution_contract_parse_failure(control_result.get("output_text") or "")
    store.add_experiment_event(
        experiment["id"],
        event_type="control_contract_parse_failed",
        level="warning",
        message="Control session did not return a parseable JSON execution contract.",
        payload={
            "run_id": control_result.get("run_id"),
            "status": control_result.get("status"),
            "parse_error": parse_error,
            "context": context_payload,
        },
        source="mycodex",
        phase="analyzing",
        phase_message="Control contract was unparseable; attempting repair.",
    )
    for repair_attempt in range(1, MAX_CONTRACT_PARSE_REPAIR_ATTEMPTS + 1):
        store.add_experiment_event(
            experiment["id"],
            event_type="contract_repair_scheduled",
            level="warning",
            message=f"Scheduling contract repair turn {repair_attempt}/{MAX_CONTRACT_PARSE_REPAIR_ATTEMPTS}.",
            payload={
                "repair_attempt": repair_attempt,
                "max_attempts": MAX_CONTRACT_PARSE_REPAIR_ATTEMPTS,
                "parse_error": parse_error,
                "context": context_payload,
            },
            source="mycodex",
            status="running",
            phase="analyzing",
            phase_message="Repairing unparseable execution contract.",
        )
        try:
            control_result = start_experiment_contract_repair_turn(
                store,
                project_name,
                experiment_name,
                previous_output=control_result.get("output_text") or "",
                parse_error=parse_error,
                timeout=min(control_timeout, 900),
                sandbox=control_sandbox,
                approval_policy=control_approval_policy,
                model=control_model,
                reasoning_effort=control_reasoning_effort,
            )
        except Exception as exc:
            parse_error = f"contract repair turn failed: {exc}"
            continue
        contract = extract_execution_contract(control_result.get("output_text") or "")
        if contract is not None:
            return control_result, contract
        parse_error = _execution_contract_parse_failure(control_result.get("output_text") or "")
        store.add_experiment_event(
            experiment["id"],
            event_type="control_contract_parse_failed",
            level="warning",
            message="Contract repair turn also returned unparseable JSON.",
            payload={
                "run_id": control_result.get("run_id"),
                "status": control_result.get("status"),
                "repair_attempt": repair_attempt,
                "parse_error": parse_error,
                "context": context_payload,
            },
            source="mycodex",
            phase="analyzing",
            phase_message="Contract repair output was unparseable.",
        )
    store.add_experiment_event(
        experiment["id"],
        event_type="supervisor_retry_scheduled",
        level="warning",
        message=(
            "Control contract remained unparseable after repair; watchdog will retry "
            "after a later remote sync if the anomaly is still present."
        ),
        payload={"reason": "contract_parse_repair_failed", "parse_error": parse_error, "context": context_payload},
        source="mycodex",
        status="running",
        phase="analyzing",
        phase_message="Contract parse repair failed; retry scheduled.",
    )
    return control_result, None


def supervise_experiment(
    store: Store,
    project_name: str,
    experiment_name: str,
    *,
    run_path: str | None = None,
    metrics: list[str] | None = None,
    stop_conditions: list[str] | None = None,
    fail_on_nonfinite: bool = True,
    complete_on_finish: bool = False,
    remote_ref: str | None = None,
    ssh_key: str | None = None,
    ssh_password_file: str | None = None,
    remote_python: str | None = None,
    remote_pid: str | None = None,
    remote_log_path: str | None = None,
    interval: int = 300,
    max_cycles: int = 1,
    trigger_next_control: bool = True,
    auto_execute: bool = True,
    allow_full_train: bool = False,
    control_timeout: int = 1800,
    control_sandbox: str | None = None,
    control_approval_policy: str | None = None,
    control_model: str | None = None,
    control_reasoning_effort: str | None = None,
    peer_review_bypass_timeout_seconds: int = 1800,
    peer_review_bypass_on: Any = None,
    peer_review_bypass_allowed_classes: Any = None,
) -> dict[str, Any]:
    if max_cycles < 1:
        raise ValueError("max_cycles must be positive")
    if max_cycles > 1 and interval < 1:
        raise ValueError("interval must be positive when max_cycles > 1")

    project = require_project(store, project_name)
    experiment = _require_experiment(store, project["id"], experiment_name)
    metric_list = metrics or []
    condition_list = stop_conditions or []
    current_run_path = run_path
    current_remote_pid = remote_pid
    current_remote_log_path = remote_log_path
    last_result: dict[str, Any] = {}
    handled_terminal_keys: set[str] = set()
    executed_contracts: set[str] = set()

    store.add_experiment_event(
        experiment["id"],
        event_type="supervisor_started",
        level="info",
        message=f"Supervisor started; interval={interval}s, max_cycles={max_cycles}.",
        payload={
            "run_path": current_run_path,
            "metrics": metric_list,
            "stop_conditions": condition_list,
            "trigger_next_control": trigger_next_control,
            "auto_execute": auto_execute,
            "allow_full_train": allow_full_train,
        },
        source="mycodex",
        status="running",
        phase="monitoring_wandb",
        phase_message="Supervisor is monitoring remote job and W&B.",
    )

    for cycle in range(max_cycles):
        terminal_reason = ""
        remote_result: dict[str, Any] | None = None
        wandb_result: dict[str, Any] | None = None

        store.add_experiment_event(
            experiment["id"],
            event_type="supervisor_heartbeat",
            iteration=experiment["current_iteration"],
            level="info",
            message=f"Supervisor cycle {cycle + 1}/{max_cycles}.",
            payload={"cycle": cycle + 1, "max_cycles": max_cycles, "run_path": current_run_path},
            source="mycodex",
            status="running",
            phase="monitoring_wandb",
            phase_message=f"Supervisor active; cycle {cycle + 1}/{max_cycles}.",
        )

        try:
            remote_result = remote_job_status(
                store,
                project_name,
                experiment_name,
                remote_ref=remote_ref,
                ssh_key=ssh_key,
                ssh_password_file=ssh_password_file,
                pid=current_remote_pid,
                log_path=current_remote_log_path,
                lines=120,
            )
            if remote_result.get("status") == "not_running":
                terminal_reason = "remote_not_running"
        except Exception as exc:
            store.add_experiment_event(
                experiment["id"],
                event_type="supervisor_warning",
                level="warning",
                message=f"Remote status check failed: {exc}",
                payload={"error": str(exc)},
                source="mycodex",
                phase="monitoring_wandb",
            )

        if current_run_path:
            try:
                wandb_result = poll_wandb_once(
                    store,
                    project_name,
                    experiment_name,
                    run_path=current_run_path,
                    metrics=metric_list,
                    history_limit=50,
                    stop_conditions=condition_list,
                    fail_on_nonfinite=fail_on_nonfinite,
                    complete_on_finish=complete_on_finish,
                )
                if wandb_result.get("should_stop"):
                    terminal_reason = str(wandb_result.get("outcome") or "wandb_terminal")
            except Exception as exc:
                store.add_experiment_event(
                    experiment["id"],
                    event_type="supervisor_warning",
                    level="warning",
                    message=f"W&B poll failed: {exc}",
                    payload={"run_path": current_run_path, "error": str(exc)},
                    source="mycodex",
                    phase="monitoring_wandb",
                )

        last_result = {
            "cycle": cycle + 1,
            "remote": remote_result,
            "wandb": wandb_result,
            "terminal_reason": terminal_reason,
            "run_path": current_run_path,
        }

        if terminal_reason:
            terminal_key = _terminal_event_key(
                terminal_reason,
                run_path=current_run_path,
                remote_pid=current_remote_pid,
                remote_log_path=current_remote_log_path,
                wandb_result=wandb_result,
                remote_result=remote_result,
            )
            if terminal_key in handled_terminal_keys:
                store.add_experiment_event(
                    experiment["id"],
                    event_type="blocked",
                    level="warning",
                    message=(
                        "Supervisor stopped instead of handling the same terminal condition twice: "
                        f"{terminal_reason}."
                    ),
                    payload={**last_result, "terminal_key": terminal_key},
                    source="mycodex",
                    phase="waiting_for_decision",
                    phase_message="Duplicate terminal condition suppressed; waiting for decision.",
                )
                break
            handled_terminal_keys.add(terminal_key)
            store.add_experiment_event(
                experiment["id"],
                event_type="supervisor_terminal_detected",
                level="warning",
                message=f"Supervisor detected terminal condition: {terminal_reason}.",
                payload=last_result,
                source="mycodex",
                phase="eval_pending" if trigger_next_control else "waiting_for_decision",
                phase_message=(
                    "Training/control job reached a terminal state; next state is eval launch or result gate."
                    if trigger_next_control
                    else "Terminal condition detected; waiting for manual decision."
                ),
            )
            if not trigger_next_control:
                break

            revision_attempts = 0
            max_revision_attempts = 1
            contract: dict[str, Any] | None = None
            control_result: dict[str, Any] | None = None
            retry_after_revision_limit = False
            retry_payload: dict[str, Any] = {}
            blocked_since = _peer_review_blocked_since(
                store.list_experiment_events(experiment["id"], limit=1000),
                trigger="supervisor",
                key_field="terminal_key",
                key_value=terminal_key,
            ) or int(time.time())
            while True:
                control_result = start_experiment_control_turn(
                    store,
                    project_name,
                    experiment_name,
                    timeout=control_timeout,
                    sandbox=control_sandbox,
                    approval_policy=control_approval_policy,
                    model=control_model,
                    reasoning_effort=control_reasoning_effort,
                )
                contract = extract_execution_contract(control_result.get("output_text") or "")
                if contract is None:
                    parse_error = _execution_contract_parse_failure(control_result.get("output_text") or "")
                    store.add_experiment_event(
                        experiment["id"],
                        event_type="control_contract_parse_failed",
                        level="warning",
                        message="Control session did not return a parseable JSON execution contract.",
                        payload={
                            "run_id": control_result.get("run_id"),
                            "status": control_result.get("status"),
                            "parse_error": parse_error,
                        },
                        source="mycodex",
                        phase="analyzing",
                        phase_message="Control contract was unparseable; attempting repair.",
                    )
                    for repair_attempt in range(1, MAX_CONTRACT_PARSE_REPAIR_ATTEMPTS + 1):
                        store.add_experiment_event(
                            experiment["id"],
                            event_type="contract_repair_scheduled",
                            level="warning",
                            message=(
                                "Scheduling contract repair turn "
                                f"{repair_attempt}/{MAX_CONTRACT_PARSE_REPAIR_ATTEMPTS}."
                            ),
                            payload={
                                "repair_attempt": repair_attempt,
                                "max_attempts": MAX_CONTRACT_PARSE_REPAIR_ATTEMPTS,
                                "parse_error": parse_error,
                            },
                            source="mycodex",
                            status="running",
                            phase="analyzing",
                            phase_message="Repairing unparseable execution contract.",
                        )
                        try:
                            control_result = start_experiment_contract_repair_turn(
                                store,
                                project_name,
                                experiment_name,
                                previous_output=control_result.get("output_text") or "",
                                parse_error=parse_error,
                                timeout=min(control_timeout, 900),
                                sandbox=control_sandbox,
                                approval_policy=control_approval_policy,
                                model=control_model,
                                reasoning_effort=control_reasoning_effort,
                            )
                        except Exception as exc:
                            parse_error = f"contract repair turn failed: {exc}"
                            continue
                        contract = extract_execution_contract(control_result.get("output_text") or "")
                        if contract is not None:
                            break
                        parse_error = _execution_contract_parse_failure(
                            control_result.get("output_text") or ""
                        )
                        store.add_experiment_event(
                            experiment["id"],
                            event_type="control_contract_parse_failed",
                            level="warning",
                            message="Contract repair turn also returned unparseable JSON.",
                            payload={
                                "run_id": control_result.get("run_id"),
                                "status": control_result.get("status"),
                                "repair_attempt": repair_attempt,
                                "parse_error": parse_error,
                            },
                            source="mycodex",
                            phase="analyzing",
                            phase_message="Contract repair output was unparseable.",
                        )
                    if contract is None:
                        retry_after_revision_limit = True
                        retry_payload = {
                            "reason": "contract_parse_repair_failed",
                            "parse_error": parse_error,
                            "terminal_key": terminal_key,
                        }
                        store.add_experiment_event(
                            experiment["id"],
                            event_type="supervisor_retry_scheduled",
                            level="warning",
                            message=(
                                "Control contract remained unparseable after repair; "
                                "supervisor will retry this terminal condition on the next cycle."
                            ),
                            payload={**retry_payload, "last_result": last_result},
                            source="mycodex",
                            status="running",
                            phase="analyzing",
                            phase_message="Contract parse repair failed; retry scheduled.",
                        )
                        break
                parsed_event_id = store.add_experiment_event(
                    experiment["id"],
                    event_type="control_contract_parsed",
                    level="info",
                    message=_contract_summary(contract),
                    payload={"contract": contract},
                    source="mycodex",
                    phase=_phase_for_contract_execution(contract) if auto_execute else "waiting_for_decision",
                )
                task_record = record_execution_contract_task(
                    store,
                    project_name,
                    experiment_name,
                    contract,
                    trigger="supervisor",
                    auto_execute_policy=auto_execute and _contract_skips_peer_review(contract),
                    allow_full_train=allow_full_train,
                    source_event_id=parsed_event_id,
                    run_id=control_result.get("run_id") if isinstance(control_result.get("run_id"), int) else None,
                    context_key=terminal_key,
                )
                if not auto_execute:
                    contract = None
                    break
                if _contract_skips_peer_review(contract):
                    _record_peer_review_skipped(
                        store,
                        experiment["id"],
                        contract=contract,
                        trigger="supervisor",
                        payload={
                            "terminal_key": terminal_key,
                            "revision_attempt": revision_attempts,
                        },
                    )
                    break
                peer_review_result = start_experiment_peer_review_turn(
                    store,
                    project_name,
                    experiment_name,
                    control_output=control_result.get("output_text") or "",
                    contract=contract,
                    timeout=min(control_timeout, 900),
                    model=control_model,
                    reasoning_effort=control_reasoning_effort,
                )
                peer_review_decision = extract_peer_review_decision(
                    peer_review_result.get("output_text") or ""
                )
                if peer_review_decision is None:
                    bypass_assessment = _assess_peer_review_bypass(
                        contract=contract,
                        peer_review_decision=None,
                        peer_review_result=peer_review_result,
                        decision="",
                        blocked_since=blocked_since,
                        now=int(time.time()),
                        allow_full_train=allow_full_train,
                        timeout_seconds=peer_review_bypass_timeout_seconds,
                        bypass_on=peer_review_bypass_on,
                        allowed_classes=peer_review_bypass_allowed_classes,
                    )
                    if bypass_assessment["eligible"]:
                        store.add_experiment_event(
                            experiment["id"],
                            event_type="peer_review_bypassed",
                            level="warning",
                            message="Bypassing timed-out peer review after configured timeout for a complete contract.",
                            payload={
                                "assessment": bypass_assessment,
                                "peer_review": peer_review_result,
                                "trigger": "supervisor",
                                "terminal_key": terminal_key,
                            },
                            source="mycodex",
                            status="running",
                            phase="checking_git",
                        )
                        break
                    store.add_experiment_event(
                        experiment["id"],
                        event_type="peer_review_bypass_denied",
                        level="info",
                        message="Peer-review timeout bypass was considered but denied by policy.",
                        payload={
                            "assessment": bypass_assessment,
                            "peer_review": peer_review_result,
                            "trigger": "supervisor",
                            "terminal_key": terminal_key,
                        },
                        source="mycodex",
                        phase="waiting_for_decision",
                    )
                    store.add_experiment_event(
                        experiment["id"],
                        event_type="blocked",
                        level="warning",
                        message="Peer review did not return a parseable JSON gate decision.",
                        payload={"peer_review": peer_review_result},
                        source="mycodex",
                        phase="waiting_for_decision",
                        phase_message="Peer review was inconclusive; waiting for the next control decision.",
                    )
                    contract = None
                    break
                decision = str(peer_review_decision.get("decision") or "").strip().lower()
                store.add_experiment_event(
                    experiment["id"],
                    event_type="peer_review_decision",
                    level="info" if decision == "approve" else "warning",
                    message=f"Peer review gate decision: {decision or '<missing>'}.",
                    payload={
                        "decision": peer_review_decision,
                        "thread_id": peer_review_result.get("thread_id"),
                        "revision_attempt": revision_attempts,
                        "trigger": "supervisor",
                        "terminal_key": terminal_key,
                    },
                    source="mycodex",
                    phase="checking_git" if decision == "approve" else "waiting_for_decision",
                    phase_message=(
                        "Peer review approved execution; proceeding."
                        if decision == "approve"
                        else "Peer review requested revision or blocked execution."
                    ),
                )
                if decision == "approve":
                    break
                hard_blockers = _peer_review_hard_blockers(
                    peer_review_decision,
                    contract=contract,
                    allow_full_train=allow_full_train,
                )
                if hard_blockers:
                    store.add_experiment_event(
                        experiment["id"],
                        event_type="blocked",
                        level="warning",
                        message="Peer review found hard safety blockers for the execution contract.",
                        payload={
                            "decision": peer_review_decision,
                            "hard_blockers": hard_blockers,
                            "terminal_key": terminal_key,
                            "revision_attempt": revision_attempts,
                            "max_revision_attempts": max_revision_attempts,
                        },
                        source="mycodex",
                        phase="waiting_for_decision",
                        phase_message="Peer review found hard safety blockers.",
                    )
                    contract = None
                    break
                bypass_assessment = _assess_peer_review_bypass(
                    contract=contract,
                    peer_review_decision=peer_review_decision,
                    peer_review_result=peer_review_result,
                    decision=decision,
                    blocked_since=blocked_since,
                    now=int(time.time()),
                    allow_full_train=allow_full_train,
                    timeout_seconds=peer_review_bypass_timeout_seconds,
                    bypass_on=peer_review_bypass_on,
                    allowed_classes=peer_review_bypass_allowed_classes,
                )
                if bypass_assessment["eligible"]:
                    store.add_experiment_event(
                        experiment["id"],
                        event_type="peer_review_bypass_eligible",
                        level="warning",
                        message="Peer review has blocked long enough and contract passed bypass eligibility checks.",
                        payload={
                            "assessment": bypass_assessment,
                            "trigger": "supervisor",
                            "terminal_key": terminal_key,
                            "revision_attempt": revision_attempts,
                        },
                        source="mycodex",
                        status="running",
                        phase="checking_git",
                        phase_message="Peer-review bypass eligible; executing contract under policy.",
                    )
                    store.add_experiment_event(
                        experiment["id"],
                        event_type="peer_review_bypassed",
                        level="warning",
                        message="Bypassing peer review after configured timeout for a complete non-block contract.",
                        payload={
                            "assessment": bypass_assessment,
                            "decision": peer_review_decision,
                            "trigger": "supervisor",
                            "terminal_key": terminal_key,
                        },
                        source="mycodex",
                        status="running",
                        phase="checking_git",
                    )
                    break
                if bypass_assessment["decision"] in set(bypass_assessment["bypass_on"]):
                    store.add_experiment_event(
                        experiment["id"],
                        event_type="peer_review_bypass_denied",
                        level="info",
                        message="Peer-review bypass was considered but denied by policy.",
                        payload={
                            "assessment": bypass_assessment,
                            "trigger": "supervisor",
                            "terminal_key": terminal_key,
                            "revision_attempt": revision_attempts,
                        },
                        source="mycodex",
                        phase="waiting_for_decision",
                    )
                if decision == "revise" and revision_attempts < max_revision_attempts:
                    revision_attempts += 1
                    store.add_experiment_event(
                        experiment["id"],
                        event_type="decision",
                        level="info",
                        message=(
                            "Peer review requested revision; triggering a revised "
                            "control turn automatically."
                        ),
                        payload={
                            "decision": peer_review_decision,
                            "revision_attempt": revision_attempts,
                            "max_revision_attempts": max_revision_attempts,
                        },
                        source="mycodex",
                        status="running",
                        phase="analyzing",
                        phase_message="Revising execution contract after peer review.",
                    )
                    continue
                if decision in {"block", "revise"} and _execution_class(contract) in SAFE_FALLBACK_EXECUTION_CLASSES:
                    contract = _record_peer_review_soft_objections(
                        store,
                        experiment["id"],
                        contract=contract,
                        peer_review_decision=peer_review_decision,
                        trigger="supervisor",
                        payload={
                            "terminal_key": terminal_key,
                            "revision_attempt": revision_attempts,
                            "max_revision_attempts": max_revision_attempts,
                        },
                    )
                    break
                if decision == "revise":
                    paused = _pause_with_reviewer_objections(
                        store,
                        experiment["id"],
                        peer_review_decision=peer_review_decision,
                        revision_attempts=revision_attempts,
                        max_revision_attempts=max_revision_attempts,
                        trigger="supervisor",
                        payload={
                            "terminal_key": terminal_key,
                            "last_result": last_result,
                        },
                    )
                    last_result = {**last_result, "control_result": paused}
                contract = None
                break
            if contract is None or control_result is None:
                if retry_after_revision_limit:
                    handled_terminal_keys.discard(terminal_key)
                    last_result = {
                        **last_result,
                        "terminal_reason": "",
                        "retry_after_revision_limit": retry_payload,
                    }
                    if cycle < max_cycles - 1:
                        time.sleep(interval)
                    continue
                break
            contract_fingerprint = _contract_execution_fingerprint(contract)
            if contract_fingerprint in executed_contracts:
                store.add_experiment_event(
                    experiment["id"],
                    event_type="blocked",
                    level="warning",
                    message="Supervisor refused to execute the same control contract twice in one run.",
                    payload={"contract": contract, "fingerprint": contract_fingerprint},
                    source="mycodex",
                    phase="waiting_for_decision",
                    phase_message="Duplicate control contract suppressed; waiting for decision.",
                )
                break
            executed_contracts.add(contract_fingerprint)
            task_id = int(task_record["task"]["id"])
            lease_owner = _lease_recorded_protocol_task(
                store,
                experiment,
                task_id,
                contract=contract,
                owner="mycodex-supervisor",
                lease_seconds=max(60, control_timeout),
            )
            execution = _execute_with_protocol_lease(
                store,
                project_name,
                experiment_name,
                contract,
                ssh_key=ssh_key,
                ssh_password_file=ssh_password_file,
                remote_ref=remote_ref,
                remote_python=remote_python,
                allow_full_train=allow_full_train,
                protocol_task_id=task_id,
                owner=lease_owner,
                lease_seconds=max(60, control_timeout),
            )
            _complete_recorded_protocol_task(
                store,
                experiment,
                task_id,
                execution=execution,
                contract=contract,
                owner=lease_owner or "",
            )
            last_result = {**last_result, "control": control_result, "contract": contract, "execution": execution}
            launched = execution.get("launch") if isinstance(execution, dict) else None
            if launched:
                current_remote_pid = str(launched.get("remote_pid") or current_remote_pid or "")
                current_remote_log_path = str(launched.get("log_path") or current_remote_log_path or "")
            next_run_path = _run_path_from_launch_or_contract(launched, contract)
            if launched:
                current_run_path = next_run_path
                continue
            if next_run_path:
                current_run_path = next_run_path
                continue
            break

        if cycle < max_cycles - 1:
            time.sleep(interval)

    store.add_experiment_event(
        experiment["id"],
        event_type="supervisor_stopped",
        level="info",
        message="Supervisor stopped.",
        payload=last_result,
        source="mycodex",
        phase="monitoring_wandb" if not last_result.get("terminal_reason") else "waiting_for_decision",
        phase_message="Supervisor stopped; check latest events.",
    )
    return last_result


def run_experiment_watchdog(
    store: Store,
    project_name: str,
    *,
    experiment_name: str | None = None,
    run_path: str | None = None,
    metrics: list[str] | None = None,
    stop_conditions: list[str] | None = None,
    fail_on_nonfinite: bool = True,
    complete_on_finish: bool = False,
    remote_ref: str | None = None,
    ssh_key: str | None = None,
    ssh_password_file: str | None = None,
    remote_state_root: str | None = None,
    remote_state_path: str | None = None,
    remote_events_path: str | None = None,
    remote_base: str | None = None,
    remote_python: str | None = None,
    remote_sync: bool = True,
    remote_event_tail: int = 200,
    remote_log_tail_lines: int = 120,
    remote_sync_timeout: int = 90,
    remote_anomaly_control: bool = True,
    interval: int = 300,
    max_cycles: int = 1,
    stale_seconds: int = 900,
    supervisor_interval: int = 300,
    trigger_next_control: bool = True,
    auto_execute: bool = True,
    allow_full_train: bool = False,
    monitor_only: bool = False,
    control_timeout: int = 1800,
    control_sandbox: str | None = None,
    control_approval_policy: str | None = None,
    control_model: str | None = None,
    control_reasoning_effort: str | None = None,
    peer_review_bypass_timeout_seconds: int = 1800,
    peer_review_bypass_on: Any = None,
    peer_review_bypass_allowed_classes: Any = None,
    force: bool = False,
    remote_runner: RemoteRunner | None = None,
    supervisor_runner: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if max_cycles < 1:
        raise ValueError("max_cycles must be positive")
    if max_cycles > 1 and interval < 1:
        raise ValueError("interval must be positive when max_cycles > 1")
    if stale_seconds < 0:
        raise ValueError("stale_seconds must be non-negative")

    project = require_project(store, project_name)
    runner = supervisor_runner or supervise_experiment
    last_result: dict[str, Any] = {}
    for cycle in range(max_cycles):
        cycle_results: list[dict[str, Any]] = []
        experiments = (
            [_require_experiment(store, project["id"], experiment_name)]
            if experiment_name
            else [
                experiment
                for experiment in store.list_experiments(project["id"])
                if experiment["status"] == "running"
            ]
        )
        for experiment in experiments:
            result = _watchdog_check_experiment(
                store,
                project_name,
                experiment,
                run_path=run_path,
                metrics=metrics or [],
                stop_conditions=stop_conditions or [],
                fail_on_nonfinite=fail_on_nonfinite,
                complete_on_finish=complete_on_finish,
                remote_ref=remote_ref,
                ssh_key=ssh_key,
                ssh_password_file=ssh_password_file,
                remote_state_root=remote_state_root,
                remote_state_path=remote_state_path,
                remote_events_path=remote_events_path,
                remote_base=remote_base,
                remote_python=remote_python,
                remote_sync=remote_sync,
                remote_event_tail=remote_event_tail,
                remote_log_tail_lines=remote_log_tail_lines,
                remote_sync_timeout=remote_sync_timeout,
                remote_anomaly_control=remote_anomaly_control,
                stale_seconds=stale_seconds,
                supervisor_interval=supervisor_interval,
                trigger_next_control=trigger_next_control,
                auto_execute=auto_execute,
                allow_full_train=allow_full_train,
                monitor_only=monitor_only,
                control_timeout=control_timeout,
                control_sandbox=control_sandbox,
                control_approval_policy=control_approval_policy,
                control_model=control_model,
                control_reasoning_effort=control_reasoning_effort,
                peer_review_bypass_timeout_seconds=peer_review_bypass_timeout_seconds,
                peer_review_bypass_on=peer_review_bypass_on,
                peer_review_bypass_allowed_classes=peer_review_bypass_allowed_classes,
                force=force,
                remote_runner=remote_runner,
                supervisor_runner=runner,
            )
            cycle_results.append(result)
        last_result = {"cycle": cycle + 1, "experiments": cycle_results}
        if cycle < max_cycles - 1:
            time.sleep(interval)
    return last_result


def _watchdog_check_experiment(
    store: Store,
    project_name: str,
    experiment,
    *,
    run_path: str | None,
    metrics: list[str],
    stop_conditions: list[str],
    fail_on_nonfinite: bool,
    complete_on_finish: bool,
    remote_ref: str | None,
    ssh_key: str | None,
    ssh_password_file: str | None,
    remote_state_root: str | None,
    remote_state_path: str | None,
    remote_events_path: str | None,
    remote_base: str | None,
    remote_python: str | None,
    remote_sync: bool,
    remote_event_tail: int,
    remote_log_tail_lines: int,
    remote_sync_timeout: int,
    remote_anomaly_control: bool,
    stale_seconds: int,
    supervisor_interval: int,
    trigger_next_control: bool,
    auto_execute: bool,
    allow_full_train: bool,
    monitor_only: bool,
    control_timeout: int,
    control_sandbox: str | None,
    control_approval_policy: str | None,
    control_model: str | None,
    control_reasoning_effort: str | None,
    peer_review_bypass_timeout_seconds: int,
    peer_review_bypass_on: Any,
    peer_review_bypass_allowed_classes: Any,
    force: bool,
    remote_runner: RemoteRunner | None,
    supervisor_runner: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    latest_job_event = store.get_latest_experiment_event(experiment["id"], "remote_job_started")
    latest_job = _event_payload(latest_job_event) if latest_job_event else {}
    config = _experiment_config(experiment)
    resolved_run_path = _resolve_watchdog_run_path(run_path, latest_job, config)
    resolved_remote_pid = str(latest_job.get("remote_pid") or "")
    resolved_remote_log_path = str(
        latest_job.get("log_path")
        or latest_job.get("outer_log")
        or latest_job.get("remote_log")
        or latest_job.get("remote_log_path")
        or ""
    )
    now = int(time.time())
    base_payload = {
        "stale_seconds": stale_seconds,
        "force": force,
        "has_remote_job": bool(latest_job),
        "remote_job_event_id": latest_job_event["id"] if latest_job_event else None,
        "remote_pid": resolved_remote_pid,
        "remote_log_path": resolved_remote_log_path,
        "run_path": resolved_run_path,
    }
    if experiment["status"] != "running":
        store.add_experiment_event(
            experiment["id"],
            event_type="watchdog_skipped",
            level="info",
            message="Watchdog skipped non-running experiment.",
            payload={**base_payload, "experiment_status": experiment["status"]},
            source="mycodex",
            phase=experiment["phase"],
            phase_message=experiment["phase_message"],
        )
        return {"experiment": experiment["name"], "action": "skipped", "reason": "not_running", **base_payload}

    project = require_project(store, project_name)
    active_guard = _experiment_active_codex_run_guard(
        store,
        project,
        experiment,
        timeout_seconds=control_timeout,
        now=now,
    )
    if active_guard.get("active"):
        return {
            "experiment": experiment["name"],
            "action": "skipped",
            "reason": "active_control_run",
            **base_payload,
            "active_run": active_guard,
        }

    remote_sync_result: dict[str, Any] | None = None
    duplicate_remote_control = False
    latest_state_hint = _latest_remote_state_hint(store, experiment["id"])
    if remote_sync and (
        _watchdog_has_remote_state_target(
            experiment,
            remote_state_root=remote_state_root,
            remote_state_path=remote_state_path,
            remote_events_path=remote_events_path,
            remote_base=remote_base,
        )
        or bool(latest_state_hint)
    ):
        remote_sync_result = sync_remote_experiment_state(
            store,
            project_name,
            experiment["name"],
            remote_ref=remote_ref,
            ssh_key=ssh_key,
            ssh_password_file=ssh_password_file,
            remote_state_root=remote_state_root,
            remote_state_path=remote_state_path,
            remote_events_path=remote_events_path,
            remote_base=remote_base,
            remote_python=remote_python,
            event_tail=remote_event_tail,
            log_tail_lines=remote_log_tail_lines,
            stale_seconds=stale_seconds,
            timeout=remote_sync_timeout,
            remote_runner=remote_runner,
        )
        if not remote_sync_result.get("ignored_stale_snapshot"):
            sync_targets = _watchdog_targets_from_remote_snapshot(remote_sync_result.get("snapshot"))
            if sync_targets.get("remote_pid"):
                resolved_remote_pid = sync_targets["remote_pid"]
                base_payload["remote_pid"] = resolved_remote_pid
            if sync_targets.get("remote_log_path"):
                resolved_remote_log_path = sync_targets["remote_log_path"]
                base_payload["remote_log_path"] = resolved_remote_log_path
            if sync_targets.get("run_path") and not resolved_run_path:
                resolved_run_path = sync_targets["run_path"]
                base_payload["run_path"] = resolved_run_path
        control_anomalies = remote_sync_result.get("control_anomalies") or []
        if control_anomalies and remote_anomaly_control and trigger_next_control:
            control_result = _run_remote_anomaly_control_cycle(
                store,
                project_name,
                experiment["name"],
                sync_result=remote_sync_result,
                ssh_key=ssh_key,
                ssh_password_file=ssh_password_file,
                remote_ref=remote_ref,
                remote_python=remote_python,
                auto_execute=auto_execute,
                allow_full_train=allow_full_train,
                control_timeout=control_timeout,
                control_sandbox=control_sandbox,
                control_approval_policy=control_approval_policy,
                control_model=control_model,
                control_reasoning_effort=control_reasoning_effort,
                peer_review_bypass_timeout_seconds=peer_review_bypass_timeout_seconds,
                peer_review_bypass_on=peer_review_bypass_on,
                peer_review_bypass_allowed_classes=peer_review_bypass_allowed_classes,
            )
            if control_result.get("reason") != "duplicate_remote_anomaly_control":
                return {
                    "experiment": experiment["name"],
                    "action": "remote_anomaly_control",
                    "remote_sync": _compact_watchdog_sync_result(remote_sync_result),
                    "control_result": control_result,
                    **base_payload,
                }
            base_payload["duplicate_remote_anomaly_control"] = control_result
            duplicate_remote_control = True
        if control_anomalies and not duplicate_remote_control:
            store.add_experiment_event(
                experiment["id"],
                event_type="watchdog_skipped",
                level="warning",
                message="Watchdog detected remote anomalies, but remote anomaly control is disabled.",
                payload={
                    **base_payload,
                    "remote_sync": _compact_watchdog_sync_result(remote_sync_result),
                },
                source="mycodex",
                phase="waiting_for_decision",
                phase_message="Remote anomaly detected; automatic control is disabled.",
            )
            return {
                "experiment": experiment["name"],
                "action": "skipped",
                "reason": "remote_anomaly_control_disabled",
                "remote_sync": _compact_watchdog_sync_result(remote_sync_result),
                **base_payload,
            }

    if monitor_only:
        payload = {
            **base_payload,
            "monitor_only": True,
            "remote_sync": _compact_watchdog_sync_result(remote_sync_result) if remote_sync_result else None,
        }
        store.add_experiment_event(
            experiment["id"],
            event_type="watchdog_monitor_only_completed",
            level="info",
            message="Watchdog monitor-only pass completed.",
            payload=payload,
            source="mycodex",
            phase=experiment["phase"],
            phase_message=experiment["phase_message"],
        )
        return {
            "experiment": experiment["name"],
            "action": "monitor_only",
            **payload,
        }

    events = store.list_experiment_events(experiment["id"], limit=1000)
    local_gate = _latest_local_gate_control_request(events)
    if local_gate and remote_anomaly_control and trigger_next_control:
        control_result = _run_remote_anomaly_control_cycle(
            store,
            project_name,
            experiment["name"],
            sync_result={
                "batch_fingerprint": local_gate["batch_fingerprint"],
                "control_anomalies": [local_gate["anomaly"]],
                "compact_snapshot": {
                    "local_gate_event": local_gate["event"],
                    "latest_remote": (
                        _compact_watchdog_sync_result(remote_sync_result) if remote_sync_result else None
                    ),
                },
            },
            ssh_key=ssh_key,
            ssh_password_file=ssh_password_file,
            remote_ref=remote_ref,
            remote_python=remote_python,
            auto_execute=auto_execute,
            allow_full_train=allow_full_train,
            control_timeout=control_timeout,
            control_sandbox=control_sandbox,
            control_approval_policy=control_approval_policy,
            control_model=control_model,
            control_reasoning_effort=control_reasoning_effort,
            peer_review_bypass_timeout_seconds=peer_review_bypass_timeout_seconds,
            peer_review_bypass_on=peer_review_bypass_on,
            peer_review_bypass_allowed_classes=peer_review_bypass_allowed_classes,
            trigger="result_gate",
        )
        return {
            "experiment": experiment["name"],
            "action": "local_gate_control",
            "gate": local_gate,
            "remote_sync": _compact_watchdog_sync_result(remote_sync_result) if remote_sync_result else None,
            "control_result": control_result,
            **base_payload,
        }
    if local_gate:
        store.add_experiment_event(
            experiment["id"],
            event_type="watchdog_skipped",
            level="warning",
            message="Watchdog detected a local result gate that needs control, but automatic control is disabled.",
            payload={**base_payload, "gate": local_gate},
            source="mycodex",
            phase="waiting_for_decision",
            phase_message="Result gate requires follow-up, but automatic control is disabled.",
        )
        return {"experiment": experiment["name"], "action": "skipped", "reason": "local_gate_control_disabled", **base_payload}

    ready_tasks = store.list_experiment_tasks(experiment["id"], statuses=["ready"], limit=1)
    if ready_tasks:
        task_result = run_ready_experiment_tasks(
            store,
            project_name,
            experiment["name"],
            owner="mycodex-watchdog",
            lease_seconds=max(60, control_timeout),
            max_tasks=1,
            ssh_key=ssh_key,
            ssh_password_file=ssh_password_file,
            remote_ref=remote_ref,
            remote_python=remote_python,
            allow_full_train=allow_full_train,
        )
        return {
            "experiment": experiment["name"],
            "action": "protocol_task",
            "task_result": task_result,
            "remote_sync": _compact_watchdog_sync_result(remote_sync_result) if remote_sync_result else None,
            **base_payload,
        }

    activity_event = _latest_event_of_types(events, WATCHDOG_ACTIVITY_EVENTS)
    activity_age = None
    if activity_event is not None:
        activity_age = max(0, now - int(activity_event["created_at"]))
    payload = {
        **base_payload,
        "activity_event": activity_event["event_type"] if activity_event else None,
        "activity_event_id": activity_event["id"] if activity_event else None,
        "activity_age_seconds": activity_age,
        "remote_sync": _compact_watchdog_sync_result(remote_sync_result) if remote_sync_result else None,
    }

    store.add_experiment_event(
        experiment["id"],
        event_type="watchdog_heartbeat",
        level="info",
        message="Experiment watchdog checked supervisor freshness.",
        payload=payload,
        source="mycodex",
        phase=experiment["phase"],
        phase_message=experiment["phase_message"],
    )

    if not force and activity_age is not None and activity_age < stale_seconds:
        store.add_experiment_event(
            experiment["id"],
            event_type="watchdog_skipped",
            level="info",
            message=(
                "Watchdog skipped supervisor restart because recent experiment "
                f"activity is only {activity_age}s old."
            ),
            payload=payload,
            source="mycodex",
            phase=experiment["phase"],
            phase_message=experiment["phase_message"],
        )
        return {"experiment": experiment["name"], "action": "skipped", "reason": "fresh", **payload}

    if (
        not force
        and remote_sync_result
        and not resolved_run_path
        and _remote_snapshot_has_active_work(remote_sync_result.get("snapshot"))
    ):
        store.add_experiment_event(
            experiment["id"],
            event_type="watchdog_skipped",
            level="info",
            message="Watchdog skipped supervisor restart because remote snapshot already shows active work.",
            payload=payload,
            source="mycodex",
            phase=experiment["phase"],
            phase_message=experiment["phase_message"],
        )
        return {"experiment": experiment["name"], "action": "skipped", "reason": "remote_active", **payload}

    if not resolved_remote_pid and not resolved_run_path:
        store.add_experiment_event(
            experiment["id"],
            event_type="watchdog_skipped",
            level="warning",
            message="Watchdog found no remote PID or W&B run to supervise.",
            payload=payload,
            source="mycodex",
            phase="waiting_for_decision",
            phase_message="Watchdog needs a remote job or W&B run before it can continue automatically.",
        )
        return {"experiment": experiment["name"], "action": "skipped", "reason": "missing_target", **payload}

    store.add_experiment_event(
        experiment["id"],
        event_type="watchdog_supervise_started",
        level="warning" if activity_age is None else "info",
        message="Watchdog is running one supervisor cycle.",
        payload=payload,
        source="mycodex",
        status="running",
        phase="monitoring_wandb",
        phase_message="Watchdog is verifying whether the experiment needs automatic continuation.",
    )
    try:
        supervisor_result = supervisor_runner(
            store,
            project_name,
            experiment["name"],
            run_path=resolved_run_path,
            metrics=metrics,
            stop_conditions=stop_conditions,
            fail_on_nonfinite=fail_on_nonfinite,
            complete_on_finish=complete_on_finish,
            remote_ref=remote_ref,
            ssh_key=ssh_key,
            ssh_password_file=ssh_password_file,
            remote_python=remote_python,
            remote_pid=resolved_remote_pid or None,
            remote_log_path=resolved_remote_log_path or None,
            interval=supervisor_interval,
            max_cycles=1,
            trigger_next_control=trigger_next_control,
            auto_execute=auto_execute,
            allow_full_train=allow_full_train,
            control_timeout=control_timeout,
            control_sandbox=control_sandbox,
            control_approval_policy=control_approval_policy,
            control_model=control_model,
            control_reasoning_effort=control_reasoning_effort,
            peer_review_bypass_timeout_seconds=peer_review_bypass_timeout_seconds,
            peer_review_bypass_on=peer_review_bypass_on,
            peer_review_bypass_allowed_classes=peer_review_bypass_allowed_classes,
        )
    except Exception as exc:
        store.add_experiment_event(
            experiment["id"],
            event_type="watchdog_supervise_failed",
            level="error",
            message=f"Watchdog supervisor cycle failed: {exc}",
            payload={**payload, "error": str(exc)},
            source="mycodex",
            phase="waiting_for_decision",
            phase_message="Watchdog could not complete the supervisor cycle.",
        )
        raise

    updated_experiment = _require_experiment(store, experiment["project_id"], experiment["name"])
    store.add_experiment_event(
        experiment["id"],
        event_type="watchdog_supervise_completed",
        level="info",
        message="Watchdog supervisor cycle completed.",
        payload={**payload, "supervisor_result": supervisor_result},
        source="mycodex",
        phase=updated_experiment["phase"],
        phase_message=updated_experiment["phase_message"],
    )
    return {
        "experiment": experiment["name"],
        "action": "supervised",
        **payload,
        "supervisor_result": supervisor_result,
    }


def _experiment_active_codex_run_guard(
    store: Store,
    project,
    experiment,
    *,
    timeout_seconds: int,
    now: int,
) -> dict[str, Any]:
    active_runs = store.active_runs_for_project(
        project["id"],
        kind_prefix=f"experiment:{experiment['id']}:",
    )
    if not active_runs:
        return {"active": False}

    stale_runs: list[dict[str, Any]] = []
    for active_run in active_runs:
        age_seconds = max(0, now - int(active_run["started_at"]))
        stale_after = _experiment_codex_run_stale_after(
            str(active_run["kind"]),
            control_timeout_seconds=timeout_seconds,
        )
        payload = {
            "run_id": active_run["id"],
            "kind": active_run["kind"],
            "thread_id": active_run["thread_id"],
            "age_seconds": age_seconds,
            "stale_after_seconds": stale_after,
        }
        if age_seconds < stale_after:
            store.add_experiment_event(
                experiment["id"],
                event_type="experiment_watchdog_skipped_active_control",
                level="info",
                message="Watchdog skipped because an experiment Codex run is still active.",
                payload={**payload, "stale_runs_cleared": stale_runs},
                source="mycodex",
                status="running",
                phase=experiment["phase"],
                phase_message=experiment["phase_message"],
            )
            return {"active": True, "stale_runs_cleared": stale_runs, **payload}

        error = (
            "stale running experiment Codex run exceeded "
            f"{stale_after}s; watchdog will retry"
        )
        store.finish_run(int(active_run["id"]), status="failed", error=error)
        stale_payload = {**payload, "error": error}
        stale_runs.append(stale_payload)
        store.add_experiment_event(
            experiment["id"],
            event_type="experiment_control_run_marked_stale",
            level="warning",
            message="Watchdog marked a stale running experiment Codex run as failed.",
            payload=stale_payload,
            source="mycodex",
            status="running",
            phase="analyzing",
            phase_message="Stale experiment Codex run was cleared; watchdog will retry.",
        )
    return {"active": False, "stale_cleared": bool(stale_runs), "stale_runs": stale_runs}


def _experiment_codex_run_stale_after(kind: str, *, control_timeout_seconds: int) -> int:
    if kind.endswith(":control"):
        return max(60, control_timeout_seconds + 60)
    return max(60, min(control_timeout_seconds, 900) + 60)


def _record_peer_review_skipped(
    store: Store,
    experiment_id: int,
    *,
    contract: dict[str, Any],
    trigger: str,
    payload: dict[str, Any] | None = None,
) -> None:
    execution_class = _execution_class(contract)
    store.add_experiment_event(
        experiment_id,
        event_type="peer_review_skipped",
        level="info",
        message=(
            f"Peer review skipped for {execution_class}: read-only eval collection "
            "is low-risk and reviewer artifact objections must not block eval data."
        ),
        payload={
            "contract": contract,
            "trigger": trigger,
            "policy": "eval-only/smoke-eval do not require peer review",
            **(payload or {}),
        },
        source="mycodex",
        status="running",
        phase=_phase_for_contract_execution(contract),
        phase_message="Eval-only contract does not require peer review; launching eval.",
    )


def _record_peer_review_soft_objections(
    store: Store,
    experiment_id: int,
    *,
    contract: dict[str, Any],
    peer_review_decision: dict[str, Any] | None,
    trigger: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    annotated = _annotate_contract_with_reviewer_objections(
        contract,
        peer_review_decision,
        reason="soft_peer_review_objections_do_not_block_safe_execution",
    )
    store.add_experiment_event(
        experiment_id,
        event_type="peer_review_soft_objections_recorded",
        level="warning",
        message=(
            "Peer review raised non-hard objections; execution may continue, "
            "but method_claim_allowed=false until objections are resolved."
        ),
        payload={
            "decision": peer_review_decision,
            "contract": annotated,
            "trigger": trigger,
            "hard_blockers": [],
            **(payload or {}),
        },
        source="mycodex",
        status="running",
        phase=_phase_for_contract_execution(contract),
        phase_message="Soft reviewer objections recorded; safe execution continues.",
    )
    return annotated


def _pause_with_reviewer_objections(
    store: Store,
    experiment_id: int,
    *,
    peer_review_decision: dict[str, Any] | None,
    revision_attempts: int,
    max_revision_attempts: int,
    trigger: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event_payload = {
        "decision": peer_review_decision,
        "revision_attempt": revision_attempts,
        "max_revision_attempts": max_revision_attempts,
        "trigger": trigger,
        **(payload or {}),
    }
    store.add_experiment_event(
        experiment_id,
        event_type="paused_with_reviewer_objections",
        level="warning",
        message=(
            "Peer review requested revision after the one-revision budget; "
            "pausing instead of opening another Codex review/control loop."
        ),
        payload=event_payload,
        source="mycodex",
        status="paused",
        phase="paused_with_reviewer_objections",
        phase_message="Reviewer objections recorded; automatic revision loop stopped.",
    )
    return {
        "executed": False,
        "reason": "paused_with_reviewer_objections",
        "decision": peer_review_decision,
        "revision_attempt": revision_attempts,
    }


def execute_execution_contract(
    store: Store,
    project_name: str,
    experiment_name: str,
    contract: dict[str, Any],
    *,
    ssh_key: str | None = None,
    ssh_password_file: str | None = None,
    remote_ref: str | None = None,
    remote_python: str | None = None,
    allow_full_train: bool = False,
    protocol_task_id: int | None = None,
    execution_guard: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    project = require_project(store, project_name)
    experiment = _require_experiment(store, project["id"], experiment_name)
    repo = Path(project["path"])
    try:
        _validate_experiment_contract_types(contract)
    except (ValueError, TypeError) as exc:
        store.add_experiment_event(
            experiment["id"], event_type="blocked", level="warning",
            message=f"Execution contract has invalid field types: {exc}",
            payload={"error": str(exc), "protocol_task_id": protocol_task_id}, source="mycodex")
        return {"executed": False, "reason": "contract_invalid", "failures": [str(exc)]}
    execution_class = str(contract.get("execution_class") or "").strip()
    auto_execute_flag = bool(contract.get("auto_execute"))

    if not auto_execute_flag:
        store.add_experiment_event(
            experiment["id"],
            event_type="blocked",
            level="warning",
            message="Execution contract has auto_execute=false.",
            payload={"contract": contract, "protocol_task_id": protocol_task_id},
            source="mycodex",
            phase="waiting_for_decision",
        )
        return {"executed": False, "reason": "auto_execute_false"}

    if execution_class == "full-train" and not allow_full_train:
        store.add_experiment_event(
            experiment["id"],
            event_type="blocked",
            level="warning",
            message="Execution contract requests full-train, but supervisor is not allowed to auto-run full training.",
            payload={"contract": contract, "protocol_task_id": protocol_task_id},
            source="mycodex",
            phase="waiting_for_decision",
        )
        return {"executed": False, "reason": "full_train_not_allowed"}
    if execution_class not in LOW_RISK_EXECUTION_CLASSES and execution_class != "full-train":
        store.add_experiment_event(
            experiment["id"],
            event_type="blocked",
            level="warning",
            message=f"Execution class {execution_class or '<missing>'} is not auto-executable.",
            payload={"contract": contract, "protocol_task_id": protocol_task_id},
            source="mycodex",
            phase="waiting_for_decision",
        )
        return {"executed": False, "reason": "unsupported_execution_class"}

    contract_failures = _contract_execution_failures(contract, allow_full_train=allow_full_train)
    policy = _experiment_config(experiment).get("execution_policy", {})
    if policy is None:
        policy = {}
    if not isinstance(policy, dict):
        contract_failures.append("policy_must_be_object")
        policy = {}
    contract_remote = contract.get("remote") or {}
    policy_command, _ = _prepare_remote_contract_command(str(contract_remote.get("command") or ""),
                                                        _resolve_remote_python(experiment, remote_python))
    contract_failures.extend(execution_policy_failures(
        {**contract, "remote": {**contract_remote, "command": policy_command}}, policy))
    contract_failures.extend(patch_scope_failures(repo, contract.get("patch") or {}, policy))
    resolved_target = remote_ref or experiment["remote_ref"] or _experiment_config(experiment).get("remote_ref")
    if policy_command and policy.get("allowed_remote_refs") is not None:
        if not resolved_target or parse_remote_ref(str(resolved_target)).ref not in policy["allowed_remote_refs"]:
            contract_failures.append("policy_remote_target_not_allowed")
    try:
        check_limit = min(float(policy.get("max_local_check_seconds", 300)),
                          float(contract.get("local_check_timeout_seconds", 300)))
        if not math.isfinite(check_limit) or check_limit <= 0:
            raise ValueError("nonpositive or nonfinite check limit")
    except (ValueError, TypeError):
        contract_failures.append("policy_invalid_max_local_check_seconds")
    if contract_failures:
        store.add_experiment_event(
            experiment["id"],
            event_type="blocked",
            level="warning",
            message="Execution contract is incomplete; refusing to mark a no-op as executed.",
            payload={"contract": contract, "failures": contract_failures, "protocol_task_id": protocol_task_id},
            source="mycodex",
            phase="waiting_for_decision",
            phase_message="Execution contract was incomplete; waiting for a corrected contract.",
        )
        return {
            "executed": False,
            "reason": "contract_incomplete",
            "failures": contract_failures,
        }

    patch_info = contract.get("patch") if isinstance(contract.get("patch"), dict) else {}
    patch_required = bool(patch_info.get("required"))
    files = [str(item) for item in patch_info.get("files") or []]
    if execution_guard is not None and not execution_guard():
        return {"executed": False, "reason": "contract_lease_lost_requires_reconciliation"}
    commit_result: dict[str, Any] | None = None
    patch_applied = False
    diff = ""

    if patch_required:
        staged = _git_text(repo, ["diff", "--cached", "--name-only", "-z"]).split("\0")
        dirty = _git_text(repo, ["diff", "--name-only", "-z"]).split("\0")
        untracked = _git_text(repo, ["ls-files", "--others", "--exclude-standard", "-z"]).split("\0")
        overlaps = sorted(set(files) & (set(dirty) | set(untracked) | set(staged)))
        unrelated_staged = sorted(set(staged) - set(files) - {""})
        if overlaps or unrelated_staged:
            failures = {"dirty_patch_files": overlaps, "unrelated_staged_files": unrelated_staged}
            store.add_experiment_event(
                experiment["id"], event_type="blocked", level="warning",
                message="Patch overlaps existing local changes or an unrelated staged file; refusing to commit user-owned work.",
                payload={**failures, "protocol_task_id": protocol_task_id}, source="mycodex")
            return {"executed": False, "reason": "contract_unowned_local_changes", **failures}
        raw_diff = str(patch_info.get("unified_diff") or patch_info.get("diff") or "")
        if not raw_diff.strip():
            store.add_experiment_event(
                experiment["id"],
                event_type="blocked",
                level="warning",
                message="Patch is required, but the execution contract does not include a unified_diff.",
                payload={"contract": contract, "protocol_task_id": protocol_task_id},
                source="mycodex",
                phase="waiting_for_decision",
            )
            return {"executed": False, "reason": "missing_unified_diff"}
        diff = raw_diff if raw_diff.endswith("\n") else raw_diff + "\n"
        store.add_experiment_event(
            experiment["id"],
            event_type="code_change_started",
            level="info",
            message="Applying unified diff from execution contract.",
            payload={"files": files, "protocol_task_id": protocol_task_id},
            source="mycodex",
            status="running",
            phase="modifying_code",
        )
        _git_apply(repo, diff)
        patch_applied = True

    check_results = _run_local_checks(repo, contract.get("local_checks") or [], contract=contract,
                                     timeout_seconds=check_limit, execution_guard=execution_guard)
    if execution_guard is not None and not execution_guard():
        return {"executed": False, "reason": "contract_lease_lost_requires_reconciliation", "checks": check_results}
    failed_checks = [item for item in check_results if item["returncode"] != 0]
    if failed_checks:
        rollback_result = None
        if patch_applied:
            rollback_result = _rollback_applied_patch(repo, diff)
            store.add_experiment_event(
                experiment["id"],
                event_type="code_change_rolled_back",
                level="info" if rollback_result["rolled_back"] else "warning",
                message=(
                    "Rolled back execution-contract patch after local check failure."
                    if rollback_result["rolled_back"]
                    else "Could not automatically roll back execution-contract patch after local check failure."
                ),
                payload={"rollback": rollback_result, "protocol_task_id": protocol_task_id},
                source="mycodex",
                phase="waiting_for_decision",
            )
        store.add_experiment_event(
            experiment["id"],
            event_type="blocked",
            level="warning",
            message="Local check failed; refusing to launch remote job.",
            payload={"checks": check_results, "rollback": rollback_result, "protocol_task_id": protocol_task_id},
            source="mycodex",
            phase="waiting_for_decision",
        )
        return {
            "executed": False,
            "reason": "local_check_failed",
            "checks": check_results,
            "rollback": rollback_result,
        }

    if patch_required:
        commit_message = str(patch_info.get("commit_message") or "Apply experiment iteration patch")
        try:
            commit_result = git_commit_push(
                store,
                project_name,
                experiment_name,
                commit_message=commit_message,
                paths=files,
            )
        except Exception as exc:
            store.add_experiment_event(
                experiment["id"], event_type="external_execution_reconciliation_required", level="warning",
                message=f"Commit/push outcome requires reconciliation: {exc}",
                payload={"error": str(exc), "protocol_task_id": protocol_task_id}, source="mycodex")
            return {"executed": False, "reason": "contract_commit_requires_reconciliation", "error": str(exc),
                    "checks": check_results}

    remote = contract.get("remote") if isinstance(contract.get("remote"), dict) else {}
    command = str(remote.get("command") or "").strip()
    command, command_env = _prepare_remote_contract_command(
        command,
        _resolve_remote_python(experiment, remote_python),
    )
    remote_command_result: dict[str, Any] | None = None
    launch_result: dict[str, Any] | None = None
    if execution_guard is not None and not execution_guard():
        return {"executed": False, "reason": "contract_lease_lost_requires_reconciliation", "checks": check_results}
    if command and execution_class == "monitor-only":
        resolved_remote_ref = remote_ref or experiment["remote_ref"] or _experiment_config(experiment).get("remote_ref")
        if not resolved_remote_ref:
            store.add_experiment_event(
                experiment["id"],
                event_type="blocked",
                level="warning",
                message="Monitor-only remote command requested but remote_ref is unknown.",
                payload={"contract": contract, "protocol_task_id": protocol_task_id},
                source="mycodex",
                phase="waiting_for_decision",
            )
            return {"executed": False, "reason": "remote_ref_missing", "checks": check_results}
        timeout_value = resolve_time_budget(remote) or 300
        try:
            monitor_timeout = float(timeout_value)
        except (TypeError, ValueError):
            monitor_timeout = 300
        target = parse_remote_ref(str(resolved_remote_ref))
        if policy.get("allowed_remote_refs") is not None and target.ref not in policy["allowed_remote_refs"]:
            return {"executed": False, "reason": "contract_remote_target_not_allowed"}
        monitor_id = validate_execution_id(contract.get("execution_id") or remote.get("execution_id") or "exec-" + canonical_hash({
            "project_id": project["id"], "experiment_id": experiment["id"], "remote_ref": target.ref,
            "protocol_task_id": protocol_task_id, "contract": contract, "command": command})[:32])
        monitor_env = _validate_env(remote.get("env") or {})
        code_commit = str(remote.get("commit") or contract.get("commit") or "") or None
        if code_commit is None and (repo / ".git").exists():
            code_commit = _git_text(repo, ["rev-parse", "HEAD"])
        monitor_manifest = build_execution_manifest(
            execution_id=monitor_id, project_id=project["id"], experiment_id=experiment["id"],
            remote_ref=target.ref, commit=code_commit, command=command, env=monitor_env,
            contract=contract, protocol_task_id=protocol_task_id, time_budget_seconds=monitor_timeout,
            termination_grace_seconds=float(remote.get("termination_grace_seconds", 30)))
        monitor_manifest_path = persist_execution_manifest(store.path.parent / "experiment_artifacts", monitor_manifest)
        manifest_ref = {"execution_id": monitor_id, "manifest_path": str(monitor_manifest_path.resolve())}
        store.add_experiment_event(experiment["id"], event_type="execution_manifest_saved", level="info",
            message=f"Saved immutable monitor execution manifest for {monitor_id}.",
            payload={**manifest_ref, "manifest": monitor_manifest}, source="mycodex")
        store.add_experiment_event(
            experiment["id"],
            event_type="remote_monitor_command_started",
            level="info",
            message="Executing monitor-only remote command.",
            payload={
                "remote_ref": target.ref,
                "timeout": monitor_timeout,
                "command": command,
                **command_env,
                "ssh_auth": "password_file" if ssh_password_file else ("key" if ssh_key else "default"),
                "protocol_task_id": protocol_task_id,
                **manifest_ref,
            },
            source="mycodex",
            status="running",
            phase="remote_preflight_pending",
            phase_message="Running monitor-only remote command.",
        )
        try:
            result = run_remote_script(
                target,
                render_bounded_command(command, monitor_timeout, float(remote.get("termination_grace_seconds", 30)), monitor_env),
                str(Path(ssh_key).expanduser()) if ssh_key else None,
                math.ceil(monitor_timeout + float(remote.get("termination_grace_seconds", 30)) + 10),
                ssh_password_file=str(ssh_password_file) if ssh_password_file else None,
            )
        except Exception as exc:
            store.add_experiment_event(
                experiment["id"],
                event_type="remote_monitor_command_failed",
                level="warning",
                message=f"Monitor-only remote command failed: {exc}",
                payload={"error": str(exc), "command": command, **command_env, "protocol_task_id": protocol_task_id},
                source="mycodex",
                phase="waiting_for_decision",
                phase_message="Monitor-only remote command failed; inspect event payload.",
            )
            return {
                "executed": False,
                "reason": "remote_monitor_command_failed",
                "checks": check_results,
                "error": str(exc),
            }
        remote_command_result = {
            "stdout": result.stdout[-8000:],
            "stderr": result.stderr[-8000:],
            "returncode": result.returncode,
            **manifest_ref,
        }
        if result.returncode != 0:
            store.add_experiment_event(
                experiment["id"], event_type="remote_monitor_command_failed", level="error",
                message=f"Monitor-only remote command exited with {result.returncode}.",
                payload={**remote_command_result, "protocol_task_id": protocol_task_id}, source="mycodex")
            return {"executed": False, "reason": "remote_monitor_command_failed", "checks": check_results,
                    "remote_command": remote_command_result}
        store.add_experiment_event(
            experiment["id"],
            event_type="remote_monitor_command_completed",
            level="info",
            message="Monitor-only remote command completed.",
            payload={**remote_command_result, "command": command, **command_env, "protocol_task_id": protocol_task_id},
            source="mycodex",
            status="running",
            phase="diagnosis_ready",
            phase_message="Monitor-only remote command completed.",
        )
    elif command:
        wandb = contract.get("wandb") if isinstance(contract.get("wandb"), dict) else {}
        launch_tracks_wandb = _contract_produces_wandb_run(contract)
        explicit_remote_commit = str(
            remote.get("commit")
            or remote.get("code_commit")
            or contract.get("commit")
            or contract.get("code_commit")
            or ""
        ).strip()
        commit = commit_result["commit"] if commit_result else (explicit_remote_commit or _git_text(repo, ["rev-parse", "HEAD"]))
        allow_dirty_launch = bool(remote.get("allow_dirty") or contract.get("allow_dirty"))
        eval_uses_explicit_commit = (
            execution_class in EVAL_EXECUTION_CLASSES
            and not patch_required
            and bool(explicit_remote_commit)
        )
        if eval_uses_explicit_commit:
            local_status = _git_text(repo, ["status", "--short"]).splitlines()
            if local_status:
                store.add_experiment_event(
                    experiment["id"],
                    event_type="dirty_worktree_ignored_for_eval",
                    level="info",
                    message=(
                        "Local worktree is dirty, but eval-only launch uses an explicit commit; "
                        "dirty state will not block remote eval collection."
                    ),
                    payload={
                        "execution_class": execution_class,
                        "commit": explicit_remote_commit,
                        "status": local_status,
                    },
                    source="mycodex",
                    status="running",
                    phase="launch_eval",
                    phase_message="Launching eval from explicit commit despite local dirty worktree.",
                )
        try:
            launch_result = launch_remote_job(
                store,
                project_name,
                experiment_name,
                command=command,
                remote_ref=remote_ref,
                ssh_key=ssh_key,
                ssh_password_file=ssh_password_file,
                commit=commit,
                require_clean=not (commit_result or allow_dirty_launch or eval_uses_explicit_commit),
                env=remote.get("env") if isinstance(remote.get("env"), dict) else {},
                wandb_entity=wandb.get("entity") if launch_tracks_wandb else None,
                wandb_project=wandb.get("project") if launch_tracks_wandb else None,
                wandb_run_id=wandb.get("run_id") if launch_tracks_wandb else None,
                wandb_run_name=wandb.get("run_name") if launch_tracks_wandb else None,
                timeout=180,
                execution_id=contract.get("execution_id") or remote.get("execution_id"),
                execution_contract=contract,
                protocol_task_id=protocol_task_id,
                time_budget_seconds=resolve_time_budget(remote),
                termination_grace_seconds=float(remote.get("termination_grace_seconds", 30)),
            )
        except Exception as exc:
            store.add_experiment_event(
                experiment["id"], event_type="external_execution_reconciliation_required", level="warning",
                message=f"Remote launch requires reconciliation before retry: {exc}",
                payload={"error": str(exc), "protocol_task_id": protocol_task_id}, source="mycodex",
                phase="waiting_for_decision")
            return {"executed": False, "reason": "contract_remote_launch_requires_reconciliation",
                    "error": str(exc), "checks": check_results, "commit": commit_result}
        post_check = launch_result.get("post_launch_check") or {}
        if post_check.get("status") == "needs_reconciliation":
            return {"executed": False, "reason": "contract_remote_execution_needs_reconciliation",
                    "launch": launch_result, "checks": check_results, "commit": commit_result}
        if post_check.get("status") in {"failed", "timed_out"} or (post_check.get("status") == "not_running" and _remote_output_has_failure_hint(post_check)):
            return {"executed": False, "reason": "remote_job_exited_early", "launch": launch_result,
                    "checks": check_results, "commit": commit_result}

    store.add_experiment_event(
        experiment["id"],
        event_type="supervisor_action_completed",
        level="info",
        message="Supervisor executed the control contract.",
        payload={
            "commit": commit_result,
            "checks": check_results,
            "launch": launch_result,
            "remote_command": remote_command_result,
            "protocol_task_id": protocol_task_id,
        },
        source="mycodex",
        status="running",
        phase=(
            "launch_eval"
            if launch_result and execution_class in EVAL_EXECUTION_CLASSES
            else "experiment_running"
            if launch_result
            else ("diagnosis_ready" if remote_command_result else "monitoring_wandb")
        ),
    )
    return {
        "executed": True,
        "commit": commit_result,
        "checks": check_results,
        "launch": launch_result,
        "remote_command": remote_command_result,
    }


def record_execution_result(
    store: Store, project_name: str, experiment_name: str, execution_id: str,
    result: dict[str, Any],
) -> dict[str, Any]:
    """Record supplied result metadata; verify files and use only observed remote receipts."""
    project = require_project(store, project_name)
    experiment = _require_experiment(store, project["id"], experiment_name)
    root = store.path.parent / "experiment_artifacts"
    manifest = read_execution_manifest(root, execution_id)
    if manifest["project_id"] != project["id"] or manifest["experiment_id"] != experiment["id"]:
        raise ValueError("execution manifest belongs to a different project or experiment")
    checked = dict(result)
    checked["execution_id"] = execution_id
    receipt = None
    for event in store.list_experiment_events(experiment["id"], limit=500):
        payload = _event_payload(event)
        if payload.get("execution_id") == execution_id and payload.get("receipt_verified"):
            candidate = payload.get("receipt") or {}
            if (candidate.get("status") in {"completed", "failed", "timed_out"}
                    and candidate.get("execution_id") == execution_id
                    and candidate.get("manifest_sha256") == canonical_hash(manifest)):
                receipt = candidate
                break
    checked["receipt_verified"] = receipt is not None
    if receipt:
        checked["status"] = receipt.get("status")
        checked["exit_code"] = receipt.get("exit_code")
    artifact_checks = []
    for artifact in result.get("artifact_refs") or []:
        if not isinstance(artifact, dict) or not artifact.get("path") or not artifact.get("sha256"):
            artifact_checks.append({"verified": False, "reason": "local path and sha256 required"})
            continue
        path = Path(str(artifact["path"])).expanduser()
        digest = hashlib.sha256()
        try:
            if not path.is_file():
                raise OSError("result artifact is not a regular file")
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
            artifact_checks.append({"path": str(path.resolve()), "sha256": digest.hexdigest(),
                                    "verified": digest.hexdigest() == artifact["sha256"]})
        except OSError as exc:
            artifact_checks.append({"path": str(path), "verified": False, "reason": str(exc)})
    checked["artifact_verification"] = artifact_checks
    checked["artifacts_verified"] = bool(artifact_checks) and all(item["verified"] for item in artifact_checks)
    gate = assess_result_evidence(manifest, checked)
    artifact_path = persist_execution_result(root, manifest, checked, gate)
    store.add_experiment_event(
        experiment["id"], event_type="result_gate_parsed", level="info",
        message=f"Result evidence assessed as {gate['evidence_level']} for {execution_id}.",
        payload={**gate, "result_path": str(artifact_path.resolve()), "artifact_verification": artifact_checks},
        source="mycodex")
    return {**gate, "result_path": str(artifact_path.resolve()), "artifact_verification": artifact_checks}


def fetch_wandb_run(run_ref: WandbRunRef, metrics: list[str], history_limit: int) -> dict[str, Any]:
    try:
        import wandb
    except ImportError as exc:
        raise RuntimeError("W&B polling requires the `wandb` package in the mycodex environment") from exc

    api = wandb.Api()
    run = api.run(run_ref.path)
    summary = _jsonable(dict(run.summary))
    selected_summary = _select_summary(summary, metrics)
    latest_metrics = {
        metric: summary[metric]
        for metric in metrics
        if metric in summary and _is_json_scalar(summary[metric])
    }
    history_tail: list[dict[str, Any]] = []
    history_error = ""
    if metrics and history_limit > 0:
        keys = list(dict.fromkeys(["_step", *metrics]))
        rows: deque[dict[str, Any]] = deque(maxlen=history_limit)
        try:
            for row in run.scan_history(keys=keys, page_size=1000):
                rows.append(_jsonable(row))
        except Exception as exc:  # W&B history can fail independently of summary polling.
            history_error = str(exc)
        history_tail = list(rows)
        if history_tail:
            latest_row = history_tail[-1]
            for metric in metrics:
                if metric in latest_row and _is_json_scalar(latest_row[metric]):
                    latest_metrics[metric] = latest_row[metric]

    latest_step = summary.get("_step")
    if history_tail and "_step" in history_tail[-1]:
        latest_step = history_tail[-1]["_step"]
    return {
        "run_path": run_ref.path,
        "entity": run_ref.entity,
        "wandb_project": run_ref.project,
        "run_id": run_ref.run_id,
        "name": getattr(run, "name", ""),
        "state": getattr(run, "state", "unknown"),
        "url": getattr(run, "url", ""),
        "latest_step": latest_step,
        "latest_metrics": latest_metrics,
        "summary": selected_summary,
        "history_tail": history_tail,
        "history_error": history_error,
        "polled_at": int(time.time()),
    }


def evaluate_stop_conditions(metrics: dict[str, Any], conditions: list[str]) -> list[str]:
    matched: list[str] = []
    for condition in conditions:
        match = _COND_RE.match(condition)
        if not match:
            raise ValueError(f"invalid stop condition {condition!r}; expected metric>=value")
        metric = match.group("metric")
        if metric not in metrics:
            continue
        try:
            observed = float(metrics[metric])
        except (TypeError, ValueError):
            continue
        if not math.isfinite(observed):
            continue
        target = float(match.group("value"))
        op = match.group("op")
        ok = {
            ">": observed > target,
            ">=": observed >= target,
            "<": observed < target,
            "<=": observed <= target,
            "==": observed == target,
            "!=": observed != target,
        }[op]
        if ok:
            matched.append(condition)
    return matched


def run_remote_script(
    target: RemoteTarget,
    script: str,
    ssh_key: str | None,
    timeout: int,
    *,
    ssh_password_file: str | None = None,
) -> RemoteExecResult:
    if ssh_password_file:
        return _run_remote_script_with_password(target, script, ssh_password_file, timeout)

    args = _ssh_args(target, ssh_key)
    args.extend([target.login, "bash", "-s"])
    completed = subprocess.run(
        args,
        input=script,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    result = RemoteExecResult(
        stdout=completed.stdout,
        stderr=completed.stderr,
        returncode=completed.returncode,
    )
    if completed.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RemoteCommandError(f"remote command failed with exit {completed.returncode}: {detail}", result)
    return result


def _strip_password_prompts(text: str) -> str:
    return _PASSWORD_PROMPT_RE.sub("", text)


def _run_remote_script_with_password(
    target: RemoteTarget,
    script: str,
    ssh_password_file: str,
    timeout: int,
) -> RemoteExecResult:
    expect_bin = shutil.which("expect")
    if not expect_bin:
        raise RuntimeError("password SSH requires `expect` to be installed")

    password_path = Path(ssh_password_file).expanduser()
    if not password_path.exists():
        raise FileNotFoundError(f"SSH password file not found: {password_path}")

    encoded_script = base64.b64encode(script.encode("utf-8")).decode("ascii")
    remote_command = f"printf %s {shlex.quote(encoded_script)} | base64 -d | bash"
    args = _ssh_args(target, None, batch_mode=False, password_auth=True)
    args.extend([target.login, remote_command])

    expect_script = r"""
set timeout $env(MYCODEX_REMOTE_TIMEOUT)
set password_file $env(MYCODEX_SSH_PASSWORD_FILE)

set pf [open $password_file r]
set password [string trim [read $pf]]
close $pf

spawn -noecho /bin/sh -c $env(MYCODEX_EXPECT_SSH_COMMAND)
expect {
  "*Are you sure you want to continue connecting*" {
    send "yes\r"
    exp_continue
  }
  "*password:*" {
    send -- "$password\r"
    exp_continue
  }
  timeout {
    exit 124
  }
  eof {}
}
catch wait result
exit [lindex $result 3]
"""
    env = {
        **os.environ,
        "MYCODEX_REMOTE_TIMEOUT": str(max(1, timeout)),
        "MYCODEX_SSH_PASSWORD_FILE": str(password_path),
        "MYCODEX_EXPECT_SSH_COMMAND": shlex.join(args),
    }
    completed = subprocess.run(
        [expect_bin, "-c", expect_script, *args],
        text=True,
        capture_output=True,
        timeout=timeout + 10,
        check=False,
        env=env,
    )

    result = RemoteExecResult(
        stdout=_strip_password_prompts(completed.stdout),
        stderr=_strip_password_prompts(completed.stderr),
        returncode=completed.returncode,
    )
    if completed.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RemoteCommandError(f"remote command failed with exit {completed.returncode}: {detail}", result)
    return result


def _remote_launch_script(
    *,
    target: RemoteTarget,
    command: str,
    commit: str | None,
    branch: str | None,
    fetch: bool,
    log_path: str,
    pid_path: str,
    env: dict[str, str],
    execution_id: str | None = None,
    manifest: dict[str, Any] | None = None,
    time_budget_seconds: float | None = None,
    termination_grace_seconds: float = 30,
) -> str:
    identity = execution_id or "exec-" + canonical_hash({"target": target.ref, "command": command,
                                                        "commit": commit, "env": env})[:32]
    return render_remote_launch_script(
        workdir=target.path, execution_id=identity, command=command, commit=commit,
        branch=branch, fetch=fetch, log_path=log_path, pid_path=pid_path, env=env,
        manifest=manifest or {"execution_id": identity}, time_budget_seconds=time_budget_seconds,
        termination_grace_seconds=termination_grace_seconds,
    )


def _remote_status_script(pid: str, log_path: str, lines: int, *, receipt_path: str = "") -> str:
    script = f"""set -euo pipefail
PID={shlex.quote(pid)}
LOG_PATH={shlex.quote(log_path)}
LINES={shlex.quote(str(lines))}
if kill -0 "$PID" 2>/dev/null; then
  STATUS=running
else
  STATUS=not_running
fi
printf 'REMOTE_STATUS=%s\\n' "$STATUS"
if [ -n "$LOG_PATH" ] && [ -f "$LOG_PATH" ]; then
  printf 'REMOTE_LOG_TAIL_START\\n'
  tail -n "$((LINES * 5))" "$LOG_PATH" \
    | tr '\\r' '\\n' \
    | awk '
        /[0-9]+%\\|/ || /it\\/s/ {{ next }}
        length($0) > 1000 {{ print substr($0, 1, 1000) "..."; next }}
        {{ print }}
      ' \
    | tail -n "$LINES" || true
  printf 'REMOTE_LOG_TAIL_END\\n'
fi
"""
    if receipt_path:
        script += "python3 -c " + shlex.quote(REMOTE_RECEIPT_OBSERVATION_SOURCE) + " " + shlex.quote(receipt_path) + "\n"
    return script


def _ssh_args(
    target: RemoteTarget,
    ssh_key: str | None,
    *,
    batch_mode: bool = True,
    password_auth: bool = False,
) -> list[str]:
    args = ["ssh", "-o", "ConnectTimeout=10"]
    if batch_mode:
        args.extend(["-o", "BatchMode=yes"])
    if password_auth:
        args.extend([
            "-o",
            "BatchMode=no",
            "-o",
            "PreferredAuthentications=password",
            "-o",
            "PubkeyAuthentication=no",
            "-o",
            "NumberOfPasswordPrompts=1",
        ])
    if ssh_key:
        args.extend(["-i", str(Path(ssh_key).expanduser()), "-o", "IdentitiesOnly=yes"])
    if target.port is not None:
        args.extend(["-p", str(target.port)])
    return args


def _parse_key_values(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.startswith("REMOTE_"):
            values[key] = value
    return values


def _call_remote_runner(
    remote_runner: RemoteRunner | None,
    target: RemoteTarget,
    script: str,
    ssh_key: str | None,
    timeout: int,
    *,
    ssh_password_file: str | None = None,
) -> RemoteExecResult:
    if remote_runner is not None:
        return remote_runner(target, script, ssh_key, timeout)
    return run_remote_script(
        target,
        script,
        ssh_key,
        timeout,
        ssh_password_file=str(ssh_password_file) if ssh_password_file else None,
    )


def _resolve_remote_state_request(
    experiment,
    latest_job: dict[str, Any],
    *,
    remote_ref: str | None,
    remote_state_root: str | None,
    remote_state_path: str | None,
    remote_events_path: str | None,
    remote_base: str | None,
    remote_python: str | None,
) -> dict[str, Any]:
    config = _experiment_config(experiment)
    remote_state = config.get("remote_state") if isinstance(config.get("remote_state"), dict) else {}
    latest_log_path = (
        latest_job.get("log_path")
        or latest_job.get("outer_log")
        or latest_job.get("remote_log")
        or latest_job.get("remote_log_path")
    )
    latest_pid = latest_job.get("remote_pid") or latest_job.get("pid")
    root = (
        remote_state_root
        or remote_state.get("root")
        or remote_state.get("supervisor_root")
        or latest_job.get("supervisor_root")
        or latest_job.get("controller_root")
        or latest_job.get("remote_state_root")
    )
    state_path = (
        remote_state_path
        or remote_state.get("state_path")
        or latest_job.get("state_path")
        or (f"{root}/state.json" if root else "")
    )
    events_path = (
        remote_events_path
        or remote_state.get("events_path")
        or latest_job.get("events_path")
        or (f"{root}/events.jsonl" if root else "")
    )
    base = (
        remote_base
        or remote_state.get("base")
        or remote_state.get("base_root")
        or latest_job.get("base_root")
        or latest_job.get("remote_base")
        or _parent_path(root)
        or _parent_path(_parent_path(str(latest_log_path or "")))
    )
    return {
        "remote_ref": (
            remote_ref
            or remote_state.get("remote_ref")
            or experiment["remote_ref"]
            or config.get("remote_ref")
        ),
        "root": str(root or ""),
        "state_path": str(state_path or ""),
        "events_path": str(events_path or ""),
        "base": str(base or ""),
        "python": str(remote_python or remote_state.get("python") or config.get("remote_python") or ""),
        "latest_log_path": str(latest_log_path or ""),
        "latest_pid": str(latest_pid or ""),
        "default_eval_epochs": _config_list(
            remote_state.get("summary_eval_epochs")
            or remote_state.get("eval_epochs")
            or config.get("summary_eval_epochs")
            or config.get("eval_epochs")
            or latest_job.get("eval_epochs")
        ),
        "default_eval_envs": _config_list(
            remote_state.get("summary_eval_envs")
            or remote_state.get("eval_envs")
            or config.get("summary_eval_envs")
            or config.get("eval_envs")
            or latest_job.get("eval_envs")
        ),
    }


def _latest_remote_state_hint(store: Store, experiment_id: int) -> dict[str, Any]:
    candidates: list[tuple[int, int, dict[str, Any]]] = []
    for event_type in REMOTE_STATE_HINT_EVENT_TYPES:
        event = store.get_latest_experiment_event(experiment_id, event_type)
        hint = _remote_state_hint_from_event(event)
        if hint:
            candidates.append((int(event["created_at"]), int(event["id"]), hint))
    if candidates:
        return max(candidates, key=lambda item: (item[0], item[1]))[2]

    noisy_types = {"agent_message_delta", "codex_event", "codex_tool"}
    for event in store.list_experiment_events(experiment_id, limit=5000):
        if event["event_type"] in noisy_types:
            continue
        hint = _remote_state_hint_from_event(event)
        if hint:
            return hint
    return {}


def _merge_latest_job_with_remote_state_hint(
    latest_job: dict[str, Any],
    latest_state_hint: dict[str, Any],
) -> dict[str, Any]:
    if not latest_state_hint:
        return dict(latest_job)
    latest_job_run_id = _remote_job_run_id(latest_job)
    hinted_run_id = _remote_job_run_id(latest_state_hint)
    if latest_job_run_id and hinted_run_id and latest_job_run_id != hinted_run_id:
        return dict(latest_job)
    return {**latest_state_hint, **latest_job}


def _remote_snapshot_binding_mismatch(
    snapshot: dict[str, Any],
    latest_job: dict[str, Any],
    latest_job_event,
) -> dict[str, Any]:
    latest_job_run_id = _remote_job_run_id(latest_job)
    snapshot_run_id = _remote_snapshot_run_id(snapshot)
    if not latest_job_run_id or not snapshot_run_id or latest_job_run_id == snapshot_run_id:
        return {}
    return {
        "latest_job_event_id": latest_job_event["id"] if latest_job_event else None,
        "latest_job_run_id": latest_job_run_id,
        "snapshot_run_id": snapshot_run_id,
        "snapshot_root": str(snapshot.get("root") or ""),
        "snapshot_state_path": str((snapshot.get("paths") or {}).get("state") or ""),
        "latest_job_remote_pid": str(latest_job.get("remote_pid") or latest_job.get("pid") or ""),
        "latest_job_log_path": str(
            latest_job.get("log_path")
            or latest_job.get("outer_log")
            or latest_job.get("remote_log")
            or latest_job.get("remote_log_path")
            or ""
        ),
    }


def _remote_job_run_id(payload: dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in (
        "run_id",
        "new_run_id",
        "correct_run_id",
        "train_run_id",
        "wandb_run_id",
        "wandb_run_name",
    ):
        value = payload.get(key)
        if value:
            return str(value)
    return ""


def _remote_snapshot_run_id(snapshot: dict[str, Any]) -> str:
    if not isinstance(snapshot, dict):
        return ""
    current = snapshot.get("current") if isinstance(snapshot.get("current"), dict) else {}
    state = snapshot.get("state") if isinstance(snapshot.get("state"), dict) else {}
    state_current = state.get("current") if isinstance(state.get("current"), dict) else {}
    for value in (
        snapshot.get("run_id"),
        current.get("run_id"),
        current.get("train_run_id"),
        state.get("run_id"),
        state_current.get("run_id"),
        state_current.get("train_run_id"),
    ):
        if value:
            return str(value)
    return ""


def _remote_state_hint_from_event(event) -> dict[str, Any]:
    if event is None:
        return {}
    payload = _event_payload(event)
    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    current = snapshot.get("current") if isinstance(snapshot.get("current"), dict) else {}
    root = (
        payload.get("remote_state_root")
        or payload.get("supervisor_root")
        or payload.get("controller_root")
        or payload.get("supervisor")
        or payload.get("controller")
        or snapshot.get("root")
    )
    if not root:
        return {}
    run_id = (
        payload.get("run_id")
        or payload.get("new_run_id")
        or payload.get("correct_run_id")
        or payload.get("next_run")
        or snapshot.get("run_id")
        or current.get("run_id")
        or current.get("train_run_id")
    )
    eval_label = (
        payload.get("eval_label")
        or payload.get("new_eval_label")
        or payload.get("correct_eval_label")
        or snapshot.get("eval_label")
        or current.get("eval_label")
        or current.get("eval_run_id")
    )
    hint = {
        "remote_state_root": str(root),
        "supervisor_root": str(root),
        "base_root": _parent_path(str(root)),
        "remote_state_event_id": event["id"],
        "remote_state_event_type": event["event_type"],
    }
    if run_id:
        hint["run_id"] = str(run_id)
    if eval_label:
        hint["eval_label"] = str(eval_label)
    for key in ("train_pid", "python_pid", "queue_pid", "supervisor_pid", "watchdog_pid"):
        if payload.get(key):
            hint[key] = payload.get(key)
    return hint


def _watchdog_has_remote_state_target(
    experiment,
    *,
    remote_state_root: str | None,
    remote_state_path: str | None,
    remote_events_path: str | None,
    remote_base: str | None,
) -> bool:
    if remote_state_root or remote_state_path or remote_events_path or remote_base:
        return True
    config = _experiment_config(experiment)
    remote_state = config.get("remote_state") if isinstance(config.get("remote_state"), dict) else {}
    return any(
        remote_state.get(key)
        for key in ("root", "supervisor_root", "state_path", "events_path", "base", "base_root")
    )


def _parent_path(value: str | None) -> str:
    value = str(value or "").rstrip("/")
    if not value or "/" not in value:
        return ""
    parent = value.rsplit("/", 1)[0]
    return parent or "/"


def _remote_state_sync_script(
    *,
    root: str,
    state_path: str,
    events_path: str,
    base: str,
    remote_python: str,
    latest_log_path: str,
    latest_pid: str,
    default_eval_epochs: list[str],
    default_eval_envs: list[str],
    event_tail: int,
    log_tail_lines: int,
) -> str:
    header = "\n".join(
        [
            "set -u",
            f"REMOTE_SYNC_ROOT={shlex.quote(root)}",
            f"REMOTE_SYNC_STATE_PATH={shlex.quote(state_path)}",
            f"REMOTE_SYNC_EVENTS_PATH={shlex.quote(events_path)}",
            f"REMOTE_SYNC_BASE={shlex.quote(base)}",
            f"REMOTE_SYNC_PYTHON={shlex.quote(remote_python)}",
            f"REMOTE_SYNC_LATEST_LOG_PATH={shlex.quote(latest_log_path)}",
            f"REMOTE_SYNC_LATEST_PID={shlex.quote(latest_pid)}",
            f"REMOTE_SYNC_DEFAULT_EVAL_EPOCHS={shlex.quote(' '.join(default_eval_epochs))}",
            f"REMOTE_SYNC_DEFAULT_EVAL_ENVS={shlex.quote(' '.join(default_eval_envs))}",
            f"REMOTE_SYNC_EVENT_TAIL={shlex.quote(str(event_tail))}",
            f"REMOTE_SYNC_LOG_TAIL_LINES={shlex.quote(str(log_tail_lines))}",
            "export REMOTE_SYNC_ROOT REMOTE_SYNC_STATE_PATH REMOTE_SYNC_EVENTS_PATH REMOTE_SYNC_BASE",
            "export REMOTE_SYNC_PYTHON REMOTE_SYNC_LATEST_LOG_PATH REMOTE_SYNC_LATEST_PID",
            "export REMOTE_SYNC_DEFAULT_EVAL_EPOCHS REMOTE_SYNC_DEFAULT_EVAL_ENVS",
            "export REMOTE_SYNC_EVENT_TAIL REMOTE_SYNC_LOG_TAIL_LINES",
            "PYBIN=\"\"",
            "if [ -n \"$REMOTE_SYNC_PYTHON\" ] && [ -x \"$REMOTE_SYNC_PYTHON\" ]; then PYBIN=\"$REMOTE_SYNC_PYTHON\"; fi",
            "if [ -z \"$PYBIN\" ] && [ -x /root/miniconda3/envs/dpt/bin/python3 ]; then PYBIN=/root/miniconda3/envs/dpt/bin/python3; fi",
            "if [ -z \"$PYBIN\" ] && command -v python3 >/dev/null 2>&1; then PYBIN=$(command -v python3); fi",
            "if [ -z \"$PYBIN\" ] && [ -x /usr/bin/python3 ]; then PYBIN=/usr/bin/python3; fi",
            "if [ -z \"$PYBIN\" ]; then printf '%s\\n' '{\"sync_status\":\"failed\",\"error\":\"python_not_found\"}'; exit 0; fi",
            "\"$PYBIN\" - <<'PY_REMOTE_SYNC'",
        ]
    )
    python_code = r'''
import collections
import csv
import hashlib
import json
import os
import signal
import subprocess
import time

def env(name, default=""):
    return os.environ.get(name, default)

def norm(path):
    return os.path.abspath(path) if path else ""

def safe_stat(path):
    path = norm(path)
    info = {"path": path, "exists": False}
    if not path:
        return info
    try:
        st = os.stat(path)
    except OSError as exc:
        info["error"] = str(exc)
        return info
    info.update({
        "exists": True,
        "size": st.st_size,
        "mtime": st.st_mtime,
    })
    return info

def read_text(path, limit=200000):
    path = norm(path)
    if not path:
        return ""
    try:
        with open(path, "rb") as fh:
            if limit and os.path.getsize(path) > limit:
                fh.seek(-limit, os.SEEK_END)
            data = fh.read()
    except OSError:
        return ""
    return data.decode("utf-8", "replace")

def read_json(path):
    text = read_text(path, limit=1000000)
    if not text.strip():
        return {}
    try:
        parsed = json.loads(text)
    except Exception as exc:
        return {"_parse_error": str(exc)}
    return parsed if isinstance(parsed, dict) else {"_value": parsed}

def sha(text):
    return hashlib.sha256((text or "").encode("utf-8", "replace")).hexdigest()

def read_boot_id():
    return read_text("/proc/sys/kernel/random/boot_id", limit=200).strip()

def read_uptime_seconds():
    text = read_text("/proc/uptime", limit=200).strip()
    if not text:
        return None
    try:
        return float(text.split()[0])
    except Exception:
        return None

def hostname():
    try:
        return os.uname().nodename
    except Exception:
        return ""

def tail_text(path, lines):
    text = read_text(path, limit=max(20000, lines * 800))
    cleaned = text.replace("\r", "\n")
    kept = []
    for line in cleaned.splitlines():
        if "%|" in line and "it/s" in line:
            continue
        kept.append(line[:1000])
    return "\n".join(kept[-lines:])

def tail_events(path, max_lines):
    path = norm(path)
    if not path:
        return []
    rows = collections.deque(maxlen=max_lines)
    count = 0
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for count, line in enumerate(fh, start=1):
                raw = line.rstrip("\n")
                rows.append((count, raw))
    except OSError:
        return []
    result = []
    for line_number, raw in rows:
        parsed = None
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = None
        result.append({
            "line_number": line_number,
            "raw": raw,
            "sha256": sha(raw),
            "parsed": parsed,
        })
    return result

def parse_env_text(text):
    result = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = value.strip().strip("\"'")
    return result

def pid_alive(pid):
    try:
        pid_int = int(str(pid).strip())
    except Exception:
        return False
    if pid_int <= 0:
        return False
    try:
        os.kill(pid_int, 0)
    except OSError:
        return False
    return True

def pid_info(path):
    text = read_text(path, limit=200).strip()
    return {"path": norm(path), "pid": text, "alive": pid_alive(text)}

def maybe_path(*parts):
    if not parts or not parts[0]:
        return ""
    return os.path.join(*parts)

def csv_rows(path):
    path = norm(path)
    if not path or not os.path.exists(path):
        return 0
    try:
        with open(path, newline="", encoding="utf-8", errors="replace") as fh:
            sample = fh.read(4096)
            fh.seek(0)
            delimiter = "\t" if path.endswith(".tsv") or sample.count("\t") > sample.count(",") else ","
            rows = [row for row in csv.reader(fh, delimiter=delimiter) if any(cell.strip() for cell in row)]
    except Exception:
        return 0
    if not rows:
        return 0
    header_names = {
        "env",
        "epoch",
        "score",
        "reward",
        "label",
        "variant",
        "eval_env",
        "model_epoch",
        "avg_reward",
        "std_reward",
        "episodes",
        "checkpoint",
        "checkpoint_path",
    }
    header_like = any(cell.strip().lower() in header_names for cell in rows[0])
    return max(0, len(rows) - (1 if header_like else 0))

def split_list(value):
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    return [item for item in str(value or "").replace(",", " ").split() if item]

def discover_state_root(base, current_root, current_state_path, current_events_path):
    if current_state_path and os.path.exists(current_state_path):
        return current_root, current_state_path, current_events_path
    if current_events_path and os.path.exists(current_events_path):
        return current_root, current_state_path, current_events_path
    base = norm(base)
    if not base or not os.path.isdir(base):
        return current_root, current_state_path, current_events_path
    candidates = []
    try:
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [name for name in dirnames if not name.startswith(".")]
            if "state.json" not in filenames and "events.jsonl" not in filenames:
                continue
            state_candidate = os.path.join(dirpath, "state.json")
            events_candidate = os.path.join(dirpath, "events.jsonl")
            mtimes = []
            for candidate in (state_candidate, events_candidate):
                try:
                    mtimes.append(os.path.getmtime(candidate))
                except OSError:
                    pass
            if mtimes:
                candidates.append((max(mtimes), dirpath, state_candidate, events_candidate))
    except OSError:
        return current_root, current_state_path, current_events_path
    if not candidates:
        return current_root, current_state_path, current_events_path
    _, root_candidate, state_candidate, events_candidate = max(candidates, key=lambda item: item[0])
    return norm(root_candidate), norm(state_candidate), norm(events_candidate)

root = norm(env("REMOTE_SYNC_ROOT"))
state_path = norm(env("REMOTE_SYNC_STATE_PATH") or (os.path.join(root, "state.json") if root else ""))
events_path = norm(env("REMOTE_SYNC_EVENTS_PATH") or (os.path.join(root, "events.jsonl") if root else ""))
base = norm(env("REMOTE_SYNC_BASE"))
latest_log_path = norm(env("REMOTE_SYNC_LATEST_LOG_PATH"))
latest_pid = env("REMOTE_SYNC_LATEST_PID")
event_tail = int(env("REMOTE_SYNC_EVENT_TAIL", "200") or "200")
log_tail_lines = int(env("REMOTE_SYNC_LOG_TAIL_LINES", "120") or "120")
default_eval_epochs = split_list(env("REMOTE_SYNC_DEFAULT_EVAL_EPOCHS"))
default_eval_envs = split_list(env("REMOTE_SYNC_DEFAULT_EVAL_ENVS"))

if not base and root:
    base = os.path.dirname(root)
if not base and latest_log_path:
    base = os.path.dirname(os.path.dirname(latest_log_path))
root, state_path, events_path = discover_state_root(base, root, state_path, events_path)
state = read_json(state_path)
current = state.get("current") if isinstance(state.get("current"), dict) else {}
run_id = str(current.get("run_id") or current.get("train_run_id") or state.get("run_id") or "")
eval_label = str(current.get("eval_label") or current.get("eval_run_id") or state.get("eval_label") or "")
run_root = norm(current.get("run_root") or (os.path.join(base, run_id) if base and run_id else ""))
eval_root = norm(current.get("eval_root") or (os.path.join(base, eval_label) if base and eval_label else ""))
queue_root = norm(current.get("queue_root") or (os.path.join(base, "followup-" + run_id) if base and run_id else ""))

contract_path = norm(current.get("contract_path") or maybe_path(queue_root, "contract.env") or maybe_path(root, "contract.env"))
command_path = norm(current.get("command_path") or maybe_path(queue_root, "command.sh") or maybe_path(root, "command.sh"))
summary_candidates = [
    current.get("summary_path"),
    maybe_path(eval_root, "combined_summary.tsv"),
    maybe_path(eval_root, "summary.tsv"),
    maybe_path(eval_root, "summary.csv"),
    maybe_path(queue_root, "summary.tsv"),
    maybe_path(queue_root, "summary.csv"),
]
summary_path = ""
for candidate in summary_candidates:
    candidate = norm(candidate)
    if candidate and os.path.exists(candidate):
        summary_path = candidate
        break
if not summary_path:
    summary_path = norm(summary_candidates[0] or maybe_path(eval_root, "combined_summary.tsv") or maybe_path(eval_root, "summary.csv"))
train_log_path = norm(current.get("train_log_path") or latest_log_path or maybe_path(run_root, "launch.log") or maybe_path(run_root, "train.log"))
queue_log_path = norm(current.get("queue_log_path") or maybe_path(queue_root, "followup_queue.log") or maybe_path(queue_root, "queue.log"))
supervisor_log_path = norm(current.get("supervisor_log_path") or maybe_path(root, "supervisor.log"))
watchdog_log_path = norm(current.get("watchdog_log_path") or maybe_path(root, "watchdog.log"))

contract_text = read_text(contract_path, limit=100000)
contract_env = parse_env_text(contract_text)
command_text = read_text(command_path, limit=200000)
eval_epochs = split_list(current.get("eval_epochs") or contract_env.get("EVAL_EPOCHS") or contract_env.get("eval_epochs") or default_eval_epochs)
eval_envs = split_list(current.get("eval_envs") or contract_env.get("EVAL_ENVS") or contract_env.get("HELDOUT_ENVS") or default_eval_envs)
expected_rows = len(eval_epochs) * len(eval_envs) if eval_epochs and eval_envs else 0
summary_rows = csv_rows(summary_path)

def log_info(path):
    tail = tail_text(path, log_tail_lines)
    info = safe_stat(path)
    info.update({"tail": tail, "tail_sha256": sha(tail)})
    return info

logs = {
    "train": log_info(train_log_path),
    "eval_queue": log_info(queue_log_path),
    "supervisor": log_info(supervisor_log_path),
    "watchdog": log_info(watchdog_log_path),
}
train_tail = logs["train"].get("tail") or ""
queue_tail = logs["eval_queue"].get("tail") or ""

pid_paths = {
    "train": current.get("train_pid_path") or maybe_path(run_root, "train.pid"),
    "eval_queue": current.get("queue_pid_path") or maybe_path(queue_root, "followup_queue.pid"),
    "supervisor": current.get("supervisor_pid_path") or maybe_path(root, "supervisor.pid"),
    "watchdog": current.get("watchdog_pid_path") or maybe_path(root, "watchdog.pid"),
}
pids = {name: pid_info(path) for name, path in pid_paths.items()}
if latest_pid and not pids["train"].get("pid"):
    pids["train"] = {"path": "", "pid": latest_pid, "alive": pid_alive(latest_pid)}

gpu_lines = []
try:
    gpu = subprocess.run(
        ["nvidia-smi", "--query-gpu=index,name,memory.used,utilization.gpu", "--format=csv,noheader,nounits"],
        text=True,
        capture_output=True,
        timeout=5,
        check=False,
    )
    gpu_lines = [line.strip() for line in gpu.stdout.splitlines() if line.strip()]
except Exception:
    gpu_lines = []

process_lines = []
patterns = [item for item in [run_id, eval_label, root, queue_root, run_root] if item]
try:
    ps = subprocess.run(["ps", "-eo", "pid,ppid,stat,etime,cmd"], text=True, capture_output=True, timeout=5, check=False)
    for line in ps.stdout.splitlines():
        if any(pattern in line for pattern in patterns):
            process_lines.append(line[:1000])
except Exception:
    process_lines = []

snapshot = {
    "sync_status": "ok",
    "remote_now": time.time(),
    "host": {
        "hostname": hostname(),
        "boot_id": read_boot_id(),
        "uptime_seconds": read_uptime_seconds(),
    },
    "root": root,
    "base": base,
    "state": state,
    "current": current,
    "run_id": run_id,
    "eval_label": eval_label,
    "paths": {
        "state": state_path,
        "events": events_path,
        "run_root": run_root,
        "eval_root": eval_root,
        "queue_root": queue_root,
        "contract": contract_path,
        "command": command_path,
        "summary": summary_path,
    },
    "files": {
        "state": safe_stat(state_path),
        "events": safe_stat(events_path),
        "contract": safe_stat(contract_path),
        "command": safe_stat(command_path),
        "summary": safe_stat(summary_path),
    },
    "pids": pids,
    "logs": logs,
    "contract": {"path": contract_path, "env": contract_env, "sha256": sha(contract_text)},
    "command": {"path": command_path, "text": command_text, "sha256": sha(command_text)},
    "summary": {
        "path": summary_path,
        "rows": summary_rows,
        "expected_rows": expected_rows,
        "complete": bool(expected_rows and summary_rows >= expected_rows),
        "eval_epochs": eval_epochs,
        "eval_envs": eval_envs,
    },
    "remote_events_tail": tail_events(events_path, event_tail),
    "train_done_marker": "\nDone." in ("\n" + train_tail),
    "queue_done_marker": "\nDone." in ("\n" + queue_tail),
    "gpu": gpu_lines,
    "processes": process_lines[-40:],
}
print(json.dumps(snapshot, ensure_ascii=False))
'''
    return header + "\n" + python_code + "\nPY_REMOTE_SYNC\n"


def _record_remote_event_tail(store: Store, experiment, snapshot: dict[str, Any]) -> int:
    remote_events = snapshot.get("remote_events_tail")
    if not isinstance(remote_events, list):
        return 0
    recent = store.list_experiment_events(experiment["id"], limit=2000)
    known = set()
    for event in recent:
        if event["event_type"] != "remote_event":
            continue
        payload = _event_payload(event)
        key = payload.get("remote_event_key")
        if key:
            known.add(str(key))
    imported = 0
    source_path = str((snapshot.get("paths") or {}).get("events") or "")
    for item in remote_events:
        if not isinstance(item, dict):
            continue
        raw = str(item.get("raw") or "")
        line_number = item.get("line_number")
        digest = str(item.get("sha256") or hashlib.sha256(raw.encode("utf-8")).hexdigest())
        key = f"{source_path}:{line_number}:{digest}"
        if key in known:
            continue
        parsed = item.get("parsed") if isinstance(item.get("parsed"), dict) else {}
        message = str(parsed.get("message") or parsed.get("event") or parsed.get("type") or raw[:120])
        store.add_experiment_event(
            experiment["id"],
            event_type="remote_event",
            level=str(parsed.get("level") or "info") if str(parsed.get("level") or "info") in {"debug", "info", "warning", "error"} else "info",
            message=f"Remote event: {message[:180]}",
            payload={
                "remote_event_key": key,
                "source_path": source_path,
                "line_number": line_number,
                "sha256": digest,
                "parsed": parsed,
                "raw": raw[-2000:],
            },
            source="remote",
            status="running",
            phase=experiment["phase"],
            phase_message=experiment["phase_message"],
        )
        known.add(key)
        imported += 1
    return imported


def _compact_remote_snapshot(snapshot: dict[str, Any], *, include_hints: bool = False) -> dict[str, Any]:
    files = snapshot.get("files") if isinstance(snapshot.get("files"), dict) else {}
    pids = snapshot.get("pids") if isinstance(snapshot.get("pids"), dict) else {}
    logs = snapshot.get("logs") if isinstance(snapshot.get("logs"), dict) else {}
    host = snapshot.get("host") if isinstance(snapshot.get("host"), dict) else {}
    compact_logs = {}
    for name, info in logs.items():
        if not isinstance(info, dict):
            continue
        compact_logs[name] = {
            "path": info.get("path"),
            "exists": info.get("exists"),
            "mtime": info.get("mtime"),
            "tail_sha256": info.get("tail_sha256"),
        }
        if include_hints:
            hints = _failure_hint_lines(str(info.get("tail") or ""))
            if hints:
                compact_logs[name]["failure_hints"] = hints[:5]
    return {
        "sync_status": snapshot.get("sync_status"),
        "remote_ref": snapshot.get("remote_ref"),
        "observed_at_local": snapshot.get("observed_at_local"),
        "remote_now": snapshot.get("remote_now"),
        "clock_skew_seconds": snapshot.get("clock_skew_seconds"),
        "host": {
            "hostname": host.get("hostname"),
            "boot_id": host.get("boot_id"),
            "uptime_seconds": host.get("uptime_seconds"),
        },
        "root": snapshot.get("root"),
        "base": snapshot.get("base"),
        "run_id": snapshot.get("run_id"),
        "eval_label": snapshot.get("eval_label"),
        "paths": snapshot.get("paths") if isinstance(snapshot.get("paths"), dict) else {},
        "files": {
            key: {
                "path": value.get("path"),
                "exists": value.get("exists"),
                "size": value.get("size"),
                "mtime": value.get("mtime"),
            }
            for key, value in files.items()
            if isinstance(value, dict)
        },
        "pids": {
            key: {
                "path": value.get("path"),
                "pid": value.get("pid"),
                "alive": value.get("alive"),
            }
            for key, value in pids.items()
            if isinstance(value, dict)
        },
        "summary": snapshot.get("summary") if isinstance(snapshot.get("summary"), dict) else {},
        "contract": {
            "path": (snapshot.get("contract") or {}).get("path") if isinstance(snapshot.get("contract"), dict) else None,
            "sha256": (snapshot.get("contract") or {}).get("sha256") if isinstance(snapshot.get("contract"), dict) else None,
            "env": _selected_contract_env((snapshot.get("contract") or {}).get("env") if isinstance(snapshot.get("contract"), dict) else {}),
        },
        "command": {
            "path": (snapshot.get("command") or {}).get("path") if isinstance(snapshot.get("command"), dict) else None,
            "sha256": (snapshot.get("command") or {}).get("sha256") if isinstance(snapshot.get("command"), dict) else None,
        },
        "logs": compact_logs,
        "gpu": snapshot.get("gpu") or [],
        "processes": (snapshot.get("processes") or [])[:8],
        "train_done_marker": bool(snapshot.get("train_done_marker")),
        "queue_done_marker": bool(snapshot.get("queue_done_marker")),
    }


def _selected_contract_env(env: Any) -> dict[str, Any]:
    if not isinstance(env, dict):
        return {}
    keys = [
        "AUTO_RELABEL_VALUE_TRANSFORM",
        "auto_relabel_value_transform",
        "AUTO_RELABEL_VALUE_DROPOUT",
        "auto_relabel_value_dropout",
        "EVAL_EPOCHS",
        "EVAL_ENVS",
        "RUN_ID",
        "EVAL_LABEL",
    ]
    return {key: env[key] for key in keys if key in env}


def _remote_heartbeat_message(compact: dict[str, Any], anomalies: list[dict[str, Any]]) -> str:
    summary = compact.get("summary") if isinstance(compact.get("summary"), dict) else {}
    pids = compact.get("pids") if isinstance(compact.get("pids"), dict) else {}
    host = compact.get("host") if isinstance(compact.get("host"), dict) else {}
    train = pids.get("train") if isinstance(pids.get("train"), dict) else {}
    queue = pids.get("eval_queue") if isinstance(pids.get("eval_queue"), dict) else {}
    boot_id = str(host.get("boot_id") or "")
    boot_label = boot_id[:8] if boot_id else "-"
    rows = summary.get("rows")
    expected = summary.get("expected_rows")
    base = (
        f"Remote heartbeat: run={compact.get('run_id') or '-'} "
        f"eval={compact.get('eval_label') or '-'} "
        f"boot={boot_label} "
        f"rows={rows if rows is not None else '-'}/{expected if expected is not None else '-'} "
        f"train_alive={bool(train.get('alive'))} queue_alive={bool(queue.get('alive'))}."
    )
    control_codes = [item["code"] for item in anomalies if item.get("action") == "control"]
    if control_codes:
        return base + " Control anomalies: " + ", ".join(control_codes) + "."
    return base


def _remote_control_event_level(anomalies: list[dict[str, Any]]) -> str:
    severities = {str(item.get("severity") or "").lower() for item in anomalies}
    if "error" in severities:
        return "error"
    if "warning" in severities:
        return "warning"
    return "info"


def _compact_watchdog_sync_result(result: dict[str, Any] | None) -> dict[str, Any] | None:
    if not result:
        return None
    return {
        "sync_status": result.get("sync_status"),
        "reason": result.get("reason"),
        "remote_ref": result.get("remote_ref"),
        "remote_events_imported": result.get("remote_events_imported"),
        "anomaly_count": len(result.get("anomalies") or []),
        "control_anomaly_count": len(result.get("control_anomalies") or []),
        "batch_fingerprint": result.get("batch_fingerprint"),
        "anomaly_event_id": result.get("anomaly_event_id"),
        "snapshot": result.get("compact_snapshot"),
        "error": result.get("error"),
    }


def _latest_local_gate_control_request(events) -> dict[str, Any] | None:
    newer_followup: dict[str, Any] | None = None
    for event in events:
        event_type = str(event["event_type"])
        if event_type in LOCAL_GATE_FOLLOWUP_EVENT_TYPES:
            newer_followup = {
                "id": event["id"],
                "event_type": event_type,
                "created_at": event["created_at"],
            }
            continue
        if event_type not in LOCAL_GATE_CONTROL_EVENT_TYPES:
            continue
        if newer_followup is not None:
            return None
        payload = _event_payload(event)
        gate_state = _local_gate_state(payload)
        if not gate_state["needs_control"]:
            return None
        event_summary = {
            "id": event["id"],
            "event_type": event_type,
            "created_at": event["created_at"],
            "message": event["message"],
            "payload": _jsonable(payload),
        }
        fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "event_id": event["id"],
                    "event_type": event_type,
                    "gate_state": gate_state,
                },
                sort_keys=True,
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()
        return {
            "event": event_summary,
            "gate_state": gate_state,
            "batch_fingerprint": fingerprint,
            "anomaly": {
                "code": "result_gate_requires_followup",
                "severity": "warning",
                "action": "control",
                "message": "Local result gate requires artifact repair, analysis, or a next experiment decision.",
                "gate_event_id": event["id"],
                "gate_state": gate_state,
            },
        }
    return None


def _local_gate_state(payload: dict[str, Any]) -> dict[str, Any]:
    manual_review_required = _deep_first(payload, "manual_review_required")
    training_allowed = _deep_first(payload, "training_allowed_by_this_gate")
    allowed_next = str(_deep_first(payload, "allowed_next_contract_class") or "").strip().lower()
    promotion_allowed = _deep_first(payload, "promotion_allowed")
    checkpoint_allowed = _deep_first(payload, "checkpoint_adoption_allowed")
    blockers = _deep_string_list(payload, {"gate_blockers", "blockers", "reasons", "failures", "strict_gate_failures"})
    needs_control = (
        manual_review_required is True
        or training_allowed is False
        or allowed_next == "blocked"
        or promotion_allowed is False
        or checkpoint_allowed is False
        or any("manifest" in item.lower() or "repair" in item.lower() for item in blockers)
    )
    return {
        "needs_control": bool(needs_control),
        "manual_review_required": manual_review_required,
        "training_allowed_by_this_gate": training_allowed,
        "allowed_next_contract_class": allowed_next,
        "promotion_allowed": promotion_allowed,
        "checkpoint_adoption_allowed": checkpoint_allowed,
        "blockers": blockers[:40],
    }


def _deep_first(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        if key in value:
            return value[key]
        for child in value.values():
            found = _deep_first(child, key)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _deep_first(child, key)
            if found is not None:
                return found
    return None


def _deep_string_list(value: Any, keys: set[str]) -> list[str]:
    results: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key in keys:
                if isinstance(child, list):
                    results.extend(str(item) for item in child if str(item))
                elif isinstance(child, str) and child:
                    results.append(child)
            results.extend(_deep_string_list(child, keys))
    elif isinstance(value, list):
        for child in value:
            results.extend(_deep_string_list(child, keys))
    return results


def _recent_remote_fingerprint_event(
    events,
    event_type: str,
    fingerprint: str,
    *,
    cooldown_seconds: int,
    now: int,
) -> bool:
    for event in events:
        if event["event_type"] != event_type:
            continue
        payload = _event_payload(event)
        if payload.get("batch_fingerprint") != fingerprint and payload.get("fingerprint") != fingerprint:
            continue
        created_at = int(event["created_at"])
        if now - created_at <= cooldown_seconds:
            return True
    return False


def _recent_remote_control_started_without_failure(
    events,
    fingerprint: str,
    *,
    cooldown_seconds: int,
    now: int,
) -> bool:
    for event in events:
        if event["event_type"] != "remote_anomaly_control_started":
            continue
        payload = _event_payload(event)
        if payload.get("batch_fingerprint") != fingerprint and payload.get("fingerprint") != fingerprint:
            continue
        created_at = int(event["created_at"])
        if now - created_at > cooldown_seconds:
            continue
        if _remote_control_failed_after(events, fingerprint, created_at):
            continue
        return True
    return False


def _remote_control_failed_after(events, fingerprint: str, started_at: int) -> bool:
    for event in events:
        created_at = int(event["created_at"])
        if created_at < started_at:
            continue
        event_type = event["event_type"]
        payload = _event_payload(event)
        if event_type == "remote_anomaly_control_failed":
            if payload.get("batch_fingerprint") in {fingerprint, None, ""}:
                return True
        if event_type == "experiment_control_run_marked_stale":
            return True
    return False


def _remote_anomaly_batch_fingerprint(anomalies: list[dict[str, Any]]) -> str:
    if not anomalies:
        return ""
    return hashlib.sha256(
        json.dumps(
            [item.get("fingerprint") or item.get("code") for item in anomalies],
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()


def _remote_anomaly_fingerprint(snapshot: dict[str, Any], anomaly: dict[str, Any]) -> str:
    files = snapshot.get("files") if isinstance(snapshot.get("files"), dict) else {}
    state = files.get("state") if isinstance(files.get("state"), dict) else {}
    events = files.get("events") if isinstance(files.get("events"), dict) else {}
    logs = snapshot.get("logs") if isinstance(snapshot.get("logs"), dict) else {}
    host = snapshot.get("host") if isinstance(snapshot.get("host"), dict) else {}
    log_tail_hashes = {
        key: value.get("tail_sha256")
        for key, value in logs.items()
        if isinstance(value, dict) and value.get("tail_sha256")
    }
    identity = {
        "code": anomaly.get("code"),
        "root": snapshot.get("root"),
        "run_id": snapshot.get("run_id"),
        "eval_label": snapshot.get("eval_label"),
        "state_mtime": state.get("mtime"),
        "events_mtime": events.get("mtime"),
        "events_size": events.get("size"),
        "contract_sha256": (snapshot.get("contract") or {}).get("sha256") if isinstance(snapshot.get("contract"), dict) else None,
        "command_sha256": (snapshot.get("command") or {}).get("sha256") if isinstance(snapshot.get("command"), dict) else None,
        "host_boot_id": host.get("boot_id") if anomaly.get("code") in {"remote_host_rebooted", "remote_recent_reboot"} else None,
        "pids": snapshot.get("pids"),
        "summary": snapshot.get("summary"),
        "log_tail_hashes": log_tail_hashes if anomaly.get("code") == "remote_log_failure_hint" else {},
        "details": {
            key: anomaly.get(key)
            for key in ("rows", "expected_rows", "mismatches", "hints", "previous_boot_id", "boot_id", "uptime_seconds")
            if key in anomaly
        },
    }
    return hashlib.sha256(json.dumps(identity, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _age_seconds(mtime: Any, now_value: float) -> float | None:
    value = _number(mtime)
    if value is None:
        return None
    return max(0.0, now_value - value)


def _detect_contract_command_mismatch(snapshot: dict[str, Any]) -> list[str]:
    contract = snapshot.get("contract") if isinstance(snapshot.get("contract"), dict) else {}
    env = contract.get("env") if isinstance(contract.get("env"), dict) else {}
    command = snapshot.get("command") if isinstance(snapshot.get("command"), dict) else {}
    command_text = str(command.get("text") or "")
    if not command_text.strip():
        return []
    mismatches: list[str] = []
    expected_transform = (
        env.get("AUTO_RELABEL_VALUE_TRANSFORM")
        or env.get("auto_relabel_value_transform")
        or env.get("VALUE_TRANSFORM")
    )
    expected_dropout = (
        env.get("AUTO_RELABEL_VALUE_DROPOUT")
        or env.get("auto_relabel_value_dropout")
        or env.get("VALUE_DROPOUT")
    )
    actual_transform = _command_flag_value(command_text, "--auto_relabel_value_transform")
    actual_dropout = _command_flag_value(command_text, "--auto_relabel_value_dropout")
    if expected_transform and actual_transform and str(expected_transform) != str(actual_transform):
        mismatches.append(f"value_transform contract={expected_transform} command={actual_transform}")
    if expected_dropout and actual_dropout and not _float_string_equal(expected_dropout, actual_dropout):
        mismatches.append(f"value_dropout contract={expected_dropout} command={actual_dropout}")

    identity_text = " ".join(
        str(item or "")
        for item in (
            snapshot.get("run_id"),
            snapshot.get("eval_label"),
            command_text,
        )
    ).lower()
    token_expectations = {
        "block_zscore": ("blockzscore", "block-zscore", "block_zscore"),
        "block_rank": ("blockrank", "block-rank", "block_rank"),
        "task_minmax": ("taskminmax", "task-minmax", "task_minmax"),
    }
    for expected, tokens in token_expectations.items():
        if any(token in identity_text for token in tokens):
            actual = actual_transform or expected_transform
            if actual and str(actual) != expected:
                mismatches.append(f"identity implies {expected}, but transform is {actual}")
    dropout_expectations = [
        (r"vd005", 0.05),
        (r"dropout005", 0.05),
        (r"vd01", 0.1),
        (r"dropout01", 0.1),
        (r"(?<![a-z0-9])vd0(?![0-9])", 0.0),
        (r"nodrop", 0.0),
    ]
    for pattern, expected in dropout_expectations:
        if re.search(pattern, identity_text):
            actual = actual_dropout or expected_dropout
            if actual and not _float_string_equal(actual, expected):
                mismatches.append(f"identity implies dropout={expected}, but dropout is {actual}")
    return list(dict.fromkeys(mismatches))


def _command_flag_value(command: str, flag: str) -> str | None:
    try:
        parts = shlex.split(command)
    except ValueError:
        parts = command.split()
    for index, part in enumerate(parts):
        if part == flag and index + 1 < len(parts):
            return parts[index + 1]
        if part.startswith(flag + "="):
            return part.split("=", 1)[1]
    return None


def _float_string_equal(left: Any, right: Any) -> bool:
    left_number = _number(left)
    right_number = _number(right)
    if left_number is not None and right_number is not None:
        return abs(left_number - right_number) < 1e-9
    return str(left) == str(right)


def _remote_log_failure_hints(snapshot: dict[str, Any]) -> list[dict[str, str]]:
    logs = snapshot.get("logs") if isinstance(snapshot.get("logs"), dict) else {}
    hints: list[dict[str, str]] = []
    for name, info in logs.items():
        if not isinstance(info, dict):
            continue
        for line in _failure_hint_lines(str(info.get("tail") or "")):
            hints.append({"log": str(name), "line": line})
    return hints


def _failure_hint_lines(text: str) -> list[str]:
    lines: list[str] = []
    for line in text.splitlines():
        if REMOTE_FAILURE_HINT_RE.search(line):
            lines.append(line[-500:])
    return lines


def _active_train_looks_stalled(
    snapshot: dict[str, Any],
    *,
    stale_seconds: int,
    now_for_age: float,
) -> bool:
    logs = snapshot.get("logs") if isinstance(snapshot.get("logs"), dict) else {}
    train_log = logs.get("train") if isinstance(logs.get("train"), dict) else {}
    age = _age_seconds(train_log.get("mtime"), now_for_age)
    if age is None or age <= stale_seconds:
        return False
    gpu_lines = [str(line) for line in (snapshot.get("gpu") or [])]
    if not gpu_lines:
        return False
    utilizations: list[float] = []
    for line in gpu_lines:
        parts = [part.strip() for part in line.split(",")]
        if len(parts) >= 4:
            value = _number(parts[-1])
            if value is not None:
                utilizations.append(value)
    return bool(utilizations) and max(utilizations) < 5.0


def _resolve_wandb_run_ref(
    store: Store,
    experiment,
    *,
    run_path: str | None,
    wandb_entity: str | None,
    wandb_project: str | None,
    wandb_run_id: str | None,
) -> WandbRunRef:
    if run_path:
        return parse_wandb_run_path(run_path)
    if wandb_entity and wandb_project and wandb_run_id:
        return WandbRunRef(wandb_entity, wandb_project, wandb_run_id)

    config = _experiment_config(experiment)
    config_wandb = config.get("wandb") if isinstance(config.get("wandb"), dict) else {}
    latest_job = _latest_event_payload(store, experiment["id"], "remote_job_started")
    latest_path = latest_job.get("wandb_run_path") or config.get("wandb_run_path")
    if latest_path:
        return parse_wandb_run_path(str(latest_path))
    entity = wandb_entity or latest_job.get("wandb_entity") or config_wandb.get("entity") or config.get("wandb_entity")
    project = wandb_project or latest_job.get("wandb_project") or config_wandb.get("project") or config.get("wandb_project")
    run_id = wandb_run_id or latest_job.get("wandb_run_id") or config_wandb.get("run_id") or config.get("wandb_run_id")
    if entity and project and run_id:
        return WandbRunRef(str(entity), str(project), str(run_id))
    raise ValueError(
        "W&B run is unknown; pass --run-path or --wandb-entity/--wandb-project/--wandb-run-id"
    )


def _latest_event_payload(store: Store, experiment_id: int, event_type: str) -> dict[str, Any]:
    event = store.get_latest_experiment_event(experiment_id, event_type)
    if event is None:
        return {}
    try:
        payload = json.loads(event["payload_json"] or "{}")
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _latest_event_of_types(events, event_types: set[str]):
    for event in events:
        if event["event_type"] in event_types:
            return event
    return None


def _resolve_watchdog_run_path(
    explicit_run_path: str | None,
    latest_job: dict[str, Any],
    config: dict[str, Any],
) -> str | None:
    if explicit_run_path:
        return explicit_run_path
    latest_path = latest_job.get("wandb_run_path") or config.get("wandb_run_path")
    if latest_path:
        return str(latest_path)
    config_wandb = config.get("wandb") if isinstance(config.get("wandb"), dict) else {}
    entity = latest_job.get("wandb_entity") or config_wandb.get("entity") or config.get("wandb_entity")
    project = latest_job.get("wandb_project") or config_wandb.get("project") or config.get("wandb_project")
    run_id = latest_job.get("wandb_run_id") or config_wandb.get("run_id") or config.get("wandb_run_id")
    if entity and project and _is_real_wandb_run_id(run_id):
        return f"{entity}/{project}/{run_id}"
    return None


def _watchdog_targets_from_remote_snapshot(snapshot: Any) -> dict[str, str]:
    if not isinstance(snapshot, dict):
        return {}
    pids = snapshot.get("pids") if isinstance(snapshot.get("pids"), dict) else {}
    logs = snapshot.get("logs") if isinstance(snapshot.get("logs"), dict) else {}
    train_pid = pids.get("train") if isinstance(pids.get("train"), dict) else {}
    queue_pid = pids.get("eval_queue") if isinstance(pids.get("eval_queue"), dict) else {}
    train_log = logs.get("train") if isinstance(logs.get("train"), dict) else {}
    queue_log = logs.get("eval_queue") if isinstance(logs.get("eval_queue"), dict) else {}
    target: dict[str, str] = {}
    if train_pid.get("pid"):
        target["remote_pid"] = str(train_pid["pid"])
    elif queue_pid.get("pid"):
        target["remote_pid"] = str(queue_pid["pid"])
    if train_log.get("path"):
        target["remote_log_path"] = str(train_log["path"])
    elif queue_log.get("path"):
        target["remote_log_path"] = str(queue_log["path"])

    current = snapshot.get("current") if isinstance(snapshot.get("current"), dict) else {}
    contract = snapshot.get("contract") if isinstance(snapshot.get("contract"), dict) else {}
    env = contract.get("env") if isinstance(contract.get("env"), dict) else {}
    explicit = current.get("wandb_run_path") or env.get("WANDB_RUN_PATH")
    if explicit:
        target["run_path"] = str(explicit)
    else:
        entity = current.get("wandb_entity") or env.get("WANDB_ENTITY")
        project = current.get("wandb_project") or env.get("WANDB_PROJECT")
        run_id = current.get("wandb_run_id") or env.get("WANDB_RUN_ID")
        if entity and project and _is_real_wandb_run_id(run_id):
            target["run_path"] = f"{entity}/{project}/{run_id}"
    return target


def _remote_snapshot_has_active_work(snapshot: Any) -> bool:
    if not isinstance(snapshot, dict):
        return False
    pids = snapshot.get("pids") if isinstance(snapshot.get("pids"), dict) else {}
    for key in ("train", "eval_queue", "supervisor", "watchdog"):
        info = pids.get(key) if isinstance(pids.get(key), dict) else {}
        if info.get("alive"):
            return True
    return False


def _run_path_from_launch_or_contract(
    launch: dict[str, Any] | None,
    contract: dict[str, Any],
) -> str | None:
    should_track_wandb = _contract_produces_wandb_run(contract) or _contract_monitors_wandb_run(contract)
    if not should_track_wandb:
        return None
    if launch:
        explicit = launch.get("wandb_run_path")
        if explicit:
            return str(explicit)
        entity = launch.get("wandb_entity")
        project = launch.get("wandb_project")
        run_id = launch.get("wandb_run_id")
        if entity and project and _is_real_wandb_run_id(run_id):
            return f"{entity}/{project}/{run_id}"
    wandb = contract.get("wandb") if isinstance(contract.get("wandb"), dict) else {}
    entity = wandb.get("entity")
    project = wandb.get("project")
    run_id = wandb.get("run_id")
    if entity and project and _is_real_wandb_run_id(run_id):
        return f"{entity}/{project}/{run_id}"
    return None


def _terminal_event_key(
    terminal_reason: str,
    *,
    run_path: str | None,
    remote_pid: str | None,
    remote_log_path: str | None,
    wandb_result: dict[str, Any] | None,
    remote_result: dict[str, Any] | None,
) -> str:
    if wandb_result and run_path:
        state = wandb_result.get("state") or ""
        outcome = wandb_result.get("outcome") or terminal_reason
        return f"wandb:{run_path}:{state}:{outcome}"
    if remote_result:
        status = remote_result.get("status") or terminal_reason
        return f"remote:{remote_pid or ''}:{remote_log_path or ''}:{status}"
    return f"terminal:{terminal_reason}:{run_path or ''}:{remote_pid or ''}:{remote_log_path or ''}"


def _experiment_config(experiment) -> dict[str, Any]:
    try:
        parsed = json.loads(experiment["config_json"] or "{}")
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _resolve_remote_python(experiment, explicit_remote_python: str | None) -> str:
    config = _experiment_config(experiment)
    remote_state = config.get("remote_state") if isinstance(config.get("remote_state"), dict) else {}
    watchdog = config.get("watchdog") if isinstance(config.get("watchdog"), dict) else {}
    experiment_watchdog = (
        config.get("experiment_watchdog") if isinstance(config.get("experiment_watchdog"), dict) else {}
    )
    for candidate in (
        explicit_remote_python,
        config.get("remote_python"),
        remote_state.get("python"),
        watchdog.get("remote_python"),
        experiment_watchdog.get("remote_python"),
    ):
        value = str(candidate or "").strip()
        if value:
            return value
    return ""


def _prepare_remote_contract_command(command: str, remote_python: str) -> tuple[str, dict[str, Any]]:
    command = command.strip()
    remote_python = str(remote_python or "").strip()
    if not command or not remote_python:
        return command, {}
    python_dir = str(PurePosixPath(remote_python).parent)
    if "MYCODEX_REMOTE_PYTHON=" in command and python_dir in command:
        return command, {
            "remote_python": remote_python,
            "remote_python_dir": python_dir,
            "remote_python_prepared": True,
        }
    prefix = [
        f"export MYCODEX_REMOTE_PYTHON={shlex.quote(remote_python)}",
    ]
    if python_dir and python_dir != ".":
        prefix.append(f"export PATH={shlex.quote(python_dir)}:$PATH")
    prepared = "\n".join(prefix) + "\n" + command
    return prepared, {
        "remote_python": remote_python,
        "remote_python_dir": python_dir,
        "remote_python_prepared": True,
        "original_command": command,
    }


def _config_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item for item in re.split(r"[, ]+", value.strip()) if item]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item)]
    return [str(value)]


def _ensure_git_repo(path: Path) -> None:
    _git_text(path, ["rev-parse", "--git-dir"])


def _git_text(path: Path, args: list[str]) -> str:
    completed = subprocess.run(
        ["git", "-C", str(path), *args],
        text=True,
        capture_output=True,
        check=False,
        timeout=120,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"git {' '.join(args)} failed: {detail}")
    return completed.stdout if "-z" in args else completed.stdout.strip()


def _git_apply(path: Path, diff: str) -> None:
    for args in (["apply", "--check"], ["apply"]):
        completed = subprocess.run(
            ["git", "-C", str(path), *args],
            input=diff,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(f"git {' '.join(args)} failed: {detail}")


def _rollback_applied_patch(path: Path, diff: str) -> dict[str, Any]:
    for args in (["apply", "-R", "--check"], ["apply", "-R"]):
        completed = subprocess.run(
            ["git", "-C", str(path), *args],
            input=diff,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip()
            return {
                "rolled_back": False,
                "command": ["git", *args],
                "error": detail,
            }
    return {"rolled_back": True}


def _run_local_check_command(path: Path, command: str, timeout_seconds: float) -> subprocess.CompletedProcess:
    """Bound check commands and terminate their process group/tree on expiration."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("local check timeout must be finite and positive")
    options: dict[str, Any] = {"cwd": path, "shell": True, "text": True,
                               "stdout": subprocess.PIPE, "stderr": subprocess.PIPE}
    if os.name == "nt":
        options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    else:
        options["start_new_session"] = True
    try:
        process = subprocess.Popen(command, **options)
    except OSError as exc:
        return subprocess.CompletedProcess(command, 127, "", f"Could not start local check: {exc}")
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
        return subprocess.CompletedProcess(command, process.returncode, stdout or "", stderr or "")
    except subprocess.TimeoutExpired:
        termination_error = ""
        if os.name == "nt":
            try:
                terminated = subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    capture_output=True, text=True, timeout=5, check=False,
                    creationflags=subprocess.CREATE_NO_WINDOW)
                if terminated.returncode != 0 and process.poll() is None:
                    termination_error = "process-tree termination was not verified; inspect child processes"
            except (OSError, subprocess.TimeoutExpired) as exc:
                termination_error = f"process-tree termination failed: {exc}"
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            stdout, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired as exc:
            process.kill()
            stdout = exc.stdout or ""
            stderr = exc.stderr or ""
            for stream in (process.stdout, process.stderr):
                if stream:
                    stream.close()
            termination_error = termination_error or "child pipe stayed open after termination; inspect detached descendants"
        if isinstance(stdout, bytes):
            stdout = stdout.decode(errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode(errors="replace")
        return subprocess.CompletedProcess(command, 124, stdout or "",
            (stderr or "") + f"\nLocal check exceeded {timeout_seconds:g}s. {termination_error}".rstrip())


def _run_local_checks(
    path: Path,
    checks: list[Any],
    *,
    contract: dict[str, Any] | None = None,
    timeout_seconds: float = 300,
    execution_guard: Callable[[], bool] | None = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for raw in checks:
        if execution_guard is not None and not execution_guard():
            results.append({"command": str(raw), "returncode": 125, "stdout": "",
                            "stderr": "Protocol task lease lost before the next local check."})
            break
        command = str(raw).strip()
        if not command:
            continue
        if _is_remote_command_sha256_instruction(command):
            results.append(_check_remote_command_sha256(command, contract))
            continue
        if _is_procgen_wrapper_defaults_instruction(command):
            results.append(_check_procgen_wrapper_defaults(path, command))
            continue
        assertion = _parse_stdout_equals_check(command)
        if assertion is not None:
            results.append(_run_stdout_equals_check(path, command, assertion[0], assertion[1], timeout_seconds=timeout_seconds))
            continue
        empty_command = _parse_stdout_empty_check(command)
        if empty_command is not None:
            results.append(_run_stdout_empty_check(path, command, empty_command, timeout_seconds=timeout_seconds))
            continue
        completed = _run_local_check_command(path, command, timeout_seconds)
        results.append(
            {
                "command": command,
                "returncode": completed.returncode,
                "stdout": completed.stdout[-4000:],
                "stderr": completed.stderr[-4000:],
            }
        )
    return results


def _parse_stdout_equals_check(command: str) -> tuple[str, str] | None:
    match = re.match(r"^(.+?)\s+==\s+(.+?)\s*$", command)
    if not match:
        return None
    left = match.group(1).strip()
    expected = match.group(2).strip().strip("'\"")
    if not left or not expected:
        return None
    return left, expected


def _run_stdout_equals_check(path: Path, original: str, command: str, expected: str, *, timeout_seconds: float = 300) -> dict[str, Any]:
    completed = _run_local_check_command(path, command, timeout_seconds)
    observed = completed.stdout.strip().splitlines()[-1] if completed.stdout.strip() else ""
    ok = completed.returncode == 0 and observed == expected
    return {
        "command": original,
        "returncode": 0 if ok else 1,
        "stdout": f"expected={expected}\nobserved={observed}\nraw_stdout={completed.stdout[-4000:]}",
        "stderr": completed.stderr[-4000:] if completed.returncode != 0 else ("" if ok else "stdout equality check failed\n"),
        "synthetic": True,
        "kind": "stdout_equals",
    }


def _parse_stdout_empty_check(command: str) -> str | None:
    match = re.match(r"^(.+?)\s+is\s+empty\s*$", command, flags=re.IGNORECASE)
    if not match:
        return None
    inner = match.group(1).strip()
    return inner or None


def _run_stdout_empty_check(path: Path, original: str, command: str, *, timeout_seconds: float = 300) -> dict[str, Any]:
    completed = _run_local_check_command(path, command, timeout_seconds)
    ok = completed.returncode == 0 and not completed.stdout.strip()
    return {
        "command": original,
        "returncode": 0 if ok else 1,
        "stdout": completed.stdout[-4000:],
        "stderr": completed.stderr[-4000:] if completed.returncode != 0 else ("" if ok else "stdout was not empty\n"),
        "synthetic": True,
        "kind": "stdout_empty",
    }


def _is_procgen_wrapper_defaults_instruction(command: str) -> bool:
    normalized = " ".join(command.lower().split())
    return (
        normalized.startswith("verified wrapper defaults include")
        and "lr=0.001" in normalized
        and "auto_relabel_value_block_len=50" in normalized
    )


def _check_procgen_wrapper_defaults(path: Path, command: str) -> dict[str, Any]:
    wrapper = path / "experiments" / "run_procgen_multi_env_train.sh"
    if not wrapper.exists():
        return {
            "command": command,
            "returncode": 1,
            "stdout": "",
            "stderr": f"missing wrapper: {wrapper}\n",
            "synthetic": True,
            "kind": "procgen_wrapper_defaults",
        }
    text = wrapper.read_text(errors="replace")
    missing: list[str] = []
    if 'LR="${LR:-0.001}"' not in text:
        missing.append("LR=0.001")
    if 'AUTO_RELABEL_VALUE_BLOCK_LEN="${AUTO_RELABEL_VALUE_BLOCK_LEN:-50}"' not in text:
        missing.append("AUTO_RELABEL_VALUE_BLOCK_LEN=50")
    return {
        "command": command,
        "returncode": 0 if not missing else 1,
        "stdout": f"wrapper={wrapper}\n",
        "stderr": "" if not missing else "missing defaults: " + ", ".join(missing) + "\n",
        "synthetic": True,
        "kind": "procgen_wrapper_defaults",
    }


def _is_remote_command_sha256_instruction(command: str) -> bool:
    normalized = " ".join(command.lower().split())
    return (
        "remote.command" in normalized
        and "sha256" in normalized
        and (
            "outer mycodex" in normalized
            or "mycodex must" in normalized
            or "compute" in normalized
            or "recompute" in normalized
            or "require equality" in normalized
        )
    )


def _check_remote_command_sha256(
    command: str,
    contract: dict[str, Any] | None,
) -> dict[str, Any]:
    remote = contract.get("remote") if isinstance(contract, dict) and isinstance(contract.get("remote"), dict) else {}
    remote_command = str(remote.get("command") or "")
    expected = str(remote.get("command_sha256") or "").strip().lower()
    actual = hashlib.sha256(remote_command.encode("utf-8")).hexdigest()
    ok = bool(expected) and actual == expected
    return {
        "command": command,
        "returncode": 0 if ok else 1,
        "stdout": f"expected={expected}\nactual={actual}\n",
        "stderr": "" if ok else "remote.command_sha256 mismatch or missing\n",
        "synthetic": True,
        "kind": "remote_command_sha256",
    }


def _validate_env(env: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in env.items():
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", str(key)):
            raise ValueError(f"invalid environment variable name: {key!r}")
        result[str(key)] = str(value)
    return result


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    try:
        item_method = getattr(value, "item")
    except Exception:
        item_method = None
    if callable(item_method):
        try:
            return _jsonable(item_method())
        except Exception:
            pass
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _select_summary(summary: dict[str, Any], metrics: list[str]) -> dict[str, Any]:
    if metrics:
        keys = list(dict.fromkeys(["_step", "_runtime", *metrics]))
        return {key: summary[key] for key in keys if key in summary and _is_json_scalar(summary[key])}
    selected: dict[str, Any] = {}
    for key, value in summary.items():
        if key.startswith("_") and key not in {"_step", "_runtime"}:
            continue
        if _is_json_scalar(value):
            selected[key] = value
        if len(selected) >= 25:
            break
    return selected


def _is_json_scalar(value: Any) -> bool:
    return isinstance(value, (str, int, float, bool)) or value is None


def _nonfinite_metrics(metrics: dict[str, Any]) -> list[str]:
    nonfinite: list[str] = []
    for key, value in metrics.items():
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(numeric):
            nonfinite.append(key)
    return nonfinite


def _wandb_metric_message(run_ref: WandbRunRef, state: str, snapshot: dict[str, Any]) -> str:
    step = snapshot.get("latest_step")
    metrics = snapshot.get("latest_metrics") or {}
    metric_text = ", ".join(f"{key}={value}" for key, value in list(metrics.items())[:5])
    parts = [f"W&B {run_ref.path} state={state}"]
    if step is not None:
        parts.append(f"step={step}")
    if metric_text:
        parts.append(metric_text)
    return "; ".join(parts) + "."


def _execute_with_protocol_lease(
    store: Store, project_name: str, experiment_name: str, contract: dict[str, Any], *,
    protocol_task_id: int, owner: str | None, lease_seconds: int = 900, **kwargs: Any,
) -> dict[str, Any]:
    return _protocol._execute_with_protocol_lease(store, project_name, experiment_name, contract,
        protocol_task_id=protocol_task_id, owner=owner, lease_seconds=lease_seconds,
        executor=execute_execution_contract, **kwargs)


def run_ready_experiment_tasks(
    store: Store, project_name: str, experiment_name: str, *,
    owner: str = "mycodex-protocol-worker", lease_seconds: int = 900, max_tasks: int = 1,
    ssh_key: str | None = None, ssh_password_file: str | None = None,
    remote_ref: str | None = None, remote_python: str | None = None,
    allow_full_train: bool = False,
) -> dict[str, Any]:
    """Compatibility entrypoint; the task protocol receives an explicit executor."""
    return _protocol.run_ready_experiment_tasks(store, project_name, experiment_name,
        executor=execute_execution_contract, owner=owner, lease_seconds=lease_seconds,
        max_tasks=max_tasks, ssh_key=ssh_key, ssh_password_file=ssh_password_file,
        remote_ref=remote_ref, remote_python=remote_python, allow_full_train=allow_full_train)
