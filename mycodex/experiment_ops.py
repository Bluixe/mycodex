from __future__ import annotations

import json
import math
import os
import re
import shlex
import subprocess
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .db import Store
from .service import require_project, start_experiment_control_turn


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
_JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.IGNORECASE | re.DOTALL)
_COND_RE = re.compile(
    r"^\s*(?P<metric>[A-Za-z0-9_./:-]+)\s*"
    r"(?P<op>>=|<=|==|!=|>|<)\s*"
    r"(?P<value>-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*$"
)

LOW_RISK_EXECUTION_CLASSES = {
    "read-only-preflight",
    "diagnostic-patch",
    "smoke-train",
    "monitor-only",
}
WANDB_PRODUCING_EXECUTION_CLASSES = {
    "smoke-train",
    "full-train",
}
PLACEHOLDER_WANDB_RUN_IDS = {
    "",
    "discover_after_launch",
    "discover_after_launch_by_run_name",
    "pending",
    "unknown",
}


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
    remote_runner: RemoteRunner | None = None,
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

    launch_ts = int(time.time())
    resolved_log_path = log_path or f"logs/mycodex/{experiment_name}-{launch_ts}.log"
    resolved_pid_path = pid_path or f"{resolved_log_path}.pid"
    resolved_env = _validate_env(env or {})
    script = _remote_launch_script(
        target=target,
        command=command,
        commit=commit,
        branch=branch,
        fetch=fetch,
        log_path=resolved_log_path,
        pid_path=resolved_pid_path,
        env=resolved_env,
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
    }

    if dry_run:
        store.add_experiment_event(
            experiment["id"],
            event_type="remote_launch_dry_run",
            level="info",
            message=f"Would launch remote job on {target.ref}.",
            payload={**payload_base, "script": script},
            source="mycodex",
            status="running",
            phase="waiting_for_experiment_start",
        )
        return {**payload_base, "script": script, "dry_run": True}

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
        result = runner(target, script, str(resolved_ssh_key) if resolved_ssh_key else None, timeout)
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
    return {**job_payload, "dry_run": False}


def remote_job_status(
    store: Store,
    project_name: str,
    experiment_name: str,
    *,
    remote_ref: str | None = None,
    ssh_key: str | None = None,
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
    resolved_pid = str(pid or latest_job.get("remote_pid") or "")
    resolved_log = str(log_path or latest_job.get("log_path") or "")
    if not resolved_pid:
        raise ValueError("remote PID is required; pass --pid or launch a remote job first")
    script = _remote_status_script(resolved_pid, resolved_log, max(1, lines))
    runner = remote_runner or run_remote_script
    result = runner(target, script, str(resolved_ssh_key) if resolved_ssh_key else None, timeout)
    parsed = _parse_key_values(result.stdout)
    status = parsed.get("REMOTE_STATUS") or "unknown"
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
            "stdout": result.stdout,
            "stderr": result.stderr,
        },
        source="mycodex",
        phase="experiment_running" if status == "running" else "waiting_for_decision",
    )
    return {"status": status, "stdout": result.stdout, "stderr": result.stderr}


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
                phase="analyzing" if trigger_next_control else "waiting_for_decision",
                phase_message=(
                    "Terminal condition detected; triggering bound control session."
                    if trigger_next_control
                    else "Terminal condition detected; waiting for manual decision."
                ),
            )
            if not trigger_next_control:
                break

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
                store.add_experiment_event(
                    experiment["id"],
                    event_type="blocked",
                    level="warning",
                    message="Control session did not return a parseable JSON execution contract.",
                    payload={"run_id": control_result.get("run_id"), "status": control_result.get("status")},
                    source="mycodex",
                    phase="waiting_for_decision",
                )
                break
            store.add_experiment_event(
                experiment["id"],
                event_type="control_contract_parsed",
                level="info",
                message=_contract_summary(contract),
                payload={"contract": contract},
                source="mycodex",
                phase="checking_git" if auto_execute else "waiting_for_decision",
            )
            if not auto_execute:
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
            execution = execute_execution_contract(
                store,
                project_name,
                experiment_name,
                contract,
                ssh_key=ssh_key,
                remote_ref=remote_ref,
                allow_full_train=allow_full_train,
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


def extract_execution_contract(text: str) -> dict[str, Any] | None:
    blocks = [match.group(1) for match in _JSON_BLOCK_RE.finditer(text or "")]
    candidates = list(reversed(blocks))
    stripped = (text or "").strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        candidates.append(stripped)
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and (
            "auto_execute" in parsed or "execution_class" in parsed or "remote" in parsed
        ):
            return parsed
    return None


def execute_execution_contract(
    store: Store,
    project_name: str,
    experiment_name: str,
    contract: dict[str, Any],
    *,
    ssh_key: str | None = None,
    remote_ref: str | None = None,
    allow_full_train: bool = False,
) -> dict[str, Any]:
    project = require_project(store, project_name)
    experiment = _require_experiment(store, project["id"], experiment_name)
    repo = Path(project["path"])
    execution_class = str(contract.get("execution_class") or "").strip()
    auto_execute_flag = bool(contract.get("auto_execute"))

    if not auto_execute_flag:
        store.add_experiment_event(
            experiment["id"],
            event_type="blocked",
            level="warning",
            message="Execution contract has auto_execute=false.",
            payload={"contract": contract},
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
            payload={"contract": contract},
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
            payload={"contract": contract},
            source="mycodex",
            phase="waiting_for_decision",
        )
        return {"executed": False, "reason": "unsupported_execution_class"}

    patch_info = contract.get("patch") if isinstance(contract.get("patch"), dict) else {}
    patch_required = bool(patch_info.get("required"))
    files = [str(item) for item in patch_info.get("files") or []]
    commit_result: dict[str, Any] | None = None

    if patch_required:
        diff = str(patch_info.get("unified_diff") or patch_info.get("diff") or "").strip()
        if not diff:
            store.add_experiment_event(
                experiment["id"],
                event_type="blocked",
                level="warning",
                message="Patch is required, but the execution contract does not include a unified_diff.",
                payload={"contract": contract},
                source="mycodex",
                phase="waiting_for_decision",
            )
            return {"executed": False, "reason": "missing_unified_diff"}
        store.add_experiment_event(
            experiment["id"],
            event_type="code_change_started",
            level="info",
            message="Applying unified diff from execution contract.",
            payload={"files": files},
            source="mycodex",
            status="running",
            phase="modifying_code",
        )
        _git_apply(repo, diff)

    check_results = _run_local_checks(repo, contract.get("local_checks") or [])
    failed_checks = [item for item in check_results if item["returncode"] != 0]
    if failed_checks:
        store.add_experiment_event(
            experiment["id"],
            event_type="blocked",
            level="warning",
            message="Local check failed; refusing to launch remote job.",
            payload={"checks": check_results},
            source="mycodex",
            phase="waiting_for_decision",
        )
        return {"executed": False, "reason": "local_check_failed", "checks": check_results}

    if patch_required:
        commit_message = str(patch_info.get("commit_message") or "Apply experiment iteration patch")
        commit_result = git_commit_push(
            store,
            project_name,
            experiment_name,
            commit_message=commit_message,
            paths=files,
        )

    remote = contract.get("remote") if isinstance(contract.get("remote"), dict) else {}
    command = str(remote.get("command") or "").strip()
    launch_result: dict[str, Any] | None = None
    if command and execution_class != "monitor-only":
        wandb = contract.get("wandb") if isinstance(contract.get("wandb"), dict) else {}
        launch_tracks_wandb = _contract_produces_wandb_run(contract)
        commit = commit_result["commit"] if commit_result else _git_text(repo, ["rev-parse", "HEAD"])
        launch_result = launch_remote_job(
            store,
            project_name,
            experiment_name,
            command=command,
            remote_ref=remote_ref,
            ssh_key=ssh_key,
            commit=commit,
            require_clean=False,
            env=remote.get("env") if isinstance(remote.get("env"), dict) else {},
            wandb_entity=wandb.get("entity") if launch_tracks_wandb else None,
            wandb_project=wandb.get("project") if launch_tracks_wandb else None,
            wandb_run_id=wandb.get("run_id") if launch_tracks_wandb else None,
            wandb_run_name=wandb.get("run_name") if launch_tracks_wandb else None,
            timeout=180,
        )

    store.add_experiment_event(
        experiment["id"],
        event_type="supervisor_action_completed",
        level="info",
        message="Supervisor executed the control contract.",
        payload={"commit": commit_result, "checks": check_results, "launch": launch_result},
        source="mycodex",
        status="running",
        phase="experiment_running" if launch_result else "monitoring_wandb",
    )
    return {"executed": True, "commit": commit_result, "checks": check_results, "launch": launch_result}


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
) -> RemoteExecResult:
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
        detail = completed.stderr.strip() or completed.stdout.strip()
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
) -> str:
    exports = "\n".join(f"export {key}={shlex.quote(value)}" for key, value in env.items())
    return f"""set -euo pipefail
REMOTE_WORKDIR={shlex.quote(target.path)}
TARGET_COMMIT={shlex.quote(commit or "")}
TARGET_BRANCH={shlex.quote(branch or "")}
FETCH_REMOTE={shlex.quote("1" if fetch else "0")}
JOB_COMMAND={shlex.quote(command)}
LOG_PATH={shlex.quote(log_path)}
PID_PATH={shlex.quote(pid_path)}

cd "$REMOTE_WORKDIR"
if [ "$FETCH_REMOTE" = "1" ]; then
  git fetch --all --prune
fi
if [ -n "$TARGET_COMMIT" ]; then
  git checkout --detach "$TARGET_COMMIT"
elif [ -n "$TARGET_BRANCH" ]; then
  git checkout "$TARGET_BRANCH"
  git pull --ff-only
fi
REMOTE_COMMIT="$(git rev-parse HEAD 2>/dev/null || true)"
case "$LOG_PATH" in
  /*) ;;
  *) LOG_PATH="$PWD/$LOG_PATH" ;;
esac
case "$PID_PATH" in
  /*) ;;
  *) PID_PATH="$PWD/$PID_PATH" ;;
esac
mkdir -p "$(dirname "$LOG_PATH")" "$(dirname "$PID_PATH")"
{exports}
nohup bash -lc "$JOB_COMMAND" > "$LOG_PATH" 2>&1 < /dev/null &
REMOTE_PID="$!"
printf '%s\\n' "$REMOTE_PID" > "$PID_PATH"
printf 'REMOTE_COMMIT=%s\\n' "$REMOTE_COMMIT"
printf 'REMOTE_PID=%s\\n' "$REMOTE_PID"
printf 'REMOTE_LOG=%s\\n' "$LOG_PATH"
printf 'REMOTE_PID_FILE=%s\\n' "$PID_PATH"
"""


def _remote_status_script(pid: str, log_path: str, lines: int) -> str:
    return f"""set -euo pipefail
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
  tail -n "$LINES" "$LOG_PATH" || true
  printf 'REMOTE_LOG_TAIL_END\\n'
fi
"""


def _ssh_args(target: RemoteTarget, ssh_key: str | None) -> list[str]:
    args = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]
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
    for event in store.list_experiment_events(experiment_id, limit=100):
        if event["event_type"] != event_type:
            continue
        try:
            payload = json.loads(event["payload_json"] or "{}")
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}
    return {}


def _contract_summary(contract: dict[str, Any]) -> str:
    execution_class = str(contract.get("execution_class") or "<missing>")
    auto_execute = bool(contract.get("auto_execute"))
    reason = str(contract.get("reason") or "").strip()
    parts = [f"Control contract parsed: class={execution_class}", f"auto_execute={auto_execute}"]
    if reason:
        parts.append(reason)
    return "; ".join(parts) + "."


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


def _contract_produces_wandb_run(contract: dict[str, Any]) -> bool:
    """Whether executing this contract is expected to create a new W&B run."""
    remote = contract.get("remote") if isinstance(contract.get("remote"), dict) else {}
    wandb = contract.get("wandb") if isinstance(contract.get("wandb"), dict) else {}
    for explicit in (
        remote.get("produces_wandb"),
        remote.get("produces_wandb_run"),
        wandb.get("produces_run"),
    ):
        if explicit is not None:
            return bool(explicit)
    execution_class = str(contract.get("execution_class") or "").strip()
    return execution_class in WANDB_PRODUCING_EXECUTION_CLASSES


def _contract_monitors_wandb_run(contract: dict[str, Any]) -> bool:
    """Whether this contract intentionally points the supervisor at an existing W&B run."""
    execution_class = str(contract.get("execution_class") or "").strip()
    return execution_class == "monitor-only"


def _is_real_wandb_run_id(value: Any) -> bool:
    return str(value or "").strip() not in PLACEHOLDER_WANDB_RUN_IDS


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


def _contract_execution_fingerprint(contract: dict[str, Any]) -> str:
    remote = contract.get("remote") if isinstance(contract.get("remote"), dict) else {}
    patch = contract.get("patch") if isinstance(contract.get("patch"), dict) else {}
    wandb = contract.get("wandb") if isinstance(contract.get("wandb"), dict) else {}
    identity = {
        "execution_class": str(contract.get("execution_class") or ""),
        "remote_command": str(remote.get("command") or ""),
        "remote_env": remote.get("env") if isinstance(remote.get("env"), dict) else {},
        "patch_required": bool(patch.get("required")),
        "patch_files": patch.get("files") or [],
        "patch_diff": str(patch.get("unified_diff") or patch.get("diff") or ""),
        "wandb_entity": wandb.get("entity"),
        "wandb_project": wandb.get("project"),
        "wandb_run_id": wandb.get("run_id"),
    }
    return json.dumps(identity, sort_keys=True, ensure_ascii=False)


def _experiment_config(experiment) -> dict[str, Any]:
    try:
        parsed = json.loads(experiment["config_json"] or "{}")
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _require_experiment(store: Store, project_id: int, experiment_name: str):
    experiment = store.get_experiment(project_id, experiment_name)
    if experiment is None:
        raise ValueError(f"unknown experiment {experiment_name!r}; run `mycodex experiments create` first")
    return experiment


def _ensure_git_repo(path: Path) -> None:
    _git_text(path, ["rev-parse", "--git-dir"])


def _git_text(path: Path, args: list[str]) -> str:
    completed = subprocess.run(
        ["git", "-C", str(path), *args],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"git {' '.join(args)} failed: {detail}")
    return completed.stdout.strip()


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


def _run_local_checks(path: Path, checks: list[Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for raw in checks:
        command = str(raw).strip()
        if not command:
            continue
        completed = subprocess.run(
            command,
            cwd=path,
            shell=True,
            text=True,
            capture_output=True,
            check=False,
        )
        results.append(
            {
                "command": command,
                "returncode": completed.returncode,
                "stdout": completed.stdout[-4000:],
                "stderr": completed.stderr[-4000:],
            }
        )
    return results


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
