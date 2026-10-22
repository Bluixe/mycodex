"""Durable experiment task protocol, ownership, retries, and recovery.

Execution is injected by the orchestration layer. No import of experiment_ops
is needed, so queue/recovery behavior can be exercised without an executor.
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from typing import Any, Callable

from .db import EXPERIMENT_TASK_TERMINAL_STATUSES, Store
from .service import require_project
from .experiment_policy import (_contract_execution_failures, _event_payload,
                                _execution_class, _phase_for_contract_execution,
                                EVAL_EXECUTION_CLASSES)

def _contract_task_idempotency_key(
    contract: dict[str, Any],
    *,
    trigger: str,
    context_key: str = "",
) -> str:
    canonical = json.dumps(
        {
            "trigger": trigger,
            "context_key": context_key,
            "contract": contract,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"contract:{digest}"


def _contract_protocol_readiness(
    contract: dict[str, Any],
    *,
    auto_execute_policy: bool,
    allow_full_train: bool,
) -> dict[str, Any]:
    execution_class = _execution_class(contract)
    failures = _contract_execution_failures(contract, allow_full_train=allow_full_train)
    completeness_failures = [failure for failure in failures if failure != "auto_execute_false"]
    requested_auto_execute = contract.get("auto_execute") is True
    if completeness_failures:
        status = "blocked"
        reason = "contract_incomplete"
    elif requested_auto_execute and auto_execute_policy:
        status = "ready"
        reason = "contract_ready"
    else:
        status = "proposed"
        reason = "awaiting_auto_execute_authority"
    return {
        "status": status,
        "reason": reason,
        "execution_class": execution_class,
        "requested_auto_execute": requested_auto_execute,
        "policy_auto_execute": bool(auto_execute_policy),
        "failures": failures,
        "completeness_failures": completeness_failures,
    }


def record_execution_contract_task(
    store: Store,
    project_name: str,
    experiment_name: str,
    contract: dict[str, Any],
    *,
    trigger: str,
    auto_execute_policy: bool = True,
    allow_full_train: bool = False,
    source_event_id: int | None = None,
    run_id: int | None = None,
    context_key: str = "",
    source: str = "mycodex",
) -> dict[str, Any]:
    project = require_project(store, project_name)
    experiment = _require_experiment(store, project["id"], experiment_name)
    readiness = _contract_protocol_readiness(
        contract,
        auto_execute_policy=auto_execute_policy,
        allow_full_train=allow_full_train,
    )
    task = store.upsert_experiment_task(
        experiment["id"],
        task_type="execution_contract",
        status=readiness["status"],
        execution_class=readiness["execution_class"],
        idempotency_key=_contract_task_idempotency_key(
            contract,
            trigger=trigger,
            context_key=context_key,
        ),
        contract=contract,
        source_event_id=source_event_id,
        run_id=run_id,
        max_attempts=3,
        result={
            "trigger": trigger,
            "readiness": readiness,
            "context_key": context_key,
        },
    )
    retry_lineage: dict[str, Any] | None = None
    if readiness["status"] in {"ready", "proposed"} and readiness["execution_class"] == "monitor-only":
        root_task_id = int(task["id"])
        for attempt in range(2, 4):
            if task["status"] != "failed" or task["error"] != "remote_monitor_command_failed":
                break
            retry_lineage = {"root_task_id": root_task_id, "retry_of_task_id": int(task["id"]),
                             "attempt": attempt, "max_attempts": 3}
            retry_contract = {**contract, "_protocol_retry": retry_lineage}
            task = store.upsert_experiment_task(
                experiment["id"], task_type="execution_contract", status=readiness["status"],
                execution_class="monitor-only", idempotency_key=f"monitor-retry:{root_task_id}:{attempt}",
                contract=retry_contract, source_event_id=source_event_id, run_id=run_id, max_attempts=3,
                result={"trigger": trigger, "readiness": readiness, "context_key": context_key,
                        "retry_lineage": retry_lineage})
        if task["status"] == "failed" and task["error"] == "remote_monitor_command_failed":
            readiness = {**readiness, "status": "blocked", "reason": "monitor_retry_limit_reached",
                         "failures": [*readiness["failures"], "monitor_retry_limit_reached"]}
        if retry_lineage:
            store.add_experiment_event(experiment["id"], event_type="experiment_task_retry_recorded", level="info",
                message=f"Monitor diagnostic attempt {retry_lineage['attempt']}/3 uses task #{task['id']}.",
                payload={"task_id": task["id"], **retry_lineage, "readiness": readiness}, source=source)
    store.add_experiment_event(
        experiment["id"],
        event_type="experiment_task_recorded",
        level="warning" if readiness["status"] == "blocked" else "info",
        message=(
            f"Execution contract task #{task['id']} recorded: "
            f"status={task['status']} class={readiness['execution_class'] or '<missing>'}."
        ),
        payload={
            "task_id": task["id"],
            "task_status": task["status"],
            "trigger": trigger,
            "readiness": readiness,
            "context_key": context_key,
            "retry_lineage": retry_lineage,
        },
        source=source,
        status="running",
        phase=_phase_for_contract_execution(contract) if task["status"] == "ready" else "waiting_for_decision",
        phase_message=(
            "Execution contract is ready and waiting for a worker lease."
            if task["status"] == "ready"
            else "Execution contract was recorded but is not ready for automatic execution."
        ),
    )
    return {"task": dict(task), "readiness": readiness, "retry_lineage": retry_lineage}


def _protocol_task_outcome(execution: dict[str, Any]) -> tuple[str, str, str, str]:
    if execution.get("executed"):
        return "succeeded", "experiment_task_succeeded", "info", "completed"
    reason = str(execution.get("reason") or "")
    if reason.startswith("contract_") or reason in {
        "auto_execute_false",
        "full_train_not_allowed",
        "unsupported_execution_class",
    }:
        return "blocked", "experiment_task_blocked", "warning", f"blocked: {reason or 'unknown'}"
    return "failed", "experiment_task_failed", "warning", f"failed: {reason or 'unknown'}"


def _lease_recorded_protocol_task(
    store: Store,
    experiment,
    task_id: int,
    *,
    contract: dict[str, Any],
    owner: str,
    lease_seconds: int = 900,
) -> str | None:
    task = store.get_experiment_task(experiment["id"], task_id)
    if task is None or task["status"] in EXPERIMENT_TASK_TERMINAL_STATUSES:
        return None
    # This helper is reached after the review/authorization gate. Promote a
    # proposal only; never reset another worker's leased or running task.
    if task["status"] == "proposed":
        try:
            store.conn.execute("BEGIN IMMEDIATE")
            current = store.get_experiment_task(experiment["id"], task_id)
            if current is not None and current["status"] == "proposed":
                store.update_experiment_task_status(experiment["id"], task_id, "ready")
            else:
                store.conn.rollback()
        except Exception:
            store.conn.rollback()
            raise
    # The label is descriptive; the unique token fences each acquisition.
    lease_owner = f"{owner}:{uuid.uuid4().hex}"
    leased = store.lease_experiment_task(
        experiment["id"],
        task_id,
        owner=lease_owner,
        lease_seconds=lease_seconds,
    )
    if leased is not None:
        store.add_experiment_event(
            experiment["id"],
            event_type="experiment_task_lease_acquired",
            level="info",
            message=f"Execution contract task #{task_id} leased by {owner}.",
            payload={
                "task_id": task_id,
                "lease_owner": lease_owner,
                "lease_expires_at": leased["lease_expires_at"],
                "execution_class": leased["execution_class"],
            },
            source=owner,
            status="running",
            phase=_phase_for_contract_execution(contract),
            phase_message="A protocol worker leased the execution contract.",
        )
        if store.start_experiment_task_if_owned(experiment["id"], task_id, owner=lease_owner) is not None:
            return lease_owner
    return None


def _record_protocol_lease_loss(store: Store, experiment_id: int, task_id: int,
                               owner: str, execution: dict[str, Any] | None = None) -> None:
    store.add_experiment_event(
        experiment_id, event_type="experiment_task_lease_lost", level="warning",
        message=f"Execution task #{task_id} lost its lease; inspect side effects before resuming.",
        payload={"protocol_task_id": task_id, "lease_owner": owner, "execution": execution or {}}, source=owner)


def _execute_with_protocol_lease(
    store: Store, project_name: str, experiment_name: str, contract: dict[str, Any], *,
    protocol_task_id: int, owner: str | None, executor: Callable[..., dict[str, Any]],
    lease_seconds: int = 900, **kwargs: Any,
) -> dict[str, Any]:
    """Renew a task on a separate connection; fence every external continuation."""
    experiment = _require_experiment(store, require_project(store, project_name)["id"], experiment_name)
    experiment_id = experiment["id"]
    if not owner:
        _record_protocol_lease_loss(store, experiment_id, protocol_task_id, "unclaimed")
        return {"executed": False, "reason": "contract_lease_not_acquired"}
    stop = threading.Event()
    lost = threading.Event()
    interval = max(0.05, min(10.0, lease_seconds / 3))

    def owns_lease() -> bool:
        if lost.is_set():
            return False
        if not store.renew_experiment_task_lease(experiment_id, protocol_task_id, owner=owner,
                                                lease_seconds=lease_seconds):
            lost.set()
            return False
        return True

    if not owns_lease():
        _record_protocol_lease_loss(store, experiment_id, protocol_task_id, owner)
        return {"executed": False, "reason": "contract_lease_lost_requires_reconciliation"}

    def pulse() -> None:
        try:
            with Store(store.path) as heartbeat_store:
                heartbeat_store.conn.execute("PRAGMA busy_timeout = 1000")
                while not stop.is_set():
                    if not heartbeat_store.renew_experiment_task_lease(
                            experiment_id, protocol_task_id, owner=owner, lease_seconds=lease_seconds):
                        lost.set()
                        return
                    if stop.wait(interval):
                        return
        except Exception:
            lost.set()

    heartbeat = threading.Thread(target=pulse, daemon=True, name=f"experiment-lease-{protocol_task_id}")
    heartbeat.start()
    try:
        execution = executor(store, project_name, experiment_name, contract,
            protocol_task_id=protocol_task_id, execution_guard=owns_lease, **kwargs)
    finally:
        stop.set()
        heartbeat.join(timeout=2)
        if heartbeat.is_alive():
            lost.set()
    if not owns_lease():
        _record_protocol_lease_loss(store, experiment_id, protocol_task_id, owner, execution)
        return {"executed": False, "reason": "contract_lease_lost_requires_reconciliation",
                "observed_execution": execution}
    return execution


def _complete_recorded_protocol_task(
    store: Store,
    experiment,
    task_id: int,
    *,
    execution: dict[str, Any],
    contract: dict[str, Any],
    owner: str,
) -> None:
    if not owner:
        return
    status, event_type, level, outcome = _protocol_task_outcome(execution)
    completed = store.complete_experiment_task_if_owned(
        experiment["id"],
        task_id,
        status,
        owner=owner,
        result=execution,
        error=str(execution.get("reason") or ""),
    )
    if completed is None:
        _record_protocol_lease_loss(store, experiment["id"], task_id, owner, execution)
        return
    store.add_experiment_event(
        experiment["id"],
        event_type=event_type,
        level=level,
        message=f"Execution contract task #{task_id} {outcome}.",
        payload={"task_id": task_id, "execution": execution},
        source=owner,
        status="running",
        phase=(
            "launch_eval"
            if execution.get("launch") and _execution_class(contract) in EVAL_EXECUTION_CLASSES
            else "experiment_running"
            if execution.get("launch")
            else "waiting_for_decision"
        ),
    )


def recover_stale_experiment_tasks(
    store: Store,
    *,
    project_name: str | None = None,
    experiment_name: str | None = None,
    running_timeout_seconds: int = 3600,
    now: int | None = None,
    source: str = "mycodex-daemon",
) -> dict[str, Any]:
    if running_timeout_seconds < 1:
        raise ValueError("running_timeout_seconds must be positive")
    ts = int(time.time()) if now is None else int(now)
    projects = [require_project(store, project_name)] if project_name else list(store.list_projects())
    recovered: list[dict[str, Any]] = []
    for project in projects:
        experiments = (
            [_require_experiment(store, project["id"], experiment_name)]
            if experiment_name
            else list(store.list_experiments(project["id"]))
        )
        for experiment in experiments:
            for task in store.list_experiment_tasks(
                experiment["id"],
                statuses=["leased", "running"],
                limit=200,
            ):
                task_id = int(task["id"])
                task_status = str(task["status"])
                lease_expires_at = int(task["lease_expires_at"] or 0)
                age_seconds = max(0, ts - int(task["updated_at"]))
                reason = ""
                next_status = ""
                event_type = "experiment_task_recovered"
                level = "info"
                if task_status == "leased":
                    if lease_expires_at and lease_expires_at > ts:
                        continue
                    reason = "lease_expired_after_local_restart"
                    next_status = "ready"
                elif task_status == "running":
                    if lease_expires_at and lease_expires_at > ts:
                        continue
                    if age_seconds < running_timeout_seconds:
                        continue
                    events = store.list_experiment_events(experiment["id"], limit=1000)
                    if _task_has_remote_launch_event(events, task_id):
                        reason = "stale_running_task_has_remote_launch"
                        next_status = "blocked"
                        event_type = "experiment_task_recovery_blocked"
                        level = "warning"
                    else:
                        reason = "stale_running_task_without_remote_launch"
                        next_status = "ready"
                if not next_status:
                    continue
                try:
                    store.conn.execute("BEGIN IMMEDIATE")
                    current = store.get_experiment_task(experiment["id"], task_id)
                    if (current is None or any(current[key] != task[key] for key in
                            ("status", "lease_owner", "lease_expires_at", "updated_at"))
                            or int(current["lease_expires_at"] or 0) > ts):
                        store.conn.rollback()
                        continue
                    if task_status == "running" and _task_has_remote_launch_event(
                            store.list_experiment_events(experiment["id"], limit=1000), task_id):
                        reason = "stale_running_task_has_remote_launch"
                        next_status = "blocked"
                        event_type = "experiment_task_recovery_blocked"
                        level = "warning"
                    updated = store.update_experiment_task_status(
                        experiment["id"],
                        task_id,
                        next_status,
                        error=reason,
                        result={
                            "recovery_reason": reason,
                            "previous_status": task_status,
                            "age_seconds": age_seconds,
                            "lease_expires_at": lease_expires_at or None,
                        },
                    )
                except Exception:
                    store.conn.rollback()
                    raise
                store.add_experiment_event(
                    experiment["id"],
                    event_type=event_type,
                    level=level,
                    message=(
                        f"Recovered experiment task #{task_id}: {task_status} -> {next_status} "
                        f"({reason})."
                    ),
                    payload={
                        "task_id": task_id,
                        "previous_status": task_status,
                        "status": next_status,
                        "reason": reason,
                        "age_seconds": age_seconds,
                        "lease_expires_at": lease_expires_at or None,
                    },
                    source=source,
                    status="running" if experiment["status"] == "running" else None,
                    phase="waiting_for_decision" if next_status == "blocked" else experiment["phase"],
                    phase_message=(
                        "A stale protocol task had already launched remote work; inspect before retrying."
                        if next_status == "blocked"
                        else "A stale protocol task was returned to ready after local restart."
                    ),
                )
                recovered.append(
                    {
                        "project": project["name"],
                        "experiment": experiment["name"],
                        "task_id": task_id,
                        "previous_status": task_status,
                        "status": updated["status"],
                        "reason": reason,
                    }
                )
    return {"recovered_tasks": len(recovered), "tasks": recovered}


def _task_has_remote_launch_event(events: list[Any], task_id: int) -> bool:
    for event in events:
        if event["event_type"] not in {
            "remote_pull_started",
            "remote_job_started",
            "remote_pull_completed",
            "remote_monitor_command_started",
            "remote_monitor_command_completed",
        }:
            continue
        payload = _event_payload(event)
        if _payload_mentions_protocol_task(payload, task_id):
            return True
    return False


def _payload_mentions_protocol_task(value: Any, task_id: int) -> bool:
    if isinstance(value, dict):
        task_id_str = str(task_id)
        if str(value.get("protocol_task_id") or "") == task_id_str or str(value.get("task_id") or "") == task_id_str:
            return True
        return any(_payload_mentions_protocol_task(child, task_id) for child in value.values())
    if isinstance(value, list):
        return any(_payload_mentions_protocol_task(child, task_id) for child in value)
    return False


def run_ready_experiment_tasks(
    store: Store,
    project_name: str,
    experiment_name: str,
    *,
    executor: Callable[..., dict[str, Any]],
    owner: str = "mycodex-protocol-worker",
    lease_seconds: int = 900,
    max_tasks: int = 1,
    ssh_key: str | None = None,
    ssh_password_file: str | None = None,
    remote_ref: str | None = None,
    remote_python: str | None = None,
    allow_full_train: bool = False,
) -> dict[str, Any]:
    if max_tasks < 1:
        raise ValueError("max_tasks must be positive")
    project = require_project(store, project_name)
    experiment = _require_experiment(store, project["id"], experiment_name)
    results: list[dict[str, Any]] = []
    for _ in range(max_tasks):
        lease_owner = f"{owner}:{uuid.uuid4().hex}"
        task = store.lease_next_experiment_task(
            experiment["id"],
            owner=lease_owner,
            lease_seconds=lease_seconds,
        )
        if task is None:
            break
        task_id = int(task["id"])
        try:
            contract = json.loads(task["contract_json"] or "{}")
        except json.JSONDecodeError as exc:
            updated = store.complete_experiment_task_if_owned(
                experiment["id"],
                task_id,
                "failed",
                owner=lease_owner,
                error=f"contract_json decode failed: {exc}",
            )
            store.add_experiment_event(
                experiment["id"],
                event_type="experiment_task_failed",
                level="error",
                message=f"Execution contract task #{task_id} has invalid JSON.",
                payload={"task_id": task_id, "error": str(exc)},
                source=owner,
                phase="waiting_for_decision",
            )
            if updated is None:
                _record_protocol_lease_loss(store, experiment["id"], task_id, lease_owner)
                updated = store.get_experiment_task(experiment["id"], task_id)
            results.append({"task": dict(updated), "executed": False, "reason": "invalid_contract_json"})
            continue
        store.add_experiment_event(
            experiment["id"],
            event_type="experiment_task_lease_acquired",
            level="info",
            message=f"Execution contract task #{task_id} leased by {owner}.",
            payload={
                "task_id": task_id,
                "lease_owner": lease_owner,
                "lease_expires_at": task["lease_expires_at"],
                "execution_class": task["execution_class"],
            },
            source=owner,
            status="running",
            phase=_phase_for_contract_execution(contract),
            phase_message="A protocol worker leased the execution contract.",
        )
        if store.start_experiment_task_if_owned(experiment["id"], task_id, owner=lease_owner) is None:
            _record_protocol_lease_loss(store, experiment["id"], task_id, lease_owner)
            updated = store.get_experiment_task(experiment["id"], task_id)
            results.append({"task": dict(updated), "execution": {"executed": False,
                            "reason": "contract_lease_lost_requires_reconciliation"}})
            continue
        try:
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
                executor=executor,
                lease_seconds=lease_seconds,
            )
        except Exception as exc:
            execution = {"executed": False, "reason": "contract_execution_requires_reconciliation", "error": str(exc)}
            store.add_experiment_event(
                experiment["id"], event_type="experiment_task_execution_error", level="warning",
                message=f"Execution task #{task_id} stopped; reconcile any local or remote side effects: {exc}",
                payload={"protocol_task_id": task_id, "error": str(exc)}, source=owner)
        _complete_recorded_protocol_task(
            store,
            experiment,
            task_id,
            execution=execution,
            contract=contract,
            owner=lease_owner,
        )
        updated = store.get_experiment_task(experiment["id"], task_id)
        if updated is None:
            raise RuntimeError(f"experiment task #{task_id} disappeared during execution")
        results.append({"task": dict(updated), "execution": execution})
    return {"executed_tasks": len(results), "tasks": results}


def _require_experiment(store: Store, project_id: int, experiment_name: str):
    experiment = store.get_experiment(project_id, experiment_name)
    if experiment is None:
        raise ValueError(f"unknown experiment {experiment_name!r}; run `mycodex experiments create` first")
    return experiment
