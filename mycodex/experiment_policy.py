"""Pure experiment contract validation and review policy.

This module never launches work, writes a Store, or imports experiment_ops.
The control plane owns authorization and side effects.
"""
from __future__ import annotations

import json
import math
import re
from typing import Any

from .contracts import (ContractValidationError, PEER_REVIEW_CONTRACT_V1,
                        parse_contract, validate_contract, validate_execution_contract)
from .experiment_artifacts import resolve_time_budget, validate_execution_id

_JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.IGNORECASE | re.DOTALL)


LOW_RISK_EXECUTION_CLASSES = {
    "read-only-preflight",
    "diagnostic-patch",
    "smoke-eval",
    "eval-only",
    "smoke-train",
    "monitor-only",
}


EVAL_EXECUTION_CLASSES = {"smoke-eval", "eval-only"}


REMOTE_COMMAND_EXECUTION_CLASSES = {
    "read-only-preflight",
    "monitor-only",
    "smoke-eval",
    "eval-only",
    "smoke-train",
    "full-train",
}


PEER_REVIEW_EXEMPT_EXECUTION_CLASSES = EVAL_EXECUTION_CLASSES


DEFAULT_PEER_REVIEW_BYPASS_ON = {"revise", "timeout"}


DEFAULT_PEER_REVIEW_BYPASS_ALLOWED_CLASSES = LOW_RISK_EXECUTION_CLASSES | {"full-train"}


WANDB_PRODUCING_EXECUTION_CLASSES = {
    "smoke-eval",
    "eval-only",
    "smoke-train",
    "full-train",
}


RESEARCH_REQUIRED_EXECUTION_CLASSES = {
    "smoke-train",
    "full-train",
}


RESEARCH_REQUIRED_SCALAR_FIELDS = (
    "hypothesis",
    "mechanism",
    "baseline",
    "intervention",
    "anti_sweep_rationale",
)


RESEARCH_REQUIRED_LIST_FIELDS = (
    "falsifiable_predictions",
    "controls",
    "analysis_plan",
)


RESEARCH_REQUIRED_DECISION_KEYS = (
    "supports",
    "falsifies",
    "ambiguous",
)


PLACEHOLDER_WANDB_RUN_IDS = {
    "",
    "discover_after_launch",
    "discover_after_launch_by_run_name",
    "pending",
    "unknown",
}


def extract_execution_contract(text: str) -> dict[str, Any] | None:
    blocks = [match.group(1) for match in _JSON_BLOCK_RE.finditer(text or "")]
    candidates = list(reversed(blocks))
    stripped = (text or "").strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        candidates.append(stripped)
    for candidate in candidates:
        try:
            parsed = parse_contract(candidate)
            _validate_experiment_contract_types(parsed)
        except (ValueError, TypeError):
            continue
        if isinstance(parsed, dict) and (
            "auto_execute" in parsed or "execution_class" in parsed or "remote" in parsed
        ):
            return parsed
    return None


def _execution_contract_parse_failure(text: str) -> str:
    blocks = [match.group(1) for match in _JSON_BLOCK_RE.finditer(text or "")]
    candidates = list(reversed(blocks))
    stripped = (text or "").strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        candidates.append(stripped)
    if not candidates:
        return "no fenced JSON object or whole-response JSON object found"

    reasons: list[str] = []
    for index, candidate in enumerate(candidates, start=1):
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError as exc:
            excerpt = candidate[max(0, exc.pos - 120) : exc.pos + 120]
            reasons.append(
                f"candidate {index} JSONDecodeError at line {exc.lineno} "
                f"column {exc.colno}: {exc.msg}; excerpt={excerpt!r}"
            )
            continue
        if not isinstance(parsed, dict):
            reasons.append(f"candidate {index} parsed as {type(parsed).__name__}, not object")
            continue
        try:
            _validate_experiment_contract_types(parse_contract(candidate))
        except (ValueError, TypeError) as exc:
            reasons.append(f"candidate {index} schema validation failed: {exc}")
            continue
        reasons.append(
            "candidate "
            f"{index} missing execution keys auto_execute/execution_class/remote"
        )
    return " | ".join(reasons[-3:])


def extract_peer_review_decision(text: str) -> dict[str, Any] | None:
    blocks = [match.group(1) for match in _JSON_BLOCK_RE.finditer(text or "")]
    candidates = list(reversed(blocks))
    stripped = (text or "").strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        candidates.append(stripped)
    for candidate in candidates:
        try:
            parsed = parse_contract(candidate)
            schema = PEER_REVIEW_CONTRACT_V1
            if "schema_version" not in parsed:
                schema = {**schema, "required": ["decision"]}
            validate_contract(parsed, schema)
        except (ValueError, TypeError):
            continue
        if isinstance(parsed, dict) and "decision" in parsed:
            return parsed
    return None


def _normalize_policy_tokens(value: Any, default: set[str]) -> set[str]:
    if value is None:
        return set(default)
    if isinstance(value, str):
        raw_items = re.split(r"[, ]+", value.strip())
    else:
        raw_items = []
        for item in value:
            raw_items.extend(re.split(r"[, ]+", str(item).strip()))
    items = {item.strip().lower() for item in raw_items if item.strip()}
    if "none" in items or "off" in items or "disabled" in items:
        return set()
    return items


def _peer_review_result_status(peer_review_result: dict[str, Any]) -> str:
    rounds = peer_review_result.get("rounds") if isinstance(peer_review_result.get("rounds"), list) else []
    statuses = {str(item.get("status") or "").strip().lower() for item in rounds if isinstance(item, dict)}
    if any(status in {"timeout", "failed", "cancelled"} for status in statuses):
        return "timeout"
    return "completed"


def _review_decision_text(peer_review_decision: dict[str, Any] | None) -> str:
    if not peer_review_decision:
        return ""
    return json.dumps(peer_review_decision, ensure_ascii=False, sort_keys=True)


def _peer_review_has_required_changes(peer_review_decision: dict[str, Any] | None) -> bool:
    if not isinstance(peer_review_decision, dict):
        return False
    changes = peer_review_decision.get("required_changes")
    if isinstance(changes, str):
        return bool(changes.strip())
    if isinstance(changes, list):
        return any(str(item).strip() for item in changes)
    return False


def _execution_class(contract: dict[str, Any]) -> str:
    return str(contract.get("execution_class") or "").strip()


def _contract_skips_peer_review(contract: dict[str, Any]) -> bool:
    return _execution_class(contract) in PEER_REVIEW_EXEMPT_EXECUTION_CLASSES


def _phase_for_contract_execution(contract: dict[str, Any]) -> str:
    execution_class = _execution_class(contract)
    if execution_class in EVAL_EXECUTION_CLASSES:
        return "launch_eval"
    if execution_class in {"read-only-preflight", "monitor-only"}:
        return "remote_preflight_pending"
    return "checking_git"


def _review_text(peer_review_decision: dict[str, Any] | None) -> str:
    if not isinstance(peer_review_decision, dict):
        return ""
    parts: list[str] = []
    for key in ("decision", "reason"):
        value = peer_review_decision.get(key)
        if value is not None:
            parts.append(str(value))
    for key in ("required_changes", "risk_notes", "blockers", "risks"):
        value = peer_review_decision.get(key)
        if isinstance(value, list):
            parts.extend(str(item) for item in value)
        elif value is not None:
            parts.append(str(value))
    return "\n".join(parts).lower()


def _peer_review_hard_blockers(
    peer_review_decision: dict[str, Any] | None,
    *,
    contract: dict[str, Any],
    allow_full_train: bool,
) -> list[str]:
    """Return reviewer objections that are allowed to stop execution.

    Peer review is not a general veto. It may only stop destructive actions,
    unauthorized long/paid training, or overwriting/committing unowned local
    changes. Method/artifact objections are recorded as soft objections.
    """
    text = _review_text(peer_review_decision)
    blockers: list[str] = []
    if any(
        token in text
        for token in (
            "destructive",
            "delete unrelated",
            "rm -rf",
            "format disk",
            "drop table",
            "破坏性",
            "删除无关",
            "清空",
        )
    ):
        blockers.append("destructive_operation")
    if any(
        token in text
        for token in (
            "billing",
            "quota",
            "paid",
            "spend",
            "cost",
            "unauthorized long",
            "unapproved full",
            "未授权长训",
            "账单",
            "配额",
            "花钱",
        )
    ):
        blockers.append("unauthorized_cost_or_long_training")
    if any(
        token in text
        for token in (
            "unowned",
            "unrelated local",
            "dirty worktree",
            "overwrite local",
            "commit unrelated",
            "覆盖未归属",
            "未归属",
            "无关本地改动",
        )
    ):
        blockers.append("unowned_local_changes")
    if _execution_class(contract) == "full-train" and not allow_full_train:
        blockers.append("full_train_without_allow_full_train")
    return sorted(set(blockers))


def _peer_review_soft_objections(peer_review_decision: dict[str, Any] | None) -> list[str]:
    if not isinstance(peer_review_decision, dict):
        return []
    objections: list[str] = []
    reason = str(peer_review_decision.get("reason") or "").strip()
    if reason:
        objections.append(reason)
    for key in ("required_changes", "risk_notes"):
        value = peer_review_decision.get(key)
        if isinstance(value, list):
            objections.extend(str(item).strip() for item in value if str(item).strip())
        elif isinstance(value, str) and value.strip():
            objections.append(value.strip())
    return objections[:40]


def _annotate_contract_with_reviewer_objections(
    contract: dict[str, Any],
    peer_review_decision: dict[str, Any] | None,
    *,
    reason: str,
) -> dict[str, Any]:
    annotated = dict(contract)
    review = annotated.get("review") if isinstance(annotated.get("review"), dict) else {}
    review = {
        **review,
        "method_claim_allowed": False,
        "execution_allowed_despite_reviewer_objections": True,
        "reason": reason,
        "reviewer_objections": _peer_review_soft_objections(peer_review_decision),
    }
    annotated["review"] = review
    annotated["method_claim_allowed"] = False
    return annotated


def _remote_eval_command_looks_destructive(command: str) -> bool:
    return bool(
        re.search(r"\brm\s+-[^;&|]*[rf]|git\s+(?:push|commit|reset|clean)\b|drop\s+table", command, re.IGNORECASE)
    )


def _contract_bypass_failures(
    contract: dict[str, Any],
    *,
    allow_full_train: bool,
    allowed_classes: set[str],
) -> list[str]:
    try:
        _validate_experiment_contract_types(contract)
    except (ValueError, TypeError) as exc:
        return [f"invalid_contract:{exc}"]
    failures: list[str] = []
    execution_class = _execution_class(contract)
    remote = contract.get("remote") if isinstance(contract.get("remote"), dict) else {}
    patch_info = contract.get("patch") if isinstance(contract.get("patch"), dict) else {}
    wandb = contract.get("wandb") if isinstance(contract.get("wandb"), dict) else {}

    if not bool(contract.get("auto_execute")):
        failures.append("auto_execute_false")
    if not execution_class:
        failures.append("missing_execution_class")
    elif execution_class not in allowed_classes:
        failures.append(f"execution_class_not_allowed:{execution_class}")
    if execution_class == "full-train" and not allow_full_train:
        failures.append("full_train_without_allow_full_train")

    command = str(remote.get("command") or "").strip()
    budget = str(remote.get("budget") or remote.get("timeout") or remote.get("timeout_seconds") or "").strip()
    if execution_class in REMOTE_COMMAND_EXECUTION_CLASSES:
        if not command:
            failures.append("missing_remote_command")
        if not budget:
            failures.append("missing_remote_budget_or_timeout")

    patch_required = bool(patch_info.get("required"))
    if execution_class in EVAL_EXECUTION_CLASSES:
        if patch_required:
            failures.append("eval_class_must_not_require_patch")
        if _remote_eval_command_looks_destructive(command):
            failures.append("eval_remote_command_not_read_only")
        if not contract.get("metrics"):
            failures.append("missing_eval_metrics")
        if not contract.get("hard_stop"):
            failures.append("missing_eval_hard_stop")
        if wandb and not (wandb.get("project") and (wandb.get("run_id") or wandb.get("run_name"))):
            failures.append("incomplete_eval_wandb_identity")

    if execution_class in {"smoke-train", "full-train"}:
        if not contract.get("metrics"):
            failures.append("missing_metrics")
        if not contract.get("hard_stop"):
            failures.append("missing_hard_stop")
        if not (wandb.get("project") and (wandb.get("run_id") or wandb.get("run_name"))):
            failures.append("missing_wandb_identity")
        failures.extend(_contract_research_failures(contract))

    if execution_class == "diagnostic-patch" and not patch_required and not command:
        failures.append("diagnostic_patch_without_patch_or_command")
    if patch_required:
        if not patch_info.get("files"):
            failures.append("missing_patch_files")
        if not str(patch_info.get("commit_message") or "").strip():
            failures.append("missing_patch_commit_message")
        if not str(patch_info.get("unified_diff") or patch_info.get("diff") or "").strip():
            failures.append("missing_patch_diff")

    return failures


def _validate_experiment_contract_types(contract: dict[str, Any]) -> None:
    validate_execution_contract(contract)
    remote = contract.get("remote") or {}
    if "local_check_timeout_seconds" in contract:
        limit = contract["local_check_timeout_seconds"]
        if isinstance(limit, bool) or not isinstance(limit, (int, float)) or not math.isfinite(limit) or limit <= 0:
            raise ContractValidationError("local check timeout must be finite positive numeric seconds")
    if "termination_grace_seconds" in remote:
        grace = remote["termination_grace_seconds"]
        if isinstance(grace, bool) or not isinstance(grace, (int, float)) or not math.isfinite(grace) or grace < 0:
            raise ContractValidationError("termination grace must be finite nonnegative numeric seconds")
    for identity in (contract.get("execution_id"), remote.get("execution_id")):
        if identity is not None:
            validate_execution_id(identity)
    if contract.get("schema_version") == "1" and contract.get("execution_class") in REMOTE_COMMAND_EXECUTION_CLASSES:
        if resolve_time_budget(remote) is None:
            raise ContractValidationError("v1 remote contracts require a numeric duration budget")


def _has_contract_content(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return any(_has_contract_content(item) for item in value)
    if isinstance(value, dict):
        return any(_has_contract_content(item) for item in value.values())
    return value is not None


def _contract_research_failures(contract: dict[str, Any]) -> list[str]:
    execution_class = str(contract.get("execution_class") or "").strip()
    if execution_class not in RESEARCH_REQUIRED_EXECUTION_CLASSES:
        return []

    research = contract.get("research")
    if not isinstance(research, dict):
        return ["missing_research_block"]

    failures: list[str] = []
    for field in RESEARCH_REQUIRED_SCALAR_FIELDS:
        if not _has_contract_content(research.get(field)):
            failures.append(f"missing_research_{field}")
    for field in RESEARCH_REQUIRED_LIST_FIELDS:
        value = research.get(field)
        if not isinstance(value, list) or not _has_contract_content(value):
            failures.append(f"missing_research_{field}")

    criteria = research.get("decision_criteria")
    if not isinstance(criteria, dict):
        failures.append("missing_research_decision_criteria")
    else:
        for key in RESEARCH_REQUIRED_DECISION_KEYS:
            if not _has_contract_content(criteria.get(key)):
                failures.append(f"missing_research_decision_criteria_{key}")
    return failures


def _peer_review_blocked_since(
    events: list[Any],
    *,
    trigger: str,
    key_field: str | None = None,
    key_value: str | None = None,
) -> int | None:
    matched_times: list[int] = []
    for event in events:
        payload = _event_payload(event)
        if trigger and payload.get("trigger") not in {None, trigger}:
            continue
        if key_field and key_value:
            payload_key = str(payload.get(key_field) or "")
            key_missing = not payload_key
            key_matches = payload_key == str(key_value)
            if not key_matches and not (
                key_missing
                and trigger == "supervisor"
                and event["event_type"] in {"supervisor_action_completed", "supervisor_stopped"}
            ):
                continue
        event_type = event["event_type"]
        decision_payload = payload.get("decision") if isinstance(payload.get("decision"), dict) else payload
        decision = str((decision_payload or {}).get("decision") or "").strip().lower()
        if event_type in {
            "remote_anomaly_control_completed",
            "supervisor_action_completed",
        } or (event_type == "peer_review_decision" and decision == "approve"):
            break
        if event_type not in {
            "peer_review_decision",
            "peer_review_failed",
            "remote_anomaly_control_failed",
            "supervisor_retry_scheduled",
        }:
            continue
        if decision in {"revise", "timeout"} or event_type in {
            "peer_review_failed",
            "remote_anomaly_control_failed",
            "supervisor_retry_scheduled",
        }:
            matched_times.append(int(event["created_at"]))
    return min(matched_times) if matched_times else None


def _assess_peer_review_bypass(
    *,
    contract: dict[str, Any],
    peer_review_decision: dict[str, Any] | None,
    peer_review_result: dict[str, Any],
    decision: str,
    blocked_since: int,
    now: int,
    allow_full_train: bool,
    timeout_seconds: int,
    bypass_on: Any,
    allowed_classes: Any,
) -> dict[str, Any]:
    normalized_decision = decision or (
        "timeout" if _peer_review_result_status(peer_review_result) == "timeout" else "unknown"
    )
    policy_on = _normalize_policy_tokens(bypass_on, DEFAULT_PEER_REVIEW_BYPASS_ON)
    policy_classes = _normalize_policy_tokens(
        allowed_classes,
        DEFAULT_PEER_REVIEW_BYPASS_ALLOWED_CLASSES,
    )
    age = max(0, now - int(blocked_since))
    failures: list[str] = []
    warnings: list[str] = []
    if normalized_decision == "block":
        failures.append("peer_review_block_is_not_bypassable")
    if normalized_decision == "revise" and _peer_review_has_required_changes(peer_review_decision):
        warnings.append("peer_review_required_changes_present")
    if normalized_decision not in policy_on:
        failures.append(f"decision_not_bypassable:{normalized_decision or '<missing>'}")
    if age < max(0, timeout_seconds):
        failures.append(f"peer_review_block_age_below_timeout:{age}<{timeout_seconds}")
    failures.extend(
        _contract_bypass_failures(
            contract,
            allow_full_train=allow_full_train,
            allowed_classes=policy_classes,
        )
    )
    return {
        "eligible": not failures,
        "decision": normalized_decision,
        "blocked_age_seconds": age,
        "blocked_since": blocked_since,
        "timeout_seconds": timeout_seconds,
        "bypass_on": sorted(policy_on),
        "allowed_classes": sorted(policy_classes),
        "failures": failures,
        "warnings": warnings,
        "review_excerpt": _review_decision_text(peer_review_decision)[:1200],
    }


def _contract_execution_failures(contract: dict[str, Any], *, allow_full_train: bool) -> list[str]:
    return _contract_bypass_failures(
        contract,
        allow_full_train=allow_full_train,
        allowed_classes=LOW_RISK_EXECUTION_CLASSES | {"full-train"},
    )


def _event_payload(event) -> dict[str, Any]:
    if event is None:
        return {}
    try:
        parsed = json.loads(event["payload_json"] or "{}")
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _contract_summary(contract: dict[str, Any]) -> str:
    execution_class = str(contract.get("execution_class") or "<missing>")
    auto_execute = bool(contract.get("auto_execute"))
    reason = str(contract.get("reason") or "").strip()
    parts = [f"Control contract parsed: class={execution_class}", f"auto_execute={auto_execute}"]
    if reason:
        parts.append(reason)
    return "; ".join(parts) + "."


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
