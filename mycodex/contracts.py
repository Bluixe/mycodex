"""Versioned wire contracts; execution authorization remains in the control plane.

Schemas describe the existing flat contracts so stored v0 contracts remain
readable. Optional extensions are retained, but known fields are never coerced:
in particular, the string ``"false"`` cannot authorize automatic execution.
"""
from __future__ import annotations

import copy
import json
import math
import re
from typing import Any


class ContractValidationError(ValueError):
    pass


def _object(properties: dict[str, Any], required: tuple[str, ...] = ()) -> dict[str, Any]:
    return {"type": "object", "properties": properties, "required": list(required),
            "additionalProperties": True}


TEXT = {"type": "string"}
BOOL = {"type": "boolean"}
STRINGS = {"type": "array", "items": TEXT}
IDS = {"type": "array", "items": {"type": "integer", "minimum": 1}}
POSITIVE_SECONDS = {"type": "number", "exclusiveMinimum": 0}
EXECUTION_ID = {"type": "string", "pattern": r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"}
EXECUTION_CLASSES = ["read-only-preflight", "diagnostic-patch", "smoke-eval", "eval-only",
                     "smoke-train", "full-train", "monitor-only", "blocked"]

EXPERIMENT_CONTRACT_V1 = _object({
    "schema_version": {"type": "string", "enum": ["1"]},
    "auto_execute": BOOL,
    "execution_class": {"type": "string", "enum": EXECUTION_CLASSES},
    "reason": TEXT,
    "revision_response": _object({"required_changes_addressed": STRINGS, "remaining_gaps": STRINGS}),
    "peer_review": _object({"required": BOOL, "background": TEXT, "proposal": TEXT,
                            "review_questions": STRINGS, "expected_reviewer_output": TEXT}),
    "research": _object({
        "hypothesis": TEXT, "mechanism": TEXT, "baseline": TEXT, "intervention": TEXT,
        "falsifiable_predictions": STRINGS, "controls": STRINGS,
        "decision_criteria": _object({"supports": TEXT, "falsifies": TEXT, "ambiguous": TEXT}),
        "analysis_plan": STRINGS, "anti_sweep_rationale": TEXT,
    }),
    "patch": _object({"required": BOOL, "files": STRINGS, "summary": TEXT,
                       "commit_message": TEXT, "unified_diff": TEXT}),
    "local_checks": STRINGS,
    "local_check_timeout_seconds": POSITIVE_SECONDS,
    "remote": _object({
        "command": TEXT,
        "budget": {"anyOf": [TEXT, POSITIVE_SECONDS, _object({"seconds": POSITIVE_SECONDS,
                                                             "max_seconds": POSITIVE_SECONDS})]},
        "resume_from": TEXT, "commit": TEXT,
        "code_commit": TEXT, "allow_dirty": BOOL, "produces_wandb": BOOL,
        "produces_wandb_run": BOOL, "command_sha256": TEXT,
        "execution_id": EXECUTION_ID,
        "time_budget_seconds": POSITIVE_SECONDS,
        "max_runtime_seconds": POSITIVE_SECONDS,
        "termination_grace_seconds": {"type": "number", "minimum": 0},
        "timeout": POSITIVE_SECONDS,
        "timeout_seconds": POSITIVE_SECONDS,
        "env": {"type": "object", "additionalProperties": TEXT},
    }),
    "wandb": _object({key: {"type": ["string", "null"]}
                       for key in ("entity", "project", "run_id", "run_name")}),
    "artifacts": _object({"inputs": STRINGS, "outputs": STRINGS, "derived_fields": STRINGS}),
    "metrics": STRINGS, "hard_stop": STRINGS, "soft_review": STRINGS,
    "allow_dirty": BOOL, "commit": TEXT, "code_commit": TEXT,
    "execution_id": EXECUTION_ID,
    "env": {"type": "object", "additionalProperties": TEXT},
}, ("auto_execute", "execution_class", "reason"))
EXPERIMENT_CONTRACT_V1["title"] = "mycodex.experiment.v1"

PEER_REVIEW_CONTRACT_V1 = _object({
    "schema_version": {"type": "string", "enum": ["1"]},
    "decision": {"type": "string", "enum": ["approve", "revise", "block"]},
    "reason": TEXT, "required_changes": STRINGS, "risk_notes": STRINGS,
    "method_claim_allowed": BOOL,
}, ("decision", "reason", "required_changes", "risk_notes"))
PEER_REVIEW_CONTRACT_V1["title"] = "mycodex.peer-review.v1"

RESEARCH_CONTRACT_V1 = _object({
    "schema_version": {"type": "string", "enum": ["1"]},
    "research_question": TEXT, "phase": TEXT, "auto_continue": BOOL,
    "concept_preflight": _object({
        "profile": TEXT, "concept_axes": {"type": "array", "items": _object({
            "axis": TEXT, "values": STRINGS, "why": TEXT})},
        "query_families": {"type": "array", "items": _object({
            "family": TEXT, "purpose": TEXT, "queries": STRINGS})},
        "search_queries": STRINGS, "anchor_promotion_criteria": STRINGS, "ranking_gate": TEXT,
    }),
    "next_action": _object({"type": TEXT, "reason": TEXT, "queries": STRINGS,
                             "sources_to_read": STRINGS, "source_ids": IDS, "idea_ids": IDS,
                             "stop_condition": TEXT}, ("type",)),
    "source_ledger_delta": {"type": "array", "items": _object({
        "title": TEXT, "url": TEXT, "source_type": TEXT, "role": TEXT, "depth": TEXT,
        "keep": BOOL, "status": TEXT, "relevance": {"type": "integer"},
        "evidence_quality": {"type": "integer"}, "notes": TEXT,
    })},
    "claim_ledger_delta": {"type": "array", "items": _object({
        "claim": TEXT, "support": TEXT,
        "status": {"type": "string", "enum": ["supported", "contradicted", "uncertain", "candidate"]},
        "confidence": {"type": "integer"}, "notes": TEXT,
        "evidence_links": {"type": "array", "items": _object({
            "passage_id": {"type": "integer", "minimum": 1},
            "relation": {"type": "string", "enum": ["supports", "contradicts"]},
            "verification_status": {"type": "string", "enum": ["unverified", "verified", "rejected"]},
            "notes": TEXT,
        }, ("passage_id", "relation", "verification_status", "notes"))},
    }, ("claim",))},
    "idea_ledger_delta": {"type": "array", "items": _object({
        "idea": TEXT, "hypothesis": TEXT, "mechanism": TEXT, "minimal_test": TEXT,
        "score": _object({key: {"type": "number"} for key in (
            "evidence", "novelty", "feasibility", "project_fit", "information_gain")}),
        "kill_criteria": STRINGS, "status": TEXT, "notes": TEXT,
    })},
    "ask_user_if": STRINGS,
}, ("research_question", "phase", "auto_continue"))
RESEARCH_CONTRACT_V1["title"] = "mycodex.research.v1"


def contract_schema_for_kind(kind: str) -> dict[str, Any] | None:
    parts = kind.split(":")
    schema = None
    if len(parts) >= 3 and parts[0] == "experiment":
        if parts[-1] in {"control", "contract_repair"}:
            schema = EXPERIMENT_CONTRACT_V1
        elif parts[-1] == "peer_review_decision":
            schema = PEER_REVIEW_CONTRACT_V1
    elif len(parts) >= 3 and parts[0] == "research" and parts[-1] == "control":
        schema = RESEARCH_CONTRACT_V1
    return copy.deepcopy(schema) if schema is not None else None


def native_output_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Compile a validation schema to the provider's strict output subset.

    Open-ended mappings cannot be expressed by strict Structured Outputs.
    Encode those maps as JSON strings and extension fields in extensions_json;
    normalize_native_contract restores the historical flat Python dictionaries.
    """
    result = copy.deepcopy(schema)
    if "anyOf" in result:
        result["anyOf"] = [native_output_schema(child) for child in result["anyOf"]]
    if result.get("type") == "object":
        properties = result.get("properties")
        if properties is None:
            return {"type": "string", "description": "A strictly valid JSON object encoded as a string; use {} when empty."}
        required = set(result.get("required", []))
        # New model outputs must declare v1. Optional versioning exists only in
        # the validation schema so historical v0 ledger rows remain readable.
        if str(result.get("title", "")).startswith("mycodex.") and "schema_version" in properties:
            required.add("schema_version")
        converted = {}
        for key, child in properties.items():
            converted_child = native_output_schema(child)
            converted[key] = converted_child if key in required else {
                "anyOf": [converted_child, {"type": "null"}],
            }
        if result.get("additionalProperties", True) is not False:
            converted["extensions_json"] = {
                "type": "string", "description": "JSON object containing additional fields only; never repeat a named field. Use {} when unused.",
            }
        result["properties"] = converted
        result["required"] = list(converted)
        result["additionalProperties"] = False
    if result.get("type") == "array":
        result["items"] = native_output_schema(result["items"])
    return result


def normalize_native_contract(value: Any, schema: dict[str, Any], path: str = "$") -> Any:
    """Decode strict wire extensions, dropping absent optional nullable fields."""
    if "anyOf" in schema:
        for child_schema in schema["anyOf"]:
            try:
                normalized = normalize_native_contract(value, child_schema, path)
                validate_contract(normalized, child_schema, path)
                return normalized
            except ContractValidationError:
                pass
        return value
    if schema.get("type") == "object":
        if "properties" not in schema and isinstance(value, str):
            value = parse_contract(value)
        if not isinstance(value, dict):
            return value  # The validator reports the correct field and type.
        properties = schema.get("properties", {})
        required = set(schema.get("required", []))
        result = {}
        for key, child in value.items():
            if key == "extensions_json" and key not in properties:
                continue
            child_schema = properties.get(key)
            if child_schema is None:
                extra = schema.get("additionalProperties")
                result[key] = normalize_native_contract(child, extra, f"{path}.{key}") if isinstance(extra, dict) else child
                continue
            if child is None and key not in required:
                try:
                    validate_contract(None, child_schema, f"{path}.{key}")
                except ContractValidationError:
                    continue
            result[key] = normalize_native_contract(child, child_schema, f"{path}.{key}")
        if "extensions_json" in value and "extensions_json" not in properties:
            extensions = parse_contract(value["extensions_json"])
            collisions = set(extensions) & (set(properties) | {"extensions_json"})
            if collisions:
                raise ContractValidationError(f"{path}: extensions duplicate named fields: {sorted(collisions)}")
            result.update(extensions)
        return result
    if schema.get("type") == "array" and isinstance(value, list):
        return [normalize_native_contract(item, schema["items"], f"{path}[{index}]")
                for index, item in enumerate(value)]
    return value


def parse_contract(text: str) -> dict[str, Any]:
    """Accept legacy fenced JSON and the current native JSON response."""
    if not isinstance(text, str):
        raise ContractValidationError("expected a JSON object string")
    candidates = [text.strip()]
    candidates.extend(reversed(re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.I | re.S)))
    for candidate in candidates:
        try:
            value = json.loads(candidate, parse_constant=_invalid_constant, object_pairs_hook=_unique_object)
        except (ValueError, TypeError):
            continue
        if isinstance(value, dict):
            return value
    raise ContractValidationError("response did not contain a strict JSON object")


def _invalid_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key}")
        result[key] = value
    return result


def validate_contract(value: Any, schema: dict[str, Any], path: str = "$") -> None:
    """Validate the deliberately small JSON Schema subset used by our contracts.

    No dependency or permissive type coercion is needed for offline validation.
    Unknown extension fields survive for old/custom experiment contracts.
    """
    if "anyOf" in schema:
        for child_schema in schema["anyOf"]:
            try:
                validate_contract(value, child_schema, path)
                return
            except ContractValidationError:
                pass
        raise ContractValidationError(f"{path}: did not match any allowed schema")
    expected = schema.get("type")
    expected_types = expected if isinstance(expected, list) else [expected]
    types = {"object": lambda x: isinstance(x, dict), "array": lambda x: isinstance(x, list),
             "string": lambda x: isinstance(x, str), "boolean": lambda x: isinstance(x, bool),
             "integer": lambda x: isinstance(x, int) and not isinstance(x, bool),
             "number": lambda x: isinstance(x, (int, float)) and not isinstance(x, bool),
             "null": lambda x: x is None}
    if expected and not any(types[name](value) for name in expected_types):
        raise ContractValidationError(f"{path}: expected {expected}")
    if isinstance(value, float) and not math.isfinite(value):
        raise ContractValidationError(f"{path}: numbers must be finite")
    if "enum" in schema and value not in schema["enum"]:
        raise ContractValidationError(f"{path}: expected one of {schema['enum']}")
    if "minimum" in schema and value < schema["minimum"]:
        raise ContractValidationError(f"{path}: below minimum {schema['minimum']}")
    if "exclusiveMinimum" in schema and value <= schema["exclusiveMinimum"]:
        raise ContractValidationError(f"{path}: must exceed {schema['exclusiveMinimum']}")
    if "pattern" in schema and re.search(schema["pattern"], value) is None:
        raise ContractValidationError(f"{path}: invalid string format")
    if isinstance(value, dict):
        for key in schema.get("required", []):
            if key not in value:
                raise ContractValidationError(f"{path}.{key}: required field missing")
        properties = schema.get("properties", {})
        additional = schema.get("additionalProperties", True)
        for key, item in value.items():
            if key in properties:
                validate_contract(item, properties[key], f"{path}.{key}")
            elif additional is False:
                raise ContractValidationError(f"{path}.{key}: unknown field")
            elif isinstance(additional, dict):
                validate_contract(item, additional, f"{path}.{key}")
            elif additional is True:
                validate_contract(item, {}, f"{path}.{key}")
    elif isinstance(value, list) and "items" in schema:
        for index, item in enumerate(value):
            validate_contract(item, schema["items"], f"{path}[{index}]")
    if schema.get("title") == "mycodex.experiment.v1":
        if value.get("execution_class") == "blocked" and value.get("auto_execute") is True:
            raise ContractValidationError("$.auto_execute: blocked contracts cannot execute")
    if schema.get("title") == "mycodex.research.v1":
        action = value.get("next_action") or {}
        if action.get("type") == "ask_user" and value.get("auto_continue") is True:
            raise ContractValidationError("$.auto_continue: ask_user cannot automatically continue")


def validate_execution_contract(value: Any) -> None:
    """Check types at execution boundaries, including stored legacy v0 tasks.

    v0 can omit fields that existing policy validation treats as a blocked plan.
    Provided authorization flags still must be actual booleans. v1 contracts
    require the declared schema's core fields. This never grants authorization.
    """
    schema = copy.deepcopy(EXPERIMENT_CONTRACT_V1)
    if isinstance(value, dict) and "schema_version" not in value:
        schema["required"] = []
    validate_contract(value, schema)
