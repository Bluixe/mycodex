from __future__ import annotations

import json
import unittest

from mycodex.contracts import (
    ContractValidationError,
    EXPERIMENT_CONTRACT_V1,
    PEER_REVIEW_CONTRACT_V1,
    RESEARCH_CONTRACT_V1,
    contract_schema_for_kind,
    native_output_schema,
    normalize_native_contract,
    parse_contract,
    validate_contract,
    validate_execution_contract,
)


class ContractTests(unittest.TestCase):
    def test_direct_execution_validator_keeps_legacy_missing_field_policy_checks(self):
        validate_execution_contract({"auto_execute": True, "execution_class": "monitor-only",
                                     "remote": {"command": "echo ready", "time_budget_seconds": 10.5,
                                                "max_runtime_seconds": 20, "termination_grace_seconds": 0,
                                                "execution_id": "execution-1"}})
        validate_execution_contract({"auto_execute": False})
        with self.assertRaises(ContractValidationError):
            validate_execution_contract({"auto_execute": "false"})
        with self.assertRaises(ContractValidationError):
            validate_execution_contract({"schema_version": "1", "auto_execute": True})
        with self.assertRaises(ContractValidationError):
            validate_execution_contract({"remote": {"time_budget_seconds": "600"}})

    def test_remote_budget_shapes_finite_numbers_and_safe_ids(self):
        for budget in ("10 seconds", 10.5, {"seconds": 10}, {"max_seconds": 20}):
            with self.subTest(budget=budget):
                validate_execution_contract({"remote": {"budget": budget}})
        for invalid in ({"time_budget_seconds": 0}, {"time_budget_seconds": float("inf")},
                        {"termination_grace_seconds": float("nan")}, {"execution_id": "../escape"},
                        {"budget": {"seconds": True}}):
            with self.subTest(invalid=invalid), self.assertRaises(ContractValidationError):
                validate_execution_contract({"remote": invalid})

    def test_native_schemas_use_closed_objects_required_fields_and_nullable_options(self):
        def check(schema):
            if schema.get("type") == "object":
                self.assertIs(schema["additionalProperties"], False)
                self.assertEqual(set(schema["required"]), set(schema["properties"]))
                for child in schema["properties"].values():
                    check(child)
            for child in schema.get("anyOf", []):
                check(child)
            if "items" in schema:
                check(schema["items"])
        for schema in (EXPERIMENT_CONTRACT_V1, RESEARCH_CONTRACT_V1, PEER_REVIEW_CONTRACT_V1):
            with self.subTest(title=schema["title"]):
                native = native_output_schema(schema)
                check(native)
                self.assertEqual(native["properties"]["schema_version"], {"type": "string", "enum": ["1"]})

    def test_native_extensions_and_dynamic_env_restore_legacy_shape(self):
        native = {"auto_execute": False, "execution_class": "blocked", "reason": "hold",
                  "schema_version": None, "remote": {"command": "echo ok", "budget": None,
                  "env": '{"PYTHONUNBUFFERED":"1"}', "extensions_json": '{"custom_grace_hint":5}'},
                  "extensions_json": '{"custom":{"preserved":true}}'}
        value = normalize_native_contract(native, EXPERIMENT_CONTRACT_V1)
        validate_contract(value, EXPERIMENT_CONTRACT_V1)
        self.assertEqual(value["remote"]["env"], {"PYTHONUNBUFFERED": "1"})
        self.assertEqual(value["remote"]["custom_grace_hint"], 5)
        self.assertEqual(value["custom"], {"preserved": True})
        self.assertNotIn("schema_version", value)
        self.assertNotIn("budget", value["remote"])
        self.assertNotIn("extensions_json", value)

    def test_native_extensions_cannot_override_authorization_or_hide_bad_types(self):
        base = {"auto_execute": False, "execution_class": "blocked", "reason": "hold"}
        with self.assertRaises(ContractValidationError):
            normalize_native_contract({**base, "extensions_json": '{"auto_execute":true}'}, EXPERIMENT_CONTRACT_V1)
        value = normalize_native_contract({**base, "remote": {"env": '{"SEED":123}'}}, EXPERIMENT_CONTRACT_V1)
        with self.assertRaises(ContractValidationError):
            validate_contract(value, EXPERIMENT_CONTRACT_V1)

    def test_versioned_schema_is_selected_without_affecting_unstructured_turns(self):
        for kind, title in (("experiment:1:control", "mycodex.experiment.v1"),
                            ("experiment:1:contract_repair", "mycodex.experiment.v1"),
                            ("experiment:1:peer_review_decision", "mycodex.peer-review.v1"),
                            ("research:1:control", "mycodex.research.v1")):
            with self.subTest(kind=kind):
                schema = contract_schema_for_kind(kind)
                self.assertEqual(schema["title"], title)
                json.dumps(schema)
        self.assertIsNone(contract_schema_for_kind("manual"))
        self.assertIsNone(contract_schema_for_kind("experiment:1:peer_review"))
        modified = contract_schema_for_kind("experiment:1:control")
        modified["properties"].clear()
        self.assertIn("auto_execute", contract_schema_for_kind("experiment:1:control")["properties"])

    def test_legacy_flat_contract_allows_extensions_without_version(self):
        value = {"auto_execute": False, "execution_class": "blocked", "reason": "pending",
                 "custom_extension": {"original": "preserved"}}
        validate_contract(value, EXPERIMENT_CONTRACT_V1)
        self.assertEqual(value["custom_extension"]["original"], "preserved")

    def test_wrong_types_are_not_coerced_to_authorization(self):
        for invalid in ("false", "true", 0, 1, None):
            with self.subTest(invalid=invalid), self.assertRaises(ContractValidationError):
                validate_contract({"auto_execute": invalid, "execution_class": "blocked", "reason": "hold"},
                                  EXPERIMENT_CONTRACT_V1)

    def test_invalid_class_version_and_blocked_execution_are_rejected(self):
        for patch in ({"execution_class": "arbitrary-shell"}, {"schema_version": "2"},
                      {"auto_execute": True}):
            with self.subTest(patch=patch), self.assertRaises(ContractValidationError):
                validate_contract({"auto_execute": False, "execution_class": "blocked", "reason": "hold", **patch},
                                  EXPERIMENT_CONTRACT_V1)

    def test_nested_patch_remote_environment_and_ledger_types_are_checked(self):
        for patch in ({"patch": {"required": "false"}}, {"remote": {"timeout_seconds": True}},
                      {"remote": {"env": {"TOKEN": 123}}}, {"local_checks": "pytest"}):
            with self.subTest(patch=patch), self.assertRaises(ContractValidationError):
                validate_contract({"auto_execute": False, "execution_class": "blocked", "reason": "hold", **patch},
                                  EXPERIMENT_CONTRACT_V1)
        with self.assertRaises(ContractValidationError):
            validate_contract({"research_question": "question", "phase": "scan", "auto_continue": True,
                               "claim_ledger_delta": [{"claim": "claim", "confidence": "100"}]},
                              RESEARCH_CONTRACT_V1)

    def test_review_and_research_consistency(self):
        validate_contract({"decision": "approve", "reason": "safe", "required_changes": [], "risk_notes": []},
                          PEER_REVIEW_CONTRACT_V1)
        with self.assertRaises(ContractValidationError):
            validate_contract({"decision": "approve", "reason": "safe"}, PEER_REVIEW_CONTRACT_V1)
        with self.assertRaises(ContractValidationError):
            validate_contract({"research_question": "question", "phase": "blocked", "auto_continue": True,
                               "next_action": {"type": "ask_user"}}, RESEARCH_CONTRACT_V1)

    def test_native_and_fenced_json_parse_but_nonfinite_and_nonobjects_do_not(self):
        self.assertEqual(parse_contract('{"reason":"native"}'), {"reason": "native"})
        self.assertEqual(parse_contract('explanation\n```json\n{"reason":"legacy"}\n```'), {"reason": "legacy"})
        for text in ('[]', '{"confidence":NaN}', '{"score":Infinity}', '{not JSON}',
                     '{"auto_execute":false,"auto_execute":true}'):
            with self.subTest(text=text), self.assertRaises(ContractValidationError):
                parse_contract(text)


if __name__ == "__main__":
    unittest.main()
