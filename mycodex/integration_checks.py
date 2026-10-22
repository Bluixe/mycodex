"""Opt-in, zero-inference integration checks with machine-readable results.

Linux supervisor tests execute only on native Linux. An unavailable environment
is recorded as skipped/partial, never as successful remote recovery validation.
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


class RecordedResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cases = []

    def addSuccess(self, test):
        super().addSuccess(test)
        self.cases.append({"case": test.id(), "status": "passed"})

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self.cases.append({"case": test.id(), "status": "skipped", "reason": reason})

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.cases.append({"case": test.id(), "status": "failed", "reason": self._exc_info_to_string(err, test)})

    def addError(self, test, err):
        super().addError(test, err)
        self.cases.append({"case": test.id(), "status": "error", "reason": self._exc_info_to_string(err, test)})


def sdk_zero_inference_probe(output_dir: Path) -> dict:
    """Validate real SDK/runtime thread lifecycle, archiving the empty thread."""
    from .runtime_adapters import SdkAppServerClient, TESTED_SDK_VERSION
    workspace = output_dir / "empty-sdk-workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    report = {"sdk_version": TESTED_SDK_VERSION, "inference_turns": 0,
              "initialized": False, "thread_list": False, "thread_started": False,
              "thread_archived": False}
    client = SdkAppServerClient()
    thread_id = None
    try:
        client.start()
        report["initialized"] = True
        rows = client.list_threads(limit=1, use_state_db_only=True)
        report.update(thread_list=True, returned_count=len(rows))
        thread = client.start_or_resume_thread(thread_id=None, cwd=str(workspace.resolve()),
                                              sandbox="read-only", approval_policy="never")
        thread_id = thread["id"]
        report["thread_started"] = True
    except Exception as exc:
        report.update(status="failed", error_type=type(exc).__name__, error=str(exc))
    finally:
        if thread_id:
            try:
                client.request("thread/archive", {"threadId": thread_id}, timeout=15)
                report["thread_archived"] = True
            except Exception as exc:
                report.update(status="failed", archive_error_type=type(exc).__name__)
        client.close()
    report.setdefault("status", "passed" if report["thread_archived"] else "failed")
    (output_dir / "sdk-runtime-probe.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def run_integration_checks(output_dir: Path, *, include_sdk: bool = False) -> dict:
    names = ["tests.test_experiment_artifacts.RemoteSupervisorIntegrationTests",
             "tests.test_experiment_ops.ExperimentOpsTests.test_sync_remote_state_discovers_latest_root_when_configured_root_is_empty"]
    if include_sdk:
        names.append("tests.test_sdk_transport_integration.SdkTransportIntegrationTests")
    output_dir.mkdir(parents=True, exist_ok=True)
    log = io.StringIO()
    suite = unittest.defaultTestLoader.loadTestsFromNames(names)
    result = unittest.TextTestRunner(stream=log, verbosity=2, resultclass=RecordedResult).run(suite)
    report = {"timestamp": datetime.now(timezone(timedelta(hours=8))).isoformat(),
              "platform": sys.platform, "inference_turns": 0, "tests": result.testsRun,
              "failures": len(result.failures), "errors": len(result.errors), "skipped": len(result.skipped),
              "status": "failed" if not result.wasSuccessful() else "partial" if result.skipped else "passed",
              "cases": result.cases}
    (output_dir / "integration-cases.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (output_dir / "integration-cases.log").write_text(log.getvalue(), encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=Path("data/integration-runtime"))
    parser.add_argument("--sdk-package-path", type=Path, help="Explicit isolated SDK dependency directory")
    parser.add_argument("--sdk", action="store_true", help="Exercise real optional SDK against scripted peers")
    parser.add_argument("--sdk-probe", action="store_true", help="Create/archive an empty real-runtime thread; no turn")
    args = parser.parse_args(argv)
    if args.sdk_package_path:
        sys.path.insert(0, str(args.sdk_package_path.resolve()))
    output_dir = args.report_dir.resolve()
    data_root = (Path.cwd() / "data").resolve()
    if not output_dir.is_relative_to(data_root):
        parser.error("integration reports must stay inside this checkout's ignored data directory")
    report = run_integration_checks(output_dir, include_sdk=args.sdk)
    if args.sdk_probe:
        report["sdk_runtime_probe"] = sdk_zero_inference_probe(output_dir)
    print(json.dumps(report, indent=2))
    return int(report["status"] == "failed" or report.get("sdk_runtime_probe", {}).get("status") == "failed")


if __name__ == "__main__":
    raise SystemExit(main())
