from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from mycodex import daemon as daemon_mod
from mycodex.db import Store
from mycodex.launch_agent import daemon_launch_agent_plist, default_daemon_launch_agent_label


class DaemonTests(unittest.TestCase):
    def test_daemon_launch_agent_plist_runs_daemon_at_login(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "db.sqlite"
            log_dir = Path(tmp) / "logs"
            label = default_daemon_launch_agent_label(db_path)

            plist = daemon_launch_agent_plist(
                label=label,
                db_path=db_path,
                cwd=Path(tmp),
                python="/usr/bin/python3",
                interval=300,
                timeout=1800,
                allow_full_train=True,
                notify_telegram=False,
                telegram_token_env="TOKEN_ENV",
                log_dir=log_dir,
            )

            args = plist["ProgramArguments"]
            self.assertEqual(plist["Label"], label)
            self.assertTrue(plist["RunAtLoad"])
            self.assertTrue(plist["KeepAlive"])
            self.assertEqual(args[:3], [str(Path("/usr/bin/python3").resolve()), "-m", "mycodex"])
            self.assertIn("--db", args)
            self.assertIn(str(db_path.resolve()), args)
            self.assertIn("daemon", args)
            self.assertIn("--interval", args)
            self.assertIn("300", args)
            self.assertIn("--timeout", args)
            self.assertIn("1800", args)
            self.assertIn("--allow-full-train", args)
            self.assertIn("--no-telegram-notify", args)
            self.assertEqual(plist["EnvironmentVariables"]["PYTHONUNBUFFERED"], "1")
            self.assertTrue(plist["StandardOutPath"].startswith(str(log_dir.resolve())))

    def test_daemon_pass_recovers_expired_experiment_task_lease(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            repo.mkdir()
            with Store(Path(tmp) / "db.sqlite") as store:
                project = store.add_project("demo", repo)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    status="running",
                )
                task = store.upsert_experiment_task(
                    experiment["id"],
                    status="ready",
                    execution_class="monitor-only",
                    idempotency_key="contract-1",
                    contract={"auto_execute": True, "execution_class": "monitor-only"},
                )
                store.lease_next_experiment_task(
                    experiment["id"],
                    owner="worker-before-restart",
                    lease_seconds=10,
                    now=1000,
                )

                daemon_mod.run_due_rules(store, notify_telegram=False)

                recovered = store.get_experiment_task(experiment["id"], task["id"])
                self.assertIsNotNone(recovered)
                self.assertEqual(recovered["status"], "ready")
                self.assertEqual(recovered["lease_owner"], "")
                event_types = [event["event_type"] for event in store.list_experiment_events(experiment["id"], limit=10)]
                self.assertIn("experiment_task_recovered", event_types)

    def test_daemon_loop_logs_uncaught_cycle_failure_without_crashing_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                buffer = io.StringIO()
                with (
                    patch.object(daemon_mod, "run_due_rules", side_effect=RuntimeError("cycle boom")),
                    redirect_stderr(buffer),
                ):
                    daemon_mod.run_daemon(store, once=True)

                log = buffer.getvalue()
                self.assertIn("daemon cycle failed error=cycle boom", log)
                self.assertIn("RuntimeError: cycle boom", log)

    def test_daily_summary_due_logs_and_notifies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            repo.mkdir()
            with Store(Path(tmp) / "db.sqlite") as store:
                project = store.add_project(
                    "demo",
                    repo,
                    daily_summary_enabled=True,
                    daily_summary_time="00:00",
                )
                buffer = io.StringIO()
                with (
                    patch.object(
                        daemon_mod,
                        "run_daily_summary",
                        return_value={
                            "run_id": 42,
                            "thread_id": "thread-1",
                            "status": "completed",
                            "output_text": "Daily summary body.",
                        },
                    ) as run_daily,
                    patch.object(
                        daemon_mod,
                        "notify_project_bindings",
                        return_value={"attempted": 1, "sent": 1, "skipped": 0, "errors": []},
                    ) as notify,
                    redirect_stderr(buffer),
                ):
                    daemon_mod.run_due_rules(
                        store,
                        timeout=7,
                        telegram_token_env="TOKEN_ENV",
                    )

                run_daily.assert_called_once_with(store, "demo", timeout=7)
                notify.assert_called_once()
                self.assertEqual(notify.call_args.args[1], project["id"])
                self.assertIn("Daily summary for demo completed.", notify.call_args.args[2])
                self.assertEqual(notify.call_args.kwargs["token_env"], "TOKEN_ENV")
                log = buffer.getvalue()
                self.assertIn("daily_summary due project=demo", log)
                self.assertIn("daily_summary completed project=demo", log)
                self.assertIn("telegram notify project=demo attempted=1 sent=1", log)

    def test_daily_summary_failure_is_logged_without_stopping_daemon_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            repo.mkdir()
            with Store(Path(tmp) / "db.sqlite") as store:
                store.add_project(
                    "demo",
                    repo,
                    daily_summary_enabled=True,
                    daily_summary_time="00:00",
                )
                buffer = io.StringIO()
                with (
                    patch.object(daemon_mod, "run_daily_summary", side_effect=RuntimeError("boom")),
                    patch.object(
                        daemon_mod,
                        "notify_project_bindings",
                        return_value={"attempted": 1, "sent": 1, "skipped": 0, "errors": []},
                    ) as notify,
                    redirect_stderr(buffer),
                ):
                    daemon_mod.run_due_rules(store)

                notify.assert_called_once()
                self.assertIn("Daily summary for demo failed.", notify.call_args.args[2])
                log = buffer.getvalue()
                self.assertIn("daily_summary failed project=demo", log)
                self.assertIn("daily_summary scheduler error project=demo", log)

    def test_interval_rule_uses_bound_thread_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            repo.mkdir()
            with Store(Path(tmp) / "db.sqlite") as store:
                project = store.add_project("demo", repo)
                store.add_rule(
                    project["id"],
                    name="hourly-status",
                    kind="status_check",
                    thread_id="thread-current",
                    interval_seconds=3600,
                    prompt="现在情况怎么样了",
                )

                with patch.object(
                    daemon_mod,
                    "send_prompt",
                    return_value={
                        "run_id": 7,
                        "thread_id": "thread-current",
                        "status": "completed",
                    },
                ) as send_prompt:
                    daemon_mod.run_due_rules(store, notify_telegram=False, timeout=11)

                send_prompt.assert_called_once_with(
                    store,
                    "demo",
                    "现在情况怎么样了",
                    kind="rule:status_check",
                    thread_id="thread-current",
                    timeout=11,
                )
                row = store.list_rules(project["id"])[0]
                self.assertEqual(row["thread_id"], "thread-current")
                self.assertIsNotNone(row["last_run_at"])

    def test_single_rule_check_only_runs_named_rule(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            repo.mkdir()
            with Store(Path(tmp) / "db.sqlite") as store:
                project = store.add_project("demo", repo)
                store.add_rule(
                    project["id"],
                    name="hourly-status",
                    kind="status_check",
                    thread_id="thread-current",
                    interval_seconds=3600,
                    prompt="现在情况怎么样了",
                )
                store.add_rule(
                    project["id"],
                    name="other-rule",
                    kind="custom",
                    thread_id="thread-other",
                    interval_seconds=3600,
                    prompt="do not run",
                )

                with patch.object(
                    daemon_mod,
                    "send_prompt",
                    return_value={
                        "run_id": 7,
                        "thread_id": "thread-current",
                        "status": "completed",
                    },
                ) as send_prompt:
                    ran = daemon_mod.run_single_rule_check(
                        store,
                        "demo",
                        "hourly-status",
                        notify_telegram=False,
                        timeout=11,
                    )

                self.assertTrue(ran)
                send_prompt.assert_called_once_with(
                    store,
                    "demo",
                    "现在情况怎么样了",
                    kind="rule:status_check",
                    thread_id="thread-current",
                    timeout=11,
                )
                rows = {row["name"]: row for row in store.list_rules(project["id"])}
                self.assertIsNotNone(rows["hourly-status"]["last_run_at"])
                self.assertIsNone(rows["other-rule"]["last_run_at"])

    def test_single_rule_check_skips_not_due_rule(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            repo.mkdir()
            with Store(Path(tmp) / "db.sqlite") as store:
                project = store.add_project("demo", repo)
                store.add_rule(
                    project["id"],
                    name="hourly-status",
                    kind="status_check",
                    thread_id="thread-current",
                    interval_seconds=3600,
                    prompt="现在情况怎么样了",
                )
                rule = store.list_rules(project["id"])[0]
                store.update_rule_run(rule["id"])

                with patch.object(daemon_mod, "send_prompt") as send_prompt:
                    ran = daemon_mod.run_single_rule_check(
                        store,
                        "demo",
                        "hourly-status",
                        notify_telegram=False,
                    )

                self.assertFalse(ran)
                send_prompt.assert_not_called()

    def test_configured_experiment_watchdog_runs_from_daemon(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            repo.mkdir()
            with Store(Path(tmp) / "db.sqlite") as store:
                project = store.add_project("demo", repo)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={
                        "watchdog": {
                            "enabled": True,
                            "interval_seconds": 300,
                            "remote_state_root": "/runs/controller",
                            "allow_full_train": True,
                            "monitor_only_after_restart_protection": True,
                            "peer_review_bypass_on": "revise,timeout",
                        }
                    },
                )
                buffer = io.StringIO()
                with (
                    patch.object(
                        daemon_mod,
                        "run_experiment_watchdog",
                        return_value={"cycle": 1, "experiments": [{"action": "skipped"}]},
                    ) as watchdog,
                    redirect_stderr(buffer),
                ):
                    daemon_mod.run_due_rules(store, notify_telegram=False)
                    daemon_mod.run_due_rules(store, notify_telegram=False)

                watchdog.assert_called_once()
                kwargs = watchdog.call_args.kwargs
                self.assertEqual(watchdog.call_args.args[:2], (store, "demo"))
                self.assertEqual(kwargs["experiment_name"], "self-iterate")
                self.assertEqual(kwargs["remote_state_root"], "/runs/controller")
                self.assertTrue(kwargs["allow_full_train"])
                self.assertTrue(kwargs["monitor_only"])
                self.assertEqual(kwargs["peer_review_bypass_on"], "revise,timeout")
                event_types = [
                    event["event_type"]
                    for event in store.list_experiment_events(experiment["id"], limit=10)
                ]
                self.assertIn("experiment_watchdog_daemon_started", event_types)
                self.assertIn("experiment_watchdog_daemon_completed", event_types)
                self.assertIn("experiment watchdog completed project=demo", buffer.getvalue())

    def test_configured_experiment_watchdog_runs_before_daily_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            repo.mkdir()
            with Store(Path(tmp) / "db.sqlite") as store:
                project = store.add_project(
                    "demo",
                    repo,
                    daily_summary_enabled=True,
                    daily_summary_time="00:00",
                )
                store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    status="running",
                    config={"watchdog": {"enabled": True, "interval_seconds": 300}},
                )
                order: list[str] = []

                def fake_watchdog(*_args, **_kwargs):
                    order.append("watchdog")
                    return {"cycle": 1, "experiments": [{"action": "skipped"}]}

                def fake_daily(*_args, **_kwargs):
                    order.append("daily")
                    return {
                        "run_id": 42,
                        "thread_id": "thread-1",
                        "status": "completed",
                        "output_text": "Daily summary body.",
                    }

                with (
                    patch.object(daemon_mod, "run_experiment_watchdog", side_effect=fake_watchdog),
                    patch.object(daemon_mod, "run_daily_summary", side_effect=fake_daily),
                ):
                    daemon_mod.run_due_rules(store, notify_telegram=False)

                self.assertEqual(order, ["watchdog", "daily"])

    def test_configured_experiment_watchdog_respects_recent_external_watchdog(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            repo.mkdir()
            with Store(Path(tmp) / "db.sqlite") as store:
                project = store.add_project("demo", repo)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    remote_ref="root@example.com:/remote/repo",
                    status="running",
                    config={"watchdog": {"enabled": True, "interval_seconds": 300}},
                )
                store.add_experiment_event(
                    experiment["id"],
                    event_type="watchdog_heartbeat",
                    message="external launchd watchdog checked recently",
                    source="mycodex",
                    status="running",
                )

                with patch.object(daemon_mod, "run_experiment_watchdog") as watchdog:
                    daemon_mod.run_due_rules(store, notify_telegram=False)

                watchdog.assert_not_called()
                event_types = [
                    event["event_type"]
                    for event in store.list_experiment_events(experiment["id"], limit=10)
                ]
                self.assertNotIn("experiment_watchdog_daemon_started", event_types)

    def test_configured_experiment_watchdog_skips_recent_active_control_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            repo.mkdir()
            with Store(Path(tmp) / "db.sqlite") as store:
                project = store.add_project("demo", repo)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    status="running",
                    config={"watchdog": {"enabled": True, "interval_seconds": 300}},
                )
                store.add_run(
                    project["id"],
                    kind=f"experiment:{experiment['id']}:control",
                    prompt="control",
                    thread_id="thread-1",
                )

                with patch.object(daemon_mod, "run_experiment_watchdog") as watchdog:
                    daemon_mod.run_due_rules(store, notify_telegram=False)

                watchdog.assert_not_called()
                event_types = [
                    event["event_type"]
                    for event in store.list_experiment_events(experiment["id"], limit=10)
                ]
                self.assertIn("experiment_watchdog_daemon_skipped_active_control", event_types)
                self.assertNotIn("experiment_watchdog_daemon_started", event_types)

    def test_configured_experiment_watchdog_marks_stale_control_run_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            repo.mkdir()
            with Store(Path(tmp) / "db.sqlite") as store:
                project = store.add_project("demo", repo)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    status="running",
                    config={
                        "watchdog": {
                            "enabled": True,
                            "interval_seconds": 300,
                            "control_timeout": 1,
                        }
                    },
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

                with patch.object(
                    daemon_mod,
                    "run_experiment_watchdog",
                    return_value={"cycle": 1, "experiments": [{"action": "skipped"}]},
                ) as watchdog:
                    daemon_mod.run_due_rules(store, notify_telegram=False)

                watchdog.assert_called_once()
                run = store.get_run(run_id)
                self.assertIsNotNone(run)
                self.assertEqual(run["status"], "failed")
                self.assertIn("stale running experiment Codex run", run["error"])
                event_types = [
                    event["event_type"]
                    for event in store.list_experiment_events(experiment["id"], limit=10)
                ]
                self.assertIn("experiment_control_run_marked_stale", event_types)
                self.assertIn("experiment_watchdog_daemon_started", event_types)


if __name__ == "__main__":
    unittest.main()
