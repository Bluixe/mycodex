from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from mycodex import daemon as daemon_mod
from mycodex.db import Store


class DaemonTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
