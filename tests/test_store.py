from __future__ import annotations

import json
import tempfile
import unittest
import sqlite3
from pathlib import Path
from unittest.mock import patch

from mycodex.db import Store
from mycodex.prompts import daily_summary_prompt, experiment_control_prompt, research_control_prompt
from mycodex.research_connectors import search_zotero
from mycodex.research_ops import (
    apply_research_contract,
    build_research_control_context,
    ensure_research_concept_preflight_task,
    extract_research_contract,
    find_research_threads,
    render_research_markdown,
    run_research_task,
    update_research_from_query,
    write_research_markdown,
)
from mycodex.service import ExperimentEventStreamer, ResearchEventStreamer, _experiment_control_context
from mycodex.status import project_snapshot, snapshot_to_jsonable


class StoreTests(unittest.TestCase):
    def test_add_project_and_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "my_codex.sqlite"
            project_path = Path(tmp) / "repo"
            project_path.mkdir()
            with Store(db_path) as store:
                project = store.add_project("demo", project_path)
                self.assertEqual(project["name"], "demo")
                self.assertEqual(project["path"], str(project_path.resolve()))

                thread = {
                    "id": "thread-1",
                    "sessionId": "session-1",
                    "path": "/tmp/rollout.jsonl",
                    "cwd": str(project_path.resolve()),
                    "preview": "hello",
                    "source": "cli",
                    "modelProvider": "openai",
                    "cliVersion": "0.0.0",
                    "status": {"type": "notLoaded"},
                    "createdAt": 10,
                    "updatedAt": 20,
                    "gitInfo": {"sha": "abc", "branch": "main", "originUrl": "git@example"},
                }
                store.upsert_session(project["id"], thread)
                store.commit()
                sessions = store.list_sessions(project["id"])
                self.assertEqual(len(sessions), 1)
                self.assertEqual(sessions[0]["thread_id"], "thread-1")

    def test_runs_are_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                run_id = store.add_run(project["id"], kind="manual", prompt="hi")
                self.assertIsNotNone(store.active_run_for_project(project["id"]))
                store.finish_run(run_id, status="completed", output_text="done")
                self.assertIsNone(store.active_run_for_project(project["id"]))
                runs = store.list_runs(project["id"])
                self.assertEqual(runs[0]["status"], "completed")
                self.assertEqual(runs[0]["output_text"], "done")

    def test_todos_are_recorded_and_updated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                todo_id = store.add_todo(
                    project["id"],
                    title="Run smoke test",
                    body="Use a tiny config.",
                    priority=1,
                )

                todos = store.list_todos(project["id"])
                self.assertEqual(len(todos), 1)
                self.assertEqual(todos[0]["title"], "Run smoke test")
                self.assertEqual(todos[0]["status"], "open")

                store.update_todo_status(todo_id, "done")
                todo = store.get_todo(todo_id)
                self.assertIsNotNone(todo)
                self.assertEqual(todo["status"], "done")
                self.assertIsNotNone(todo["completed_at"])

    def test_experiments_are_recorded_with_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    objective="Improve a metric over repeated runs.",
                    max_iterations=3,
                    config={"metric": "success_rate"},
                )
                self.assertEqual(experiment["status"], "planned")

                event_id = store.add_experiment_event(
                    experiment["id"],
                    event_type="metric",
                    iteration=1,
                    message="First metric.",
                    payload={"success_rate": 0.5},
                    status="running",
                )
                self.assertGreater(event_id, 0)

                updated = store.get_experiment(project["id"], "self-iterate")
                self.assertIsNotNone(updated)
                self.assertEqual(updated["status"], "running")
                self.assertEqual(updated["phase"], "monitoring_wandb")
                self.assertIn("success_rate", updated["last_metric_json"])
                self.assertEqual(updated["current_iteration"], 1)

                events = store.list_experiment_events(experiment["id"])
                self.assertEqual(events[0]["event_type"], "metric")
                self.assertIn("success_rate", events[0]["payload_json"])

                store.add_experiment_event(
                    experiment["id"],
                    event_type="decision",
                    iteration=1,
                    message="Continue with a smaller smoke run.",
                    phase="waiting_for_experiment_start",
                )
                decided = store.get_experiment(project["id"], "self-iterate")
                self.assertEqual(decided["phase"], "waiting_for_experiment_start")
                self.assertEqual(decided["latest_decision"], "Continue with a smaller smoke run.")

    def test_experiment_tasks_are_idempotent_and_leaseable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                )
                source_event_id = store.add_experiment_event(
                    experiment["id"],
                    event_type="control_contract_parsed",
                    message="Contract parsed.",
                )
                run_id = store.add_run(project["id"], kind="experiment:1:control", prompt="decide")

                task = store.upsert_experiment_task(
                    experiment["id"],
                    status="ready",
                    execution_class="monitor-only",
                    idempotency_key="contract-1",
                    contract={"auto_execute": True, "execution_class": "monitor-only"},
                    source_event_id=source_event_id,
                    run_id=run_id,
                )
                duplicate = store.upsert_experiment_task(
                    experiment["id"],
                    status="ready",
                    execution_class="monitor-only",
                    idempotency_key="contract-1",
                    contract={"auto_execute": True, "execution_class": "monitor-only"},
                    source_event_id=999999,
                    run_id=999999,
                )

                self.assertEqual(task["id"], duplicate["id"])
                self.assertEqual(duplicate["source_event_id"], source_event_id)
                self.assertEqual(duplicate["run_id"], run_id)

                leased = store.lease_next_experiment_task(
                    experiment["id"],
                    owner="worker-1",
                    lease_seconds=60,
                )
                self.assertIsNotNone(leased)
                self.assertEqual(leased["status"], "leased")
                self.assertEqual(leased["lease_owner"], "worker-1")
                self.assertEqual(leased["attempts"], 1)

                updated = store.update_experiment_task_status(
                    experiment["id"],
                    leased["id"],
                    "succeeded",
                    result={"ok": True},
                )
                self.assertEqual(updated["status"], "succeeded")
                self.assertEqual(updated["lease_owner"], "")
                self.assertIsNotNone(updated["completed_at"])

    def test_experiment_task_invalid_foreign_keys_are_observation_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                )

                task = store.upsert_experiment_task(
                    experiment["id"],
                    status="ready",
                    execution_class="monitor-only",
                    idempotency_key="contract-invalid-fks",
                    contract={"auto_execute": True, "execution_class": "monitor-only"},
                    source_event_id=123456,
                    run_id=123456,
                )

                self.assertIsNone(task["source_event_id"])
                self.assertIsNone(task["run_id"])

    def test_experiment_event_write_rolls_back_on_mid_write_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                )

                with patch("mycodex.db._artifact_ref_for_event", side_effect=RuntimeError("boom")):
                    with self.assertRaises(RuntimeError):
                        store.add_experiment_event(
                            experiment["id"],
                            event_type="remote_job_started",
                            payload={"log_path": "/tmp/train.log"},
                        )

                event_id = store.add_experiment_event(
                    experiment["id"],
                    event_type="decision",
                    message="Recovered after rollback.",
                )
                self.assertGreater(event_id, 0)
                events = store.list_experiment_events(experiment["id"], limit=10)
                self.assertEqual([event["event_type"] for event in events], ["decision"])

    def test_experiment_streamer_records_agent_deltas(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                )
                streamer = ExperimentEventStreamer(store, experiment["id"], min_chars=4)
                streamer.handle_codex_event(
                    {
                        "method": "item/agentMessage/delta",
                        "params": {"threadId": "thread-1", "turnId": "turn-1", "delta": "abcd"},
                    }
                )
                streamer.handle_codex_event(
                    {
                        "method": "turn/completed",
                        "params": {"threadId": "thread-1", "turn": {"id": "turn-1", "status": "completed"}},
                    }
                )
                events = store.list_experiment_events(experiment["id"])
                self.assertEqual(events[1]["event_type"], "agent_message_delta")
                self.assertIn("abcd", events[1]["payload_json"])
                self.assertEqual(events[0]["event_type"], "codex_event")

    def test_research_threads_ledgers_and_events_are_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                research = store.add_research_thread(
                    project["id"],
                    name="reasoning-ideas",
                    thread_id="thread-1",
                    question="What can be done on LLM reasoning?",
                    max_iterations=5,
                )
                self.assertEqual(research["status"], "planned")

                source_id = store.upsert_research_source(
                    research["id"],
                    title="Tree of Thoughts",
                    url="https://example.test/tot",
                    role="method",
                    depth="skim",
                    status="kept",
                    relevance=4,
                    evidence_quality=3,
                )
                claim_id = store.add_research_claim(
                    research["id"],
                    claim="Deliberate search can improve some reasoning tasks.",
                    status="supported",
                    support="Tree of Thoughts",
                    confidence=3,
                )
                idea_id = store.add_research_idea(
                    research["id"],
                    title="Uncertainty-gated deliberation",
                    hypothesis="Spend test-time compute only when uncertainty is high.",
                    minimal_test="Compare fixed vs gated budget.",
                )
                task_id = store.add_research_task(
                    research["id"],
                    task_type="triage",
                    title="Read closest threats",
                    query="uncertainty gated reasoning LLM",
                )
                event_id = store.add_research_event(
                    research["id"],
                    event_type="decision",
                    message="Triage closest threats next.",
                    status="running",
                )
                self.assertGreater(source_id, 0)
                self.assertGreater(claim_id, 0)
                self.assertGreater(idea_id, 0)
                self.assertGreater(task_id, 0)
                self.assertGreater(event_id, 0)

                updated = store.get_research_thread(project["id"], "reasoning-ideas")
                self.assertEqual(updated["status"], "running")
                self.assertEqual(updated["latest_decision"], "Triage closest threats next.")
                self.assertEqual(store.list_research_sources(research["id"])[0]["title"], "Tree of Thoughts")
                self.assertEqual(store.list_research_claims(research["id"])[0]["status"], "supported")
                self.assertEqual(store.list_research_ideas(research["id"])[0]["title"], "Uncertainty-gated deliberation")
                self.assertEqual(store.list_research_tasks(research["id"])[0]["task_type"], "triage")

                store.update_research_task(task_id, status="done", result={"ok": True})
                task = store.get_research_task(research["id"], task_id)
                self.assertEqual(task["status"], "done")
                self.assertIn("ok", task["result_json"])
                self.assertIsNotNone(task["completed_at"])

                store.update_research_source(source_id, depth="deep", notes="read", metadata={"depth": "deep"})
                source = store.get_research_source(research["id"], source_id)
                self.assertEqual(source["depth"], "deep")
                self.assertEqual(source["notes"], "read")

    def test_research_control_context_includes_current_ledgers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                research = store.add_research_thread(
                    project["id"],
                    name="reasoning-control",
                    thread_id="thread-1",
                    question="What can be done on LLM reasoning?",
                    max_iterations=4,
                )
                source_id = store.upsert_research_source(
                    research["id"],
                    title="Verifier-Guided Reasoning",
                    url="https://example.test/verifier",
                    role="method",
                    depth="skim",
                    status="kept",
                    relevance=5,
                    evidence_quality=4,
                    notes="A retained method source.",
                )
                store.add_research_claim(
                    research["id"],
                    claim="Verifier feedback can guide test-time search.",
                    status="supported",
                    support="Verifier-Guided Reasoning",
                    source_id=source_id,
                    confidence=4,
                )
                idea_id = store.add_research_idea(
                    research["id"],
                    title="Adaptive verifier budget",
                    hypothesis="Use verifier confidence to allocate reasoning budget.",
                    mechanism="Spend more search only when confidence is unstable.",
                    minimal_test="Compare fixed and adaptive budgets.",
                    kill_criteria="No quality or cost improvement.",
                    score={"evidence": 3},
                )
                store.add_research_task(
                    research["id"],
                    task_type="threat_search",
                    title="Find nearest verifier-budget threats",
                    idea_id=idea_id,
                    payload={"idea_ids": [idea_id]},
                )
                store.add_research_event(
                    research["id"],
                    event_type="decision",
                    message="Run nearest-threat search before ranking.",
                    phase="idea_tournament",
                )

                context = json.loads(build_research_control_context(store, "demo", "reasoning-control"))

                self.assertEqual(context["research"]["name"], "reasoning-control")
                self.assertEqual(context["research"]["phase"], "idea_tournament")
                self.assertEqual(context["sources"][0]["title"], "Verifier-Guided Reasoning")
                self.assertEqual(context["claims"][0]["source_title"], "Verifier-Guided Reasoning")
                self.assertEqual(context["ideas"][0]["title"], "Adaptive verifier budget")
                self.assertEqual(context["tasks"][0]["task_type"], "threat_search")
                self.assertIn("nearest-threat", context["events"][0]["message"])
                self.assertIsNone(context["concept_preflight"])

    def test_research_concept_preflight_builds_icrl_query_map(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                research = store.add_research_thread(
                    project["id"],
                    name="icrl-directions",
                    thread_id="thread-1",
                    question="LLM+in-context RL 最新研究和能连接到 agent Sokoban 的 idea",
                )
                task_id = store.add_research_task(
                    research["id"],
                    task_type="concept_preflight",
                    payload={"auto_queue_search": True},
                )

                result = run_research_task(store, "demo", "icrl-directions", task_id=task_id)

                self.assertEqual(result["status"], "done")
                self.assertEqual(result["profile"], "icrl_agent")
                self.assertTrue(any("LaMer" in query for query in result["queries"]))
                self.assertTrue(any("MAGE" in query for query in result["queries"]))
                self.assertGreaterEqual(len(result["queued_tasks"]), 1)
                context = json.loads(build_research_control_context(store, "demo", "icrl-directions"))
                self.assertEqual(context["concept_preflight"]["profile"], "icrl_agent")
                self.assertIn("anchor_promotion_criteria", context["concept_preflight"])
                markdown = render_research_markdown(store, "demo", "icrl-directions")
                self.assertIn("## Concept Preflight", markdown)
                self.assertIn("LaMer", markdown)

    def test_search_task_auto_records_concept_preflight_before_search(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                research = store.add_research_thread(
                    project["id"],
                    name="icrl-search",
                    thread_id="thread-1",
                    question="LLM in-context RL for test-time adaptation in language agents",
                )
                task_id = store.add_research_task(
                    research["id"],
                    task_type="search",
                    query="in-context RL language agents",
                    payload={"expand_queries": True},
                )
                seen_queries: list[str] = []

                def fake_searcher(query: str, **_kwargs):
                    seen_queries.append(query)
                    return [
                        {
                            "title": "Meta-RL Induces Exploration in Language Agents",
                            "url": "https://example.test/lamer",
                            "abstract": "LaMer trains language agents for test-time adaptation from trial-and-error feedback.",
                        }
                    ]

                result = run_research_task(
                    store,
                    "demo",
                    "icrl-search",
                    task_id=task_id,
                    searcher=fake_searcher,
                    limit=1,
                )

                self.assertEqual(result["status"], "done")
                self.assertEqual(result["concept_preflight"]["profile"], "icrl_agent")
                self.assertTrue(any("LaMer" in query for query in seen_queries))
                claims = store.list_research_claims(research["id"])
                self.assertIn("concept_preflight", claims[0]["metadata_json"])

    def test_research_find_update_and_markdown_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                research = store.add_research_thread(
                    project["id"],
                    name="reasoning-verifiers",
                    thread_id="thread-1",
                    question="Track verifier-guided LLM reasoning ideas.",
                )
                store.upsert_research_source(
                    research["id"],
                    title="Verifier-Guided Reasoning",
                    url="https://example.test/verifier",
                    role="method",
                    status="kept",
                    notes="Verifier feedback for test-time search.",
                )
                store.add_research_idea(
                    research["id"],
                    title="Adaptive verifier budget",
                    hypothesis="Use verifier confidence to allocate reasoning budget.",
                    minimal_test="Compare fixed and adaptive verifier budgets.",
                )

                matches = find_research_threads(store, "demo", "verifier budget")
                self.assertEqual(matches[0]["research"]["name"], "reasoning-verifiers")
                self.assertIn("idea", matches[0]["matched_fields"])

                update = update_research_from_query(
                    store,
                    "demo",
                    "verifier budget 2026",
                    max_cycles=0,
                )
                self.assertEqual(update["name"], "reasoning-verifiers")
                self.assertGreater(update["task_id"], 0)
                self.assertTrue(Path(update["markdown_path"]).exists())
                task = store.get_research_task(research["id"], update["task_id"])
                self.assertEqual(task["task_type"], "search")
                self.assertIn("verifier budget 2026", task["query"])

                markdown = render_research_markdown(store, "demo", "reasoning-verifiers")
                self.assertIn("# Research: reasoning-verifiers", markdown)
                self.assertIn("Verifier-Guided Reasoning", markdown)
                path = write_research_markdown(store, "demo", "reasoning-verifiers")
                self.assertTrue(path.exists())
                self.assertIn("SQLite is the source of truth", path.read_text(encoding="utf-8"))

    def test_research_contract_is_parsed_and_applied(self) -> None:
        text = """
```json
{
  "research_question": "What can be done on GUI agents?",
  "phase": "idea_tournament",
  "auto_continue": true,
  "next_action": {
    "type": "triage",
    "reason": "Read GUI benchmark threats",
    "queries": ["GUI agent benchmark OSWorld WebArena"],
    "stop_condition": "top benchmarks identified"
  },
  "source_ledger_delta": [
    {
      "title": "OSWorld",
      "url": "https://example.test/osworld",
      "source_type": "paper",
      "role": "benchmark",
      "depth": "abstract",
      "keep": true
    }
  ],
  "claim_ledger_delta": [
    {
      "claim": "GUI agents need stateful evaluation beyond web-only tasks.",
      "support": "OSWorld",
      "status": "supported",
      "confidence": 3
    }
  ],
  "idea_ledger_delta": [
    {
      "idea": "State-change feedback",
      "hypothesis": "Feedback on GUI state changes improves recovery.",
      "minimal_test": "Add state-change diagnostics to a GUI benchmark.",
      "kill_criteria": ["No effect on recovery tasks"]
    }
  ]
}
```
"""
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                store.add_research_thread(
                    project["id"],
                    name="gui-agents",
                    thread_id="thread-1",
                    question="What can be done on GUI agents?",
                )

                contract = extract_research_contract(text)
                self.assertIsNotNone(contract)
                applied = apply_research_contract(store, "demo", "gui-agents", contract)
                self.assertEqual(applied["sources"], 1)
                self.assertEqual(applied["claims"], 1)
                self.assertEqual(applied["ideas"], 1)
                self.assertEqual(applied["tasks"], 1)

                research = store.get_research_thread(project["id"], "gui-agents")
                self.assertEqual(research["phase"], "idea_tournament")
                self.assertIn("Research contract applied", research["phase_message"])
                self.assertEqual(store.list_research_sources(research["id"])[0]["status"], "kept")
                self.assertEqual(store.list_research_tasks(research["id"])[0]["status"], "queued")

    def test_research_contract_normalizes_llm_idea_statuses(self) -> None:
        contract = {
            "research_question": "What can be done on in-context RL?",
            "phase": "idea_tournament",
            "auto_continue": False,
            "idea_ledger_delta": [
                {
                    "idea": "Trace curriculum",
                    "hypothesis": "Outcome-labeled traces improve decisions.",
                    "status": "downgraded",
                }
            ],
            "next_action": {"type": "ask_user", "reason": "Choose a direction."},
        }
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                store.add_research_thread(
                    project["id"],
                    name="icrl",
                    thread_id="thread-1",
                )
                applied = apply_research_contract(store, "demo", "icrl", contract)
                self.assertEqual(applied["ideas"], 1)
                idea = store.list_research_ideas(store.get_research_thread(project["id"], "icrl")["id"])[0]
                self.assertEqual(idea["status"], "candidate")

    def test_research_streamer_records_agent_deltas(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                research = store.add_research_thread(
                    project["id"],
                    name="auto-research",
                    thread_id="thread-1",
                )
                streamer = ResearchEventStreamer(store, research["id"], min_chars=4)
                streamer.handle_codex_event(
                    {
                        "method": "item/agentMessage/delta",
                        "params": {"threadId": "thread-1", "turnId": "turn-1", "delta": "abcd"},
                    }
                )
                streamer.handle_codex_event(
                    {
                        "method": "turn/completed",
                        "params": {"threadId": "thread-1", "turn": {"id": "turn-1", "status": "completed"}},
                    }
                )
                events = store.list_research_events(research["id"])
                self.assertEqual(events[1]["event_type"], "agent_message_delta")
                self.assertIn("abcd", events[1]["payload_json"])
                self.assertEqual(events[0]["event_type"], "codex_event")

    def test_research_task_runner_searches_and_ingests_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                research = store.add_research_thread(
                    project["id"],
                    name="auto-research",
                    thread_id="thread-1",
                    question="What can be done on LLM reasoning?",
                )
                search_task = store.add_research_task(
                    research["id"],
                    task_type="search",
                    query="test-time reasoning",
                )

                def fake_searcher(query: str, **_kwargs):
                    return [
                        {
                            "title": "Test-time reasoning survey",
                            "url": "https://example.test/reasoning",
                            "abstract": f"About {query}",
                        }
                    ]

                result = run_research_task(
                    store,
                    "demo",
                    "auto-research",
                    task_id=search_task,
                    searcher=fake_searcher,
                )
                self.assertEqual(result["status"], "done")
                self.assertEqual(result["sources"], 1)
                task = store.get_research_task(research["id"], search_task)
                self.assertEqual(task["status"], "done")
                source = store.list_research_sources(research["id"])[0]
                self.assertEqual(source["title"], "Test-time reasoning survey")

                read_task = store.add_research_task(
                    research["id"],
                    task_type="deep_read",
                    source_id=source["id"],
                )

                def fake_ingester(location: str, **_kwargs):
                    return {
                        "location": location,
                        "resolved_location": location,
                        "content_type": "text/plain",
                        "bytes": 12,
                        "text": "important excerpt",
                        "method": "fake",
                    }

                read_result = run_research_task(
                    store,
                    "demo",
                    "auto-research",
                    task_id=read_task,
                    ingester=fake_ingester,
                )
                self.assertEqual(read_result["status"], "done")
                self.assertEqual(read_result["chars"], len("important excerpt"))
                updated_source = store.get_research_source(research["id"], source["id"])
                self.assertEqual(updated_source["status"], "kept")
                self.assertIn("important excerpt", updated_source["notes"])

    def test_research_deep_read_resolves_sources_to_read_titles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                research = store.add_research_thread(
                    project["id"],
                    name="icrl",
                    thread_id="thread-1",
                )
                source_id = store.upsert_research_source(
                    research["id"],
                    title="SKILL0: In-Context Agentic Reinforcement Learning",
                    url="https://example.test/skill0.pdf",
                    status="kept",
                    evidence_quality=2,
                )
                task_id = store.add_research_task(
                    research["id"],
                    task_type="deep_read",
                    payload={"sources_to_read": ["SKILL0: In-Context Agentic Reinforcement Learning"]},
                )

                def fake_ingester(location: str, **_kwargs):
                    return {
                        "location": location,
                        "resolved_location": location,
                        "content_type": "application/pdf",
                        "bytes": 10,
                        "text": "deep paper text " * 300,
                        "method": "fake",
                    }

                result = run_research_task(
                    store,
                    "demo",
                    "icrl",
                    task_id=task_id,
                    ingester=fake_ingester,
                )
                self.assertEqual(result["status"], "done")
                self.assertEqual(len(result["ingested"]), 1)
                self.assertEqual(result["ingested"][0]["source_id"], source_id)
                updated = store.get_research_source(research["id"], source_id)
                self.assertEqual(updated["depth"], "deep")

    def test_research_task_runner_filters_and_synthesizes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                research = store.add_research_thread(
                    project["id"],
                    name="gui-research",
                    thread_id="thread-1",
                    question="LLM+GUI agents: status and innovation points around OSWorld and GUI grounding.",
                )
                search_task = store.add_research_task(
                    research["id"],
                    task_type="search",
                    query="LLM GUI agent benchmark OSWorld grounding",
                    payload={"expand_queries": False, "auto_synthesize": True},
                )

                def fake_searcher(_query: str, **_kwargs):
                    return [
                        {
                            "title": "GUI Agent Benchmark for OSWorld Grounding",
                            "url": "https://example.test/gui",
                            "abstract": "A GUI agent benchmark for screen grounding and computer use.",
                        },
                        {
                            "title": "Chemical Data Extraction with Language Models",
                            "url": "https://example.test/chemistry",
                            "abstract": "A chemistry survey unrelated to GUI agents.",
                        },
                    ]

                search_result = run_research_task(
                    store,
                    "demo",
                    "gui-research",
                    task_id=search_task,
                    searcher=fake_searcher,
                )
                self.assertEqual(search_result["sources"], 1)
                self.assertEqual(len(search_result["rejected_source_ids"]), 1)

                synth_task = store.next_research_task(research["id"])
                self.assertIsNotNone(synth_task)
                synth_result = run_research_task(
                    store,
                    "demo",
                    "gui-research",
                    task_id=synth_task["id"],
                )
                self.assertEqual(synth_result["profile"], "gui")
                self.assertGreaterEqual(synth_result["claims"], 1)
                self.assertGreaterEqual(synth_result["ideas"], 1)
                idea_titles = [row["title"] for row in store.list_research_ideas(research["id"])]
                self.assertIn("State-change verifier for GUI agents", idea_titles)

    def test_research_refinement_tasks_read_threat_search_and_critique(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                research = store.add_research_thread(
                    project["id"],
                    name="gui-refine",
                    thread_id="thread-1",
                    question="LLM+GUI agents need state-change verification and recovery.",
                )
                source_id = store.upsert_research_source(
                    research["id"],
                    title="OSWorld: Benchmarking Multimodal Agents",
                    url="https://example.test/osworld",
                    role="benchmark",
                    depth="abstract",
                    status="kept",
                    relevance=5,
                    evidence_quality=3,
                    metadata={"provider": "arxiv", "pdf_url": "https://example.test/osworld.pdf"},
                )
                idea_id = store.add_research_idea(
                    research["id"],
                    title="State-change verifier for GUI agents",
                    hypothesis="Checking predicted UI state transitions improves recovery.",
                    mechanism="Predict expected UI deltas and compare them after each action.",
                    minimal_test="Evaluate on a GUI benchmark subset.",
                    kill_criteria="No recovery improvement over reflection baseline.",
                    score={"evidence": 3},
                )

                read_task = store.add_research_task(
                    research["id"],
                    task_type="deep_read_top",
                    payload={"max_sources": 1},
                )

                def fake_ingester(location: str, **_kwargs):
                    return {
                        "location": location,
                        "resolved_location": location,
                        "content_type": "application/pdf",
                        "bytes": 4000,
                        "text": "state verification recovery GUI benchmark " * 120,
                        "method": "fake-pdf",
                    }

                read_result = run_research_task(
                    store,
                    "demo",
                    "gui-refine",
                    task_id=read_task,
                    ingester=fake_ingester,
                )
                self.assertEqual(len(read_result["ingested"]), 1)
                source = store.get_research_source(research["id"], source_id)
                self.assertEqual(source["depth"], "deep")

                threat_task = store.add_research_task(
                    research["id"],
                    task_type="threat_search",
                    payload={"idea_ids": [idea_id], "max_queries": 1},
                )

                def fake_searcher(_query: str, **_kwargs):
                    return [
                        {
                            "title": "GUI Grounding with Action Verification",
                            "url": "https://example.test/threat",
                            "abstract": "GUI agent grounding with post-action verification and recovery.",
                        }
                    ]

                threat_result = run_research_task(
                    store,
                    "demo",
                    "gui-refine",
                    task_id=threat_task,
                    searcher=fake_searcher,
                    providers=["arxiv"],
                )
                self.assertEqual(threat_result["ideas"], 1)
                self.assertEqual(threat_result["sources"], 1)
                idea = store.get_research_idea(research["id"], idea_id)
                self.assertIn("threat_source_ids", idea["metadata_json"])

                critique_task = store.add_research_task(
                    research["id"],
                    task_type="idea_critique",
                    payload={"idea_ids": [idea_id]},
                )
                critique_result = run_research_task(
                    store,
                    "demo",
                    "gui-refine",
                    task_id=critique_task,
                )
                self.assertEqual(critique_result["ideas"], 1)
                updated = store.get_research_idea(research["id"], idea_id)
                self.assertIn(updated["status"], {"active", "candidate"})
                self.assertIn("Idea critique:", updated["notes"])
                self.assertIn("total", updated["score_json"])

    def test_zotero_connector_searches_local_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            zotero_dir = Path(tmp) / "Zotero"
            storage = zotero_dir / "storage" / "ATTACH1"
            storage.mkdir(parents=True)
            (storage / "paper.pdf").write_bytes(b"%PDF- fake")
            db_path = zotero_dir / "zotero.sqlite"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE items (
                    itemID INTEGER PRIMARY KEY,
                    itemTypeID INT NOT NULL,
                    dateModified TIMESTAMP NOT NULL,
                    key TEXT NOT NULL
                );
                CREATE TABLE itemTypes (itemTypeID INTEGER PRIMARY KEY, typeName TEXT);
                CREATE TABLE itemData (itemID INT, fieldID INT, valueID INT);
                CREATE TABLE itemDataValues (valueID INTEGER PRIMARY KEY, value TEXT);
                CREATE TABLE fields (fieldID INTEGER PRIMARY KEY, fieldName TEXT);
                CREATE TABLE itemCreators (itemID INT, creatorID INT, creatorTypeID INT, orderIndex INT);
                CREATE TABLE creators (creatorID INTEGER PRIMARY KEY, firstName TEXT, lastName TEXT, fieldMode INT);
                CREATE TABLE itemTags (itemID INT, tagID INT, type INT);
                CREATE TABLE tags (tagID INTEGER PRIMARY KEY, name TEXT);
                CREATE TABLE collectionItems (collectionID INT, itemID INT, orderIndex INT);
                CREATE TABLE collections (collectionID INTEGER PRIMARY KEY, collectionName TEXT);
                CREATE TABLE itemAttachments (
                    itemID INTEGER PRIMARY KEY,
                    parentItemID INT,
                    linkMode INT,
                    contentType TEXT,
                    charsetID INT,
                    path TEXT
                );
                INSERT INTO itemTypes VALUES (3, 'attachment'), (22, 'journalArticle');
                INSERT INTO items VALUES (1, 22, '2026-01-01', 'ITEM1');
                INSERT INTO items VALUES (2, 3, '2026-01-01', 'ATTACH1');
                INSERT INTO fields VALUES
                    (1, 'title'),
                    (2, 'abstractNote'),
                    (6, 'date'),
                    (13, 'url'),
                    (38, 'publicationTitle'),
                    (59, 'DOI');
                INSERT INTO itemDataValues VALUES
                    (1, 'Adaptive LLM Reasoning'),
                    (2, 'A paper about test-time reasoning.'),
                    (3, '2026'),
                    (4, 'https://example.test/paper'),
                    (5, 'TestConf'),
                    (6, '10.0000/test');
                INSERT INTO itemData VALUES
                    (1, 1, 1),
                    (1, 2, 2),
                    (1, 6, 3),
                    (1, 13, 4),
                    (1, 38, 5),
                    (1, 59, 6);
                INSERT INTO creators VALUES (1, 'Ada', 'Lovelace', 0);
                INSERT INTO itemCreators VALUES (1, 1, 1, 0);
                INSERT INTO itemAttachments VALUES
                    (2, 1, 1, 'application/pdf', NULL, 'storage:paper.pdf');
                """
            )
            conn.commit()
            conn.close()

            results = search_zotero("test-time reasoning", limit=2, zotero_dir=zotero_dir)
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].provider, "zotero")
            self.assertEqual(results[0].title, "Adaptive LLM Reasoning")
            self.assertEqual(Path(results[0].pdf_url).resolve(), (storage / "paper.pdf").resolve())

    def test_telegram_users_and_bindings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)

                self.assertFalse(store.telegram_user_allowed("42"))
                store.allow_telegram_user("42", "alice")
                self.assertTrue(store.telegram_user_allowed("42"))
                self.assertEqual(store.get_telegram_user("42")["username"], "alice")

                store.set_telegram_binding(
                    chat_id="-100",
                    message_thread_id="7",
                    project_id=project["id"],
                    thread_id="thread-1",
                )
                binding = store.get_telegram_binding(chat_id="-100", message_thread_id="7")
                self.assertIsNotNone(binding)
                self.assertEqual(binding["project_name"], "demo")
                self.assertEqual(binding["thread_id"], "thread-1")

                removed = store.remove_telegram_binding(chat_id="-100", message_thread_id="7")
                self.assertTrue(removed)
                self.assertIsNone(store.get_telegram_binding(chat_id="-100", message_thread_id="7"))

    def test_project_snapshot_includes_todos_and_failed_run_health(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                store.add_todo(project["id"], title="Fix failure", status="blocked")
                summary_id = store.add_run(project["id"], kind="daily_summary", prompt="summary")
                store.finish_run(summary_id, status="completed", output_text="today summary")
                run_id = store.add_run(project["id"], kind="manual", prompt="hi")
                store.finish_run(run_id, status="failed", error="boom")
                store.conn.execute(
                    "UPDATE runs SET started_at = started_at + 1 WHERE id = ?",
                    (run_id,),
                )
                store.commit()
                store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    status="running",
                )

                snapshot = snapshot_to_jsonable(project_snapshot(store, "demo"))
                self.assertEqual(snapshot["open_todo_count"], 1)
                self.assertEqual(snapshot["active_experiment_count"], 1)
                self.assertEqual(snapshot["health"]["severity"], "danger")
                self.assertEqual(snapshot["daily_summary"]["latest"]["id"], summary_id)
                self.assertEqual(snapshot["daily_summary"]["latest"]["output_text"], "today summary")
                self.assertEqual(snapshot["latest_run"]["status"], "failed")


class PromptTests(unittest.TestCase):
    def test_daily_summary_mentions_project(self) -> None:
        prompt = daily_summary_prompt("demo")
        self.assertIn("demo", prompt)
        self.assertIn("每日进度总结", prompt)

    def test_experiment_control_prompt_includes_context_and_strict_json_rule(self) -> None:
        prompt = experiment_control_prompt(
            project_name="icrl",
            experiment_name="procgen-multi-env-icl",
            experiment_id=5,
            objective="Improve held-out Procgen performance.",
            max_iterations=20,
            remote_ref="root@example.com:/root/icrl",
            recent_context='{"latest_decision": "baseline eval completed", "ad_bossfight": 0.06}',
        )
        self.assertIn("baseline eval completed", prompt)
        self.assertIn("不要重复已经完成", prompt)
        self.assertIn("json.loads", prompt)
        self.assertIn("\\n", prompt)
        self.assertIn("不能出现 literal newline", prompt)
        self.assertIn("revision_response", prompt)
        self.assertIn("required_changes", prompt)
        self.assertIn("artifacts", prompt)
        self.assertIn("remote_terminal_complete_ready_for_next", prompt)

    def test_experiment_control_context_keeps_peer_review_over_noisy_deltas(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Store(Path(tmp) / "db.sqlite") as store:
                project_path = Path(tmp) / "repo"
                project_path.mkdir()
                project = store.add_project("demo", project_path)
                experiment = store.add_experiment(
                    project["id"],
                    name="self-iterate",
                    thread_id="thread-1",
                    status="running",
                )
                store.add_experiment_event(
                    experiment["id"],
                    event_type="peer_review_decision",
                    level="warning",
                    message="Peer review gate decision: revise.",
                    payload={
                        "decision": {
                            "decision": "revise",
                            "required_changes": ["address real objection"],
                        }
                    },
                )
                for idx in range(50):
                    store.add_experiment_event(
                        experiment["id"],
                        event_type="agent_message_delta",
                        message=f"noisy patch chunk {idx}",
                    )

                experiment = store.get_experiment(project["id"], "self-iterate")
                self.assertIsNotNone(experiment)
                context = json.loads(_experiment_control_context(store, experiment, limit=5))

                self.assertEqual(context["recent_noisy_events_omitted"], 50)
                self.assertIn(
                    "address real objection",
                    json.dumps(context["latest_peer_review_decisions_desc"], ensure_ascii=False),
                )
                self.assertEqual(
                    context["latest_peer_review_decisions_desc"][0]["type"],
                    "peer_review_decision",
                )
                self.assertNotIn(
                    "noisy patch chunk 49",
                    json.dumps(context["recent_events_desc"], ensure_ascii=False),
                )

    def test_research_control_prompt_mentions_contract(self) -> None:
        prompt = research_control_prompt(
            project_name="demo",
            research_name="reasoning",
            research_id=1,
            question="What can be done on reasoning?",
            max_iterations=3,
            ledger_context='{"sources": [{"title": "Verifier-Guided Reasoning"}]}',
        )
        self.assertIn("reasoning", prompt)
        self.assertIn("research ledger snapshot", prompt)
        self.assertIn("Verifier-Guided Reasoning", prompt)
        self.assertIn("source_ledger_delta", prompt)
        self.assertIn("idea_ledger_delta", prompt)
        self.assertIn("deep_read_top", prompt)
        self.assertIn("threat_search", prompt)
        self.assertIn("idea_critique", prompt)


if __name__ == "__main__":
    unittest.main()
