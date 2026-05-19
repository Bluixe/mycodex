# Codex Memory

Maintained project context for future Codex sessions. Keep mandatory rules in
`AGENTS.md`; use this file for durable background, decisions, and recurring
pitfalls that should be easy to update.

## Background
- `mycodex` is a local control plane around Codex projects, sessions, runs,
  rules, todos, a web dashboard, and Telegram bindings.
- The app keeps its own state in `data/my_codex.sqlite` and treats Codex's
  session store as an external runtime.
- `mycodex` currently sends work through the Codex app-server JSON-RPC API.
  It can start or resume threads, then records the run result locally.

## Durable Decisions
- Do not write directly to Codex internal automation tables.
- For self-iterating experiments, the bound Codex session should not write
  `mycodex` SQLite directly or call `mycodex experiments event` itself. The
  outer `mycodex` control plane records events to avoid SQLite lock conflicts.
- Self-iterating experiments should support low-risk auto-execution: when the
  bound session returns `auto_execute: true` with a complete contract for
  read-only preflight, diagnostic patch, monitor-only, or tightly budgeted
  smoke training, the outer control plane may commit/push intended changes,
  launch the remote job, and monitor stop conditions without another user
  confirmation.
- Prefer service-layer helpers in `mycodex/service.py` over duplicating DB or
  app-server protocol logic in CLI, web, daemon, or Telegram code.
- Project defaults are conservative by design: `read-only` sandbox and
  `approval_policy=never`. Use explicit send-time overrides for implementation
  tasks.
- Todos are lightweight per-project dashboard state. They are useful operating
  context, but Codex does not automatically see them unless `mycodex` injects
  them into a prompt.

## Current Operating Notes
- The local dashboard is expected on `127.0.0.1:8765`.
- Experiment automation now has control-plane commands for local git push,
  remote SSH launch/status, and W&B polling:
  `experiments git-push`, `experiments launch-remote`,
  `experiments remote-status`, and `experiments poll-wandb`.
- Remote experiment launch records PID/log/commit in `remote_job_started`.
  W&B polling records compact `metric` events plus `decision`, `failed`, or
  `wandb_run_finished` events when conditions are reached.
- Remote experiment commands can use `--ssh-password-file` for password-only
  servers. Keep the password under ignored local `data/secrets/`, require the
  local `expect` binary, and record only the auth mode, never the password.
- Research automation is intentionally split into levels: skills define the
  behavioral protocol, while the L3 `mycodex research` control plane persists
  source, claim, idea, task, and event ledgers around a bound Codex session.
  This is the right place to build auto-research state, not a single prompt or
  skill file.
- The L3 research worker now has two paths: local queued tasks can search public
  sources or ingest URL/PDF/text into the source ledger, while cognitive tasks
  can be delegated back to the bound Codex control session by
  `mycodex research supervise`.
- Research search also supports a read-only local Zotero connector. It copies
  `~/Zotero/zotero.sqlite` before querying to avoid lock conflicts, stores
  matched items as Zotero source entries, and can pass local PDF attachments to
  later `deep_read` ingestion tasks. Use `ZOTERO_DATA_DIR` for a non-default
  Zotero data directory.
- Research quality now depends on topic profiles in `mycodex/research_quality.py`:
  bounded anchor query expansion, topic-aware source scoring/rejection, coverage
  summaries, and local synthesis from retained sources into claim and idea
  ledgers. This is deliberately heuristic and should be followed by deeper
  source reading before major research commitments.
- The local research worker also supports refinement tasks:
  `deep_read_top` ingests top retained sources, `threat_search` finds nearest
  related work for idea ids, and `idea_critique` writes feasibility, novelty,
  falsifiability, risk, and revised minimal-test notes back to idea metadata.
  Use these as a triage/iteration layer, not as final novelty proof.
- Research control turns are the LLM decision layer, not just a trigger for a
  local worker. `mycodex research start` injects a compact JSON snapshot of the
  current source, claim, idea, task, and event ledgers into the bound Codex
  session; that session should decide the next contract, while the outer
  `mycodex` process executes low-risk worker tasks and writes ledger updates.
- Research has a topic lookup/update entrance: `mycodex research find` searches
  SQLite ledgers for relevant prior threads, and `mycodex research update`
  queues an incremental search task on the best match before running the
  supervisor. Markdown snapshots under `data/research_markdown/` are auxiliary
  exports generated from SQLite and must not become the source of truth.
- Runtime artifacts under `data/` and `logs/` are local generated state.
- The Telegram adapter serializes work per chat/topic/project to avoid
  concurrent turns against the same thread and SQLite lock surprises.
- When Desktop is already running, `codex app-server proxy` may be preferable
  to launching a separate stdio app-server if UI refresh behavior matters.

## Pitfalls
- `python -m mycodex.cli` imports the module but does not execute the CLI; use
  `python -m mycodex` or installed `mycodex`.
- `mycodex` projects are not the same registry as Codex Desktop projects.
- A resumed thread keeps its old title even after new turns, so Desktop may not
  show the newest task in the list title.
- Never paste or commit Telegram tokens, API keys, or proxy credentials into
  docs or code.
