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
- Version 0.3 sends/summaries always persist jobs; the CLI consumes immediately
  unless `--enqueue` saves only. Pause requires a confirmed interrupted terminal
  event, not just an interrupt RPC acknowledgement. Ambiguous effects stay
  blocked. Resume preserves the thread and cumulative budget; unknown prior
  token usage cannot become a fabricated zero. Goal edits preserve authority
  and use optimistic revisions. Session-index writes must commit before model
  calls so independent reviews do not hold the SQLite writer lock.
- Task deadlines include setup RPCs and turn waiting, with separate bounded
  interrupt/cleanup grace. A completed turn that overshoots its observed token
  limit remains completed in the runtime record while its queue job blocks
  dependent automatic work. Usage-based limits cannot undo input already billed.
- The optional pinned SDK transport shares fail-closed lifecycle parsing and
  explicitly declines approval requests; select with MYCODEX_RUNTIME_BACKEND=sdk.
  Native app-server remains the default. The PDF documents extra uses bounded
  subprocess extraction; headings/tables/numbered formulas are located candidates,
  not semantically verified research evidence.
- Review routing uses explicit risk flags and persists its reason. Cost is actual
  reported usage or unknown. Accepted owner-verified findings are distinct from
  net savings, which require a paired baseline. OTLP exports contain metadata
  only, correlate all matching run attempts, and flag event truncation.
- Operational benchmarks distinguish constructed faults, retained historical
  traces and real material/model tasks. The first small SQLite recency-stress
  pair measures source-context coverage, not general scientific reasoning.
  Unavailable refs/capabilities and skipped integrations never count as passes.
- Version 0.2 adds `jobs` as the durable queue for web, Telegram, and scheduled
  work. Submission freezes runtime permissions and thread selection. Expired
  unstarted leases can be retried; expired running leases are blocked for
  inspection. Writes in the same project and turns in the same thread serialize.
- Native JSON output schemas are normalized into versioned contracts and then
  type/policy checked. A failed/interrupted turn or invalid contract must never
  be treated as a successful executable plan. Prefer `MYCODEX_CODEX_BIN`, or the
  valid desktop `CODEX_CLI_PATH`, over a stale PATH runtime; never rewrite global
  user configuration to accommodate an old binary.
- Research source versions/passages are immutable. Provider failures are coverage
  gaps, not negative evidence. Template claims remain uncertain; passage-linked
  verification records are required for promotion. Retrieval is lexical and
  locators remain unknown when extraction cannot establish a page or section.
- Managed remote execution uses a stable execution ID, an atomic claim,
  persistent receipts, numeric time budgets, and immutable local manifests.
  Repeating an execution ID observes the existing run. A result gate records
  artifact/receipt consistency separately from scientific validity. Lease
  ownership must be renewed and atomically fenced at start and completion.
- MCP is a local STDIO interface and read-only by default. Optional writes submit
  or cancel bounded jobs and cannot enlarge project permissions. Bounded review
  workflows use independent read-only threads; their proposals grant no new
  authority. Recorded-run evaluation is a consistency check, not a scientific
  quality benchmark. SDK, embedding, and external tracing integrations remain
  optional future work as of 0.2; 0.3 adds the opt-in SDK and trace exporter.
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
- Experiment execution contracts are persisted as durable protocol tasks in
  `experiment_tasks`. The bound Codex session proposes a contract; the outer
  control plane records it as `proposed`, `ready`, or `blocked`; and only a
  leased `ready` task is executed. This keeps consensus, wakeup, execution, and
  completion states in SQLite instead of relying on an implicit phase label
  such as `eval_pending`.
- Training contracts must be hypothesis-driven rather than hyperparameter
  fishing. For `smoke-train` and `full-train`, the execution contract must
  include a `research` block with a falsifiable hypothesis, mechanism,
  baseline, intervention, predictions, controls, support/falsification/
  ambiguity criteria, analysis plan, and anti-sweep rationale. The outer
  control plane refuses incomplete training contracts before launching remote
  work.
- The `icrl/procgen-multi-env-icl` experiment has a durable research handoff at
  `docs/procgen-multi-env-icl-research-handoff.md`. Future control turns must
  treat the accepted mechanism as competence/phase-coordinate conditioning, not
  raw-return matching, and should avoid naive hyperparameter sweeps. Resume via
  `train_completed -> eval_pending -> launch_eval -> result_gate ->
  next_research_decision`.
- Experiment iteration should use a peer Codex review gate before execution
  instead of repeatedly waiting for user approval. After drafting a minimal
  implementation, diagnostic validation, smoke run, or larger experiment plan,
  start or resume a separate Codex session with the project background, current
  metrics, proposed contract, expected runtime/cost, risks, and rationale.
  Incorporate its critique, record the resulting decision/contract, then
  execute directly when the contract is complete and within the user's standing
  authorization.
- The peer review gate should produce actionable objections or approval, not
  open-ended discussion. If the reviewer flags missing metrics, unclear stop
  conditions, unsafe file changes, uncertain remote state, or excessive cost,
  revise the plan or run a cheaper diagnostic first. Escalate to the user only
  for credentials, billing/quota changes, destructive or unrelated-data
  operations, or genuinely ambiguous research direction.
- Peer review must not have an unbounded veto over authorized experiment
  progress. The default timed bypass policy is explicit: after 1800 seconds of
  `revise` or peer-review timeout, the control plane may bypass review only if
  the execution contract is complete, the class is allowed by policy, `block`
  was not returned, and `full-train` has explicit `allow_full_train`. The
  bypass decision is recorded as `peer_review_bypass_eligible`,
  `peer_review_bypassed`, or `peer_review_bypass_denied`.
- Peer review is now a bounded safety gate, not a general artifact/provenance
  veto. `eval-only` and `smoke-eval` are read-only, low-risk eval collection
  classes and skip peer review entirely. Reviewer objections about artifact
  gates or method claims may set `method_claim_allowed=false`, but must not
  block eval data collection. Peer review may block only destructive
  operations, unauthorized long/paid training, or overwriting/committing
  unowned local changes. Consecutive `revise` is capped at one automatic
  revision; the next `revise` either executes a safe fallback contract or
  records `paused_with_reviewer_objections` instead of opening more Codex
  sessions.
- Dirty local worktrees only block contracts that need local patch/commit. An
  `eval-only` or `smoke-eval` contract that explicitly binds an already-pushed
  `commit`/`remote.commit` may launch remote eval from that commit even when
  unrelated local files are dirty.
- Terminal train completion should progress through
  `train_completed -> eval_pending -> launch_eval -> result_gate ->
  next_research_decision`. The control plane should not route terminal train
  completion through repeated peer-review/block loops before collecting eval
  data.
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
  `experiments remote-status`, `experiments sync-remote`,
  `experiments poll-wandb`, and `experiments watchdog`. Durable protocol tasks
  are inspected and operated with `experiments task-list`,
  `experiments task-approve`, `experiments task-run`, and
  `experiments task-recover`. Local restart recovery is intentionally
  conservative: expired `leased` tasks go back to `ready`; stale `running`
  tasks go back to `ready` only if no remote launch/monitor event mentions the
  protocol task id. If the task already touched remote execution, recovery
  marks it `blocked` for an explicit control decision before retry.
- Watchdog checks for `ready` protocol tasks before stale-supervisor fallback.
  A complete eval/preflight contract can therefore be woken up and executed
  even when no current remote PID, log path, or W&B target exists.
- Use `experiments watch --project NAME --name EXPERIMENT --interval 10` or
  the project page in the local web dashboard to monitor control/review
  progression without returning to the controlling chat session.
- For self-iterating experiments, do not rely on a single long-lived supervisor
  process. Run `experiments watchdog` as a separate local background process
  with a stale threshold; it writes `watchdog_heartbeat`,
  `watchdog_skipped`, `watchdog_supervise_started`, and
  `watchdog_supervise_completed` events, and starts a one-cycle supervisor when
  recent supervisor/control/Codex activity is stale. This is the concrete
  evidence path for "background auto-continue" behavior.
- Experiments can also be watched by `mycodex daemon` directly when the
  experiment `config_json` contains `{"watchdog": {"enabled": true, ...}}`.
  The daemon writes `experiment_watchdog_daemon_started`,
  `experiment_watchdog_daemon_completed`, or
  `experiment_watchdog_daemon_failed` before/after calling the same
  `run_experiment_watchdog` control path. This is the preferred durable
  ownership path when the user expects mycodex itself to keep checking without
  a separate hand-started watchdog shell. To avoid duplicate control turns, the
  daemon also treats recent `watchdog_heartbeat`,
  `watchdog_supervise_started`, and `remote_anomaly_control_started` rows from
  an external LaunchAgent watchdog as satisfying the configured interval.
  Experiment watchdogs run before daily summaries and scheduled rules, so a
  slow daily-summary Codex turn cannot delay experiment continuation.
  Before starting a new cycle, the daemon also checks for active
  `experiment:<id>:` runs. A recent active control/review/repair run causes an
  `experiment_watchdog_daemon_skipped_active_control` event to avoid duplicate
  Codex turns; a run older than `control_timeout + 60s` is marked failed with
  `experiment_control_run_marked_stale`, then the watchdog retries.
  The same active-run lease is also enforced inside `experiments watchdog`
  itself via `experiment_watchdog_skipped_active_control`, so LaunchAgent,
  daemon, and manual watchdog invocations share the duplicate-control guard.
  macOS reboot protection is provided by `mycodex daemon
  --install-launch-agent`, which writes a per-user LaunchAgent with
  `RunAtLoad` and `KeepAlive` and starts the same daemon command after login.
  Each daemon pass runs stale protocol-task recovery before watchdogs, daily
  summaries, or scheduled rules.
  Use `watchdog.monitor_only=true` when the daemon should only recover stale
  local leases and record heartbeat/anomaly evidence; monitor-only passes do
  not execute ready protocol tasks, run supervisor cycles, or start control
  turns.
- When an experiment has a configured remote controller root, watchdog first
  runs `sync-remote` semantics against the remote machine and imports
  `state.json`, `events.jsonl`, pidfiles, summary rows, contract/env hashes,
  command hashes, GPU state, host boot id/uptime, and short log tails.
  Successful sync writes
  `remote_heartbeat` and deduped `remote_event` rows; SSH/Python/network
  failures write `remote_sync_failed` and must not trigger control by
  themselves. If the configured root is empty or stale but its base directory
  contains newer controller `state.json`/`events.jsonl` files, remote sync
  discovers the newest valid root instead of repeatedly reporting a false empty
  controller. New remote anomalies are fingerprinted persistently in SQLite.
  A changed remote boot id, or a fresh boot with no live controller processes
  and incomplete structured work, is a control anomaly; terminal-complete
  snapshots only observe the boot change.
  Terminal-complete anomaly fingerprints are based on stable run/eval/artifact
  identity and ignore drifting log-tail hashes; log-tail hashes only affect
  the fingerprint for actual log-failure anomalies.
- `remote_heartbeat` is remote-observation evidence, not supervisor freshness.
  It must not by itself prevent W&B polling or a stale one-cycle supervisor
  check. If the remote snapshot shows active work and no W&B target is known,
  the watchdog may skip with reason `remote_active`; otherwise stale
  supervisor/control activity should still be eligible for a supervisor pass.
- Remote anomaly continuation is control-plane driven, not a direct remote
  kill/restart. A new anomaly fingerprint writes `remote_anomaly`, starts the
  bound control session, repairs unparseable contracts if needed, runs the peer
  review gate, and only then executes a complete approved contract.
- Monitor-only contracts are not successful control cycles unless the remote
  command actually succeeds. A failed `remote_monitor_command_failed` must also
  write `remote_anomaly_control_failed`, so duplicate-control guards do not
  treat the anomaly as handled. When `remote_python` is configured, contract
  remote commands are prefixed with `MYCODEX_REMOTE_PYTHON` and the Python
  environment's `bin` directory on `PATH`, so bare `python`/`python3` probes use
  the intended remote env.
- Local result gates such as `result_gate_parsed` can be continuation
  conditions. If the latest gate says training is blocked, manual review is
  required, or manifest repair is needed, watchdog starts a new control cycle
  even when the remote terminal anomaly itself is a duplicate.
- A complete terminal remote snapshot that is merely ready for the next
  decision is an info-level continuation condition, not an error-level crash.
  Treat `remote_terminal_complete_ready_for_next` as result-analysis/next-run
  input unless artifacts are actually missing or logs show failure.
- Remote experiment launch records PID/log/commit in `remote_job_started`.
  W&B polling records compact `metric` events plus `decision`, `failed`, or
  `wandb_run_finished` events when conditions are reached.
- Remote summary expected-row defaults should come from experiment config or
  remote contract fields (`eval_epochs`, `eval_envs`, `EVAL_EPOCHS`,
  `EVAL_ENVS`, `HELDOUT_ENVS`). Do not hard-code Procgen environment names in
  the generic remote sync path.
- Experiment control loops should log the peer-review prompt, reviewer
  conclusion, final execution contract, and reason for any deviation as
  experiment events so later sessions can reconstruct why a run was launched.
- Experiment control contract parsing is a recoverable control-plane failure,
  not a reason to strand an experiment. If the bound Codex session returns an
  unparseable execution contract, the supervisor records
  `control_contract_parse_failed`, starts a focused `contract_repair` turn, and
  retries the same terminal condition on the next supervisor cycle if repair
  still fails.
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
- Research threads must now pass a concept preflight before normal search,
  synthesis, or idea ranking: define mechanism axes, query families, synonym
  expansions, anchor promotion criteria, and exact/partial/adjacent/irrelevant
  threat rules. This was added after the `LLM+in-context RL` thread showed that
  literal keyword search could over-promote SKILL0 while under-promoting LaMer
  and MAGE.
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
