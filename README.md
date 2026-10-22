# mycodex

`mycodex` is a local Codex project/session manager. It keeps a small SQLite index of projects, Codex sessions, scheduled rules, and run results, then drives Codex through the local `codex app-server` JSON-RPC interface.

The first version intentionally does not write Codex's internal automation tables. It keeps its own state in `data/my_codex.sqlite` and treats Codex sessions as an external runtime.

## Requirements

- Python 3.11+
- Codex CLI installed and logged in
- Windows, macOS, or Linux for the local manager; remote experiment runners use Linux/POSIX

Check:

```bash
codex --version
python3 --version
```

## Install For Local Development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

You can also run without installing:

```bash
python3 -m mycodex --help
```

## Follow-up iteration (0.3)

All ordinary CLI sends and summaries now save a durable job before execution.
Without `--enqueue` the CLI consumes that saved job immediately; if another task
owns its resource it stays queued. `--enqueue` saves only. `--no-sync` is retained
as a compatibility flag: queued jobs use the session index frozen at submission.

### Pause, edit, and resume

```text
mycodex jobs pause 12
mycodex jobs show 12
mycodex jobs revise 12 --revision 1 --prompt "Inspect the latest source passages before proposing an experiment."
mycodex jobs resume 12
```

The `/jobs` page exposes these controls and the goal revision. A running Codex turn
must confirm its terminal interruption before it becomes resumable. An interrupt
acknowledgement alone leaves ambiguous work blocked for reconciliation. A queued
task can pause immediately. Resume preserves its conversation, permissions and
remaining observed-token/time budget; unknown prior token usage prevents resuming
a token-limited task. An edit requires the current revision, so stale browser tabs
cannot overwrite newer goals. Review inputs remain immutable. Active remote
experiment operations cannot be paused by this local control.

### Selective review and actual cost

```text
mycodex reviews --project NAME --risk evidence-conflict --token-budget 12000 --prompt "Check these passages and proposed conclusions."
mycodex reviews --project NAME --policy auto --prompt "Summarize the saved state."
mycodex review-report 1
mycodex review-report 1 --assessment-file assessment.json
```

Explicit risk flags select evidence, implementation or experiment reviewers; a
routine task with no declared risk skips additional model calls. Explicit `--role`
still works. Reports contain routing reasons, actual reported tokens, elapsed time
and owner-verified findings. An assessment JSON contains `verifier`, `notes`, and
`findings`; each finding needs `review_job_id`, `summary`, `disposition`
(`accepted`, `rejected`, or `unresolved`) and inspectable `evidence`. Assessments
are write-once. Agreement is not verification, and no paired baseline means net
token savings and avoided losses remain unknown.

### Sourced operational and material benchmarks

```text
mycodex benchmarks --output data/benchmark-new
mycodex benchmarks --output data/benchmark-three --ref ed16415 --ref a4eb8d0 --ref working-tree
mycodex benchmarks --output data/benchmark-live --live-material-pair
```

The default performs zero model calls. It archives each selected Git version into
an isolated import environment and runs the same sourced operational cases.
Missing capabilities/refs are reported separately from passes. Case provenance,
source tree hashes, timings and available metrics accompany the report. The local
`a4eb8d0` reference is optional; a fresh clone may not contain it.

`--live-material-pair` explicitly runs two real read-only tasks with identical
questions and a public-domain SQLite documentation gold set. Each uses an observed
30000-token budget and a 180-second timeout; input or in-flight tokens can overshoot
an event-based limit. Probe threads are archived. This small recency-stress task
measures source retrieval and visible citation support, not general research
quality or a statistically established model improvement. Full provenance and
scoring limits are in [benchmarks/README.md](benchmarks/README.md).

### Structured PDFs and optional runtime transport

Install optional capabilities into the project environment:

```text
python -m pip install -e ".[dev,documents,sdk]" -c requirements-dev.lock
```

PDF ingestion can use `pdfplumber` in a timeout-bounded child process. It preserves
physical page numbers, character offsets and bounding boxes for actual extracted
passages, headings, figure/table captions, detected table regions and explicitly
numbered formula candidates. Missing glyphs and incomplete extraction remain
visible. Formula recognition does not recover LaTeX or validate mathematics, and
table detection does not verify cell values. `pdftotext` remains a fallback.
GROBID is an explicit optional service adapter; it sends a PDF only when its URL
is supplied, and an unavailable service is recorded as unavailable.

The `sdk` extra pins the tested official Python SDK. Set
`MYCODEX_RUNTIME_BACKEND=sdk` to use its transport through the same lifecycle,
contract, usage and interruption checks. The default remains `app-server`. The
adapter explicitly declines approval requests, applies RPC deadlines, and refuses
untested SDK versions rather than silently falling back.

### Trace export and integration checks

```text
mycodex traces --job-id 12 --output data/job-12-otlp.json
mycodex traces --job-id 12 --output data/job-12-otlp.json --endpoint http://127.0.0.1:4318
python -m mycodex.integration_checks --report-dir data/integration-check
python -m mycodex.integration_checks --report-dir data/integration-sdk --sdk --sdk-probe
```

Traces correlate job events and Codex attempts using OTLP/HTTP JSON. Exported
attributes omit prompts, outputs, source text, file paths and credentials. A
collector POST happens only with an explicit endpoint. Its acknowledgement is
transport evidence, not proof of backend persistence. Wire format follows the
[OTLP specification](https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding).

Integration reports list every pass/failure/skip. Native Linux tests inject lost
observers, concurrent retries, supervisor death and restart attempts inside
temporary directories. Receipts require matching workload and supervisor
identities; an orphan workload is `needs_reconciliation`, never silently restarted.
The SDK probe creates and archives an empty real-runtime thread with no inference.
Live SSH, W&B and Telegram delivery still require their configured host/accounts.
Experiment policy and protocol/recovery code now live in separate modules while
the original imports remain compatible.

## Reliability and research iteration (0.2 foundation)

This iteration keeps SQLite as the control plane and adds a persistent job queue,
versioned research evidence, bounded remote execution, and recorded-run checks.
The CLI command names remain available; version 0.3 makes durable execution the default.
The web and Telegram prompt entrances save jobs before execution. The daemon queues
scheduled prompts/summaries and owns a separate local queue consumer.

Set up an isolated environment (on Windows use `.venv\Scripts\python.exe` and
`.venv\Scripts\mycodex.exe`; on macOS/Linux use `.venv/bin/python`):

```text
python -m venv .venv
.venv\Scripts\python.exe -m pip install -e ".[dev]" -c requirements-dev.lock
.venv\Scripts\python.exe -m mycodex doctor
.venv\Scripts\python.exe -m unittest discover -v
```

On macOS/Linux substitute `.venv/bin/python` for the Windows environment path. The lock file
pins the tested development dependencies; W&B remains an optional install.

### Durable jobs and bounded reviews

```text
mycodex send --project NAME --enqueue --max-total-tokens 8000 "Read the current evidence and propose the next diagnostic."
mycodex worker --once
mycodex worker
mycodex jobs list --project NAME
mycodex jobs show 12
mycodex jobs cancel 12
mycodex jobs recover
mycodex reviews --project NAME --role evidence --role experiment --token-budget 12000 --prompt "Review this plan and its supplied evidence."
```

Jobs freeze thread and permission defaults on submission. Atomic leases prevent
two consumers claiming the same task; dependencies prevent synthesis before its
reviews finish. Independent read-only review threads can run concurrently using
multiple worker processes. Existing shared threads and writable project tasks
are serialized. Review outputs are proposals and cannot themselves authorize execution.

Expired **unstarted** leases return to the queue. Expired **running** leases become
blocked because work may already have happened; inspect run/remote evidence before
submitting a replacement. Cancellation interrupts a live queued Codex turn through
the owning worker. It does not kill unrelated processes or already launched remote
training. Token limits are checked on reported usage and can overshoot by an in-flight
model call; timeouts bound the local turn. No currency/billing estimate is invented.

### Evidence and research coverage

```text
mycodex evidence index --project NAME --name RESEARCH --source-id 1 --file paper.pdf
mycodex evidence search --project NAME --name RESEARCH --query "policy adaptation"
mycodex evidence versions --project NAME --name RESEARCH --source-id 1
mycodex evidence link --project NAME --name RESEARCH --claim-id 2 --passage-id 3 --relation supports --verification-status verified --verifier human-review --notes "The passage states the measured condition and its limitation."
```

Search records per-provider coverage, cache hashes and failures. Empty results
are distinct from unavailable providers. `OPENALEX_API_KEY` and
`SEMANTIC_SCHOLAR_API_KEY` are environment variables, never stored credentials.
Source versions and passage text are immutable; locators are only as precise as
the extractor supports. Missing page/section values remain unknown. Retrieval is
lexical, including Chinese bigrams; no embedding service is required.

Local templates produce uncertain hypotheses. An explicit evidence link with a
passage, verifier and justification is required for autonomous claim promotion;
this is a traceable verification record, not proof that a scientific claim is true.
Empty PDF extraction cannot improve reading depth. Manual legacy labels remain
visible as `legacy_unverified`. Source text and reviewer text are untrusted data.

### Remote execution identity and artifacts

```text
mycodex experiments launch-remote --project NAME --name EXPERIMENT --execution-id diagnostic-001 --time-budget-seconds 600 --command "python scripts/diagnostic.py"
mycodex artifacts list --project NAME --name EXPERIMENT
mycodex artifacts result --project NAME --name EXPERIMENT --execution-id diagnostic-001 --result-file result.json
```

Remote launches use an atomic claim and durable receipt under a stable execution
identity. A retry with that identity observes the existing job; an intentional rerun
uses a new identity. Numeric time budgets are enforced by the remote supervisor
with termination/grace/kill behavior. Legacy prose budgets are explicitly marked
unbounded; schema-v1 execution contracts require an enforceable numeric duration.
The experiment `config_json.execution_policy` may restrict remote targets, command
hashes, local-check hashes, patch roots, execution classes, destructive operations
and runtime budgets. This policy is not an operating-system sandbox. Local checks
have a default 300-second per-check limit, capped by policy when configured.

Local manifests live beside the selected DB under `experiment_artifacts/` and
record the resolved code commit, command/config hashes, budget and supplied research
metadata. Result gates distinguish triage from method-claim evidence and validate
recorded receipts plus referenced local artifact hashes. Unknown seed/dataset fields
remain unknown. Native remote supervisor integration tests run on Linux; a Windows
local manager requires a compatible remote Linux environment.

### MCP, task decisions, and evaluation

```text
mycodex web
mycodex mcp
mycodex mcp --allow-writes
mycodex evals --project NAME
mycodex evals --input exported-runs.json
```

`/jobs` explains active work, blockers, limits and observed consumption, and supports
saved-task cancellation. Research evidence/coverage is accessible from this view.
The STDIO MCP server exposes project state, jobs, source passages and experiment
manifests. It is read-only by default. `--allow-writes` adds bounded job submission
and cancellation; MCP submissions cannot enlarge project permissions. Start the
server with the absolute environment Python, `-m mycodex --db ABSOLUTE_DB mcp`,
in your MCP client's configuration. No client settings are changed by installation.

`evals` runs deterministic consistency/evidence/budget checks over recorded or exported
runs; it exits unsuccessfully when checks fail. It is not a live scientific-quality
benchmark. Runtime traces preserve model events, native usage and job correlation.
Native JSON output schemas use the supported strict subset and normalize into
versioned control contracts, then undergo separate type and policy checks. The
native app-server transport remains the default; version 0.3 adds an optional SDK
transport and explicit OTLP trace export. Embedding retrieval is not implemented.

Executable selection is: explicit client argument, `MYCODEX_CODEX_BIN`, an existing
host-provided `CODEX_CLI_PATH`, then PATH. This avoids accidentally choosing an old
global CLI that cannot parse a newer desktop configuration. It never rewrites the
global service tier or other user settings. `doctor` shows the selected executable.

Development validation includes a local handshake and one real read-only native-v1
structured-output turn using the host runtime; the temporary probe thread was
archived afterward. Live SSH/W&B/Telegram and Linux supervisor execution require a
configured remote host/account and remain separate integration checks. The shared
remote checkout refuses a distinct active or ambiguous managed execution before
fetch/checkout; unmanaged legacy jobs and deliberately detached child groups still
require an external controller or manual reconciliation.
Experiment protocol tasks renew their own leases and fence start/completion by a
unique acquisition token. Repeated contract recording cannot replace an active
lease. Only a failed read-only monitor command can derive a new retry task, up to
three total attempts with persisted lineage; prior terminal tasks remain unchanged.

## Core command reference

Add a project:

```bash
mycodex projects add my-codex /Users/bluixe/Code/my-codex \
  --enable-daily-summary \
  --daily-summary-time 18:00
```

Discover Codex projects from local session history, then connect the ones you want:

```bash
mycodex projects discover
mycodex projects connect my-arxiv-daily /Users/bluixe/Code/my-arxiv-daily
```

List, inspect, and remove mycodex-connected projects:

```bash
mycodex projects list
mycodex projects status my-codex
mycodex projects remove my-codex --yes
```

Sync Codex sessions for a project:

```bash
mycodex sessions sync --project my-codex
mycodex sessions list --project my-codex
mycodex sessions set-default --project my-codex --thread-id THREAD_ID
```

Send a prompt to the default project thread:

```bash
mycodex send --project my-codex "请总结今天进度并列出下一步"
```

Run a read-only daily summary prompt:

```bash
mycodex daily-summary --project my-codex
```

Run the local scheduler:

```bash
mycodex daemon
```

For a single scheduler pass, useful from launchd/cron:

```bash
mycodex daemon --once
```

Install the daemon as a per-user macOS LaunchAgent so it starts again after a
Mac reboot or user login:

```bash
mycodex daemon --install-launch-agent --interval 300 --timeout 1800
mycodex daemon --launch-agent-status
mycodex daemon --uninstall-launch-agent
```

The LaunchAgent uses `RunAtLoad` and `KeepAlive`, writes logs under
`~/Library/Logs/mycodex`, and invokes the same `mycodex daemon` command with
the selected SQLite DB path.

The daemon logs due/completed/failed scheduled work to stdout/stderr. If a
project has Telegram bindings and `MYCODEX_TELEGRAM_BOT_TOKEN` is set in the
daemon environment, or a local token file exists at
`data/secrets/telegram_bot_token`, completed daily summaries are also pushed to
the bound Telegram chat or topic. Disable that with:

```bash
mycodex daemon --no-telegram-notify
```

Run the local web dashboard:

```bash
mycodex web --host 127.0.0.1 --port 8765
```

The dashboard shows connected projects, git/session/run/rule status, project todos, the latest daily summary for each project, and quick actions for syncing sessions, sending prompts, and running daily summaries. It only listens on localhost by default.

The dashboard also includes `/usage`, a read-only local token usage view. It reads only Codex's local `~/.codex/logs_2.sqlite` log database, parses completed response usage events, and aggregates tokens by day, model, and connected mycodex project. This is useful for local real-time monitoring, but final billing and plan limits are still authoritative on the OpenAI/ChatGPT backend. Override the log path for testing with:

```bash
export MYCODEX_CODEX_LOGS_DB=/path/to/logs_2.sqlite
```

Run the Telegram adapter:

```bash
export MYCODEX_TELEGRAM_BOT_TOKEN="123456:bot-token"
mycodex telegram allow TELEGRAM_USER_ID
mycodex telegram run
```

The Telegram adapter uses long polling and stores only users/bindings in SQLite. The bot token is read from `--token`, `--token-file`, the environment, or the local ignored token file; it is not stored in the database.
If Telegram needs your local proxy, run with `--proxy-url http://127.0.0.1:7890` or set standard proxy environment variables before starting the bot.
You can also store the token in a local ignored file and have both `telegram run`
and `daemon` read it automatically:

```bash
mkdir -p data/secrets
chmod 700 data/secrets
printf '%s\n' "$MYCODEX_TELEGRAM_BOT_TOKEN" > data/secrets/telegram_bot_token
chmod 600 data/secrets/telegram_bot_token
```

For a non-default path, use `mycodex telegram run --token-file PATH` or
`mycodex daemon --telegram-token-file PATH`.

Bind a Telegram DM, group, or forum topic to a mycodex project and Codex thread:

```text
/projects
/bind my-codex
/bind_thread my-codex THREAD_ID
/send 请总结当前项目状态
```

Once bound, plain text messages are sent to the bound Codex thread. The same thread can be opened locally:

```bash
codex resume THREAD_ID -C /path/to/project
```

Wait for the final `Run #... completed` or `Run failed` reply before sending the next prompt. The bot serializes work per Telegram chat/topic and project so one Codex thread is not driven by several concurrent turns.

Useful Telegram commands:

```text
/binding
/status
/sessions
/sync
/runs
/summary
/new 请开始一个新的 session
/todo list
/todo add Run the smoke test
/todo done 12
```

Local binding management is also available:

```bash
mycodex telegram users
mycodex telegram bindings
mycodex telegram bind CHAT_ID PROJECT --message-thread-id TOPIC_ID --thread-id THREAD_ID
mycodex telegram unbind CHAT_ID --message-thread-id TOPIC_ID
```

## Safety Model

Projects default to:

- `sandbox=read-only`
- `approval_policy=never`

This is deliberate: summaries and monitoring should not mutate repos by default. If you want a project to run implementation tasks, add or update the project with:

```bash
mycodex projects add NAME PATH --sandbox workspace-write --approval-policy never
```

You can also override per send:

```bash
mycodex send --project NAME --sandbox workspace-write "实现下一步"
```

## Rules

Project-level daily summaries are configured on `projects add` with `--enable-daily-summary`.

Custom local rules can be added:

```bash
mycodex rules add \
  --project my-codex \
  --name bug-scan \
  --kind bug_scan \
  --time-of-day 09:30 \
  --prompt "请只读扫描这个项目中最可能的 bug，并给出优先级。"
```

Inspect rules and run history:

```bash
mycodex rules list
mycodex runs list --project my-codex
```

## Experiments

`mycodex` can track a self-iterating experiment that is controlled by a chosen
Codex session while execution happens elsewhere. The framework stores the
experiment record, status, current iteration, and a structured event stream in
the same SQLite database.

Create an experiment bound to a project session:

```bash
mycodex experiments create \
  --project icrl-llm \
  --name self-iterate \
  --thread-id 019dc49b-6572-7692-846f-06c4cb8168c6 \
  --objective "Iteratively improve an icrl-llm experiment." \
  --max-iterations 10
```

Record progress from a local script, remote job wrapper, or manual checkpoint:

```bash
mycodex experiments event \
  --project icrl-llm \
  --name self-iterate \
  --type metric \
  --iteration 1 \
  --message "First smoke run completed" \
  --payload-json '{"success_rate": 0.42}' \
  --status running
```

Inspect state:

```bash
mycodex experiments list --project icrl-llm
mycodex experiments status --project icrl-llm --name self-iterate
mycodex experiments watch --project icrl-llm --name self-iterate --interval 10 --limit 25
```

`watch` refreshes the experiment phase and recent high-level event stream,
including control-turn, peer-review, remote-job, and W&B polling events. Add
`--include-deltas` when you also want streamed agent-message chunks. The local
web dashboard shows the recent event stream on the project page, and the same
data is available from `/api/projects/{project}/experiments/{name}`.

Start a Codex control turn on the experiment thread:

```bash
mycodex experiments start --project icrl-llm --name self-iterate
```

While an experiment control turn is running, `mycodex` records streaming Codex
agent-message chunks and selected tool/turn events into `experiment_events`.
That lets the dashboard/API show partial progress instead of waiting for the
whole turn to complete.

The control session should not write SQLite directly. It should produce the
patch, command, W&B run identity, metrics, and stop conditions; the outer
`mycodex` process records events.

For autonomous self-iteration, the control session should also emit an
execution contract with `auto_execute`, `execution_class`, patch scope, remote
command, budget, W&B identity, metrics, and hard stop conditions. Low-risk
read-only preflight, diagnostic patches, monitor-only steps, and tightly
budgeted smoke training can proceed without another user confirmation when that
contract is complete.

Control-turn contracts are also persisted as durable protocol tasks. This makes
the loop explicit: the bound Codex session proposes a contract, `mycodex`
records it as `proposed`, `ready`, or `blocked`, and a protocol worker leases
and executes only `ready` tasks. Inspect and operate that queue with:

```bash
mycodex experiments task-list --project icrl-llm --name self-iterate
mycodex experiments task-approve --project icrl-llm --name self-iterate --task-id 12
mycodex experiments task-run --project icrl-llm --name self-iterate --max-tasks 1
mycodex experiments task-recover --project icrl-llm --name self-iterate
```

`task-approve` moves a `proposed` or `blocked` task to `ready`.
`task-run` leases ready tasks, executes the stored contract, and writes
`experiment_task_lease_acquired`, `experiment_task_succeeded`,
`experiment_task_failed`, or `experiment_task_blocked` events. The watchdog also
checks for ready protocol tasks before falling back to stale-supervisor logic,
so a complete eval/preflight contract does not depend on a remote PID or W&B
target merely to be woken up.
`task-recover` is the local-restart repair path: expired `leased` tasks return
to `ready`; stale `running` tasks return to `ready` only if no remote launch
event mentions the protocol task id. If a stale `running` task already touched
the remote side, it is marked `blocked` so the control session can inspect
before any retry.

Commit and push local experiment changes:

```bash
mycodex experiments git-push \
  --project icrl-llm \
  --name self-iterate \
  --message "Add next Sokoban iteration" \
  --all
```

Sync the remote repository to the chosen commit and start a background job:

```bash
mycodex experiments launch-remote \
  --project icrl-llm \
  --name self-iterate \
  --ssh-key ~/.ssh/KeyPair-wenhao.pem \
  --command "bash scripts/run_next_experiment.sh" \
  --wandb-entity ENTITY \
  --wandb-project PROJECT \
  --wandb-run-id RUN_ID
```

For password-only machines, keep the password in an ignored local file and pass
`--ssh-password-file path/to/secret` instead of `--ssh-key`. Password mode uses
the local `expect` command and records only `ssh_auth=password_file`, never the
secret value.

`launch-remote` writes `remote_pull_started`, `remote_pull_completed`, and
`remote_job_started` events. The job-started payload includes the remote PID,
log path, commit, command, and optional W&B run fields.

Sync structured remote controller state into local events:

```bash
mycodex experiments sync-remote \
  --project icrl-llm \
  --name self-iterate \
  --remote-ref root@example.com:/remote/repo \
  --remote-state-root /root/autodl-tmp/icrl_runs/procgen-multi-env-icl/controller
```

`sync-remote` imports remote `state.json`, `events.jsonl`, pidfiles, summary
rows, contract/env hashes, command hashes, GPU state, and short log tails. It
writes `remote_heartbeat`, deduped `remote_event`, and `remote_anomaly` events.
SSH/Python sync failures are recorded as `remote_sync_failed` and do not count
as remote experiment failures.

Run a background watchdog that first syncs remote state and then continues the
control loop when the remote snapshot itself shows a real anomaly:

```bash
mycodex experiments watchdog \
  --project icrl-llm \
  --name self-iterate \
  --remote-state-root /root/autodl-tmp/icrl_runs/procgen-multi-env-icl/controller \
  --interval 300 \
  --max-cycles 288
```

For each successful sync, the watchdog uses persistent anomaly fingerprints to
avoid opening duplicate control turns. When a new control anomaly appears, it
starts the bound control session, runs the peer-review gate, and only executes a
complete approved contract. The watchdog does not directly kill or restart
remote jobs based only on a local stale timer.

To let `mycodex daemon` own this lifecycle, put the watchdog options in the
experiment config and keep the normal daemon running:

```json
{
  "watchdog": {
    "enabled": true,
    "interval_seconds": 300,
    "remote_base": "/root/autodl-tmp/icrl_runs/procgen-multi-env-icl",
    "remote_python": "/root/miniconda3/envs/dpt/bin/python3",
    "remote_sync": true,
    "remote_anomaly_control": true,
    "stale_seconds": 900,
    "monitor_only": false,
    "trigger_next_control": true,
    "auto_execute": true,
    "allow_full_train": true,
    "peer_review_bypass_timeout_seconds": 1800,
    "peer_review_bypass_on": "revise,timeout"
  },
  "remote_state": {
    "base": "/root/autodl-tmp/icrl_runs/procgen-multi-env-icl",
    "eval_epochs": ["1", "2", "final"],
    "eval_envs": ["bossfight", "caveflyer"]
  }
}
```

Daemon-owned cycles write `experiment_watchdog_daemon_started`,
`experiment_watchdog_daemon_completed`, or `experiment_watchdog_daemon_failed`
events around the same watchdog state machine. `remote_heartbeat` remains
remote-observation evidence; it is not counted as local supervisor freshness.
If a LaunchAgent or another wrapper has just written `watchdog_heartbeat`, the
daemon respects that freshness window and does not open a duplicate watchdog
cycle. If an `experiment:<id>:` Codex run is still active, the daemon skips
with `experiment_watchdog_daemon_skipped_active_control`; `experiments
watchdog` itself also skips with `experiment_watchdog_skipped_active_control`.
If that run exceeds `control_timeout + 60s`, it is marked failed with
`experiment_control_run_marked_stale` and the watchdog retries. The daemon runs
experiment watchdog checks before daily-summary and scheduled-rule work. Prefer
`remote_base`/`remote_state.base` for experiment families with one controller
directory per run; if an explicit controller root is stale or empty, remote
sync falls back to the newest valid `state.json`/`events.jsonl` under the base
directory. Remote heartbeats include host boot id and uptime when available; a
boot-id change, or a very recent remote boot with no live controller processes
and incomplete work, is treated as a control anomaly. Set
`watchdog.monitor_only=true` when the daemon should only recover stale local
leases and record remote heartbeat/anomaly evidence; in that mode it does not
execute ready protocol tasks, run supervisor cycles, or start control turns.
Terminal-complete anomaly fingerprints ignore drifting log-tail hashes, so a
completed run does not repeatedly spawn control turns just because logs were
appended after completion.

Poll W&B once or repeatedly and write metric/decision events:

```bash
mycodex experiments poll-wandb \
  --project icrl-llm \
  --name self-iterate \
  --run-path ENTITY/PROJECT/RUN_ID \
  --metric val/success_rate \
  --metric phase_a/actor/grad_norm \
  --stop-condition "val/success_rate>=0.5" \
  --interval 300 \
  --max-polls 12
```

Use `--trigger-next-control` when a finished/failed run or stop condition should
immediately start another control turn on the bound Codex session.
The local Python environment running `mycodex` needs W&B installed for polling;
use `pip install -e '.[wandb]'` if it is missing.

## Research Threads

`mycodex` can also track long-lived, self-iterating research work. This is the
research counterpart to `experiments`: a bound Codex session does the analysis,
but the outer `mycodex` control plane stores state in SQLite.

The research control plane stores:

- `research_threads`: top-level question, phase, status, iteration, and bound
  Codex thread
- `research_sources`: source ledger for papers, code, benchmarks, docs, and
  rejected/kept sources
- `research_claims`: claim ledger with support status and confidence
- `research_ideas`: idea ledger with hypotheses, mechanisms, minimal tests, and
  kill criteria
- `research_tasks`: queued next actions such as search, triage, deep read,
  synthesis, ranking, and experiment design
- `research_events`: dashboard/event stream for partial Codex output and
  control decisions

Create a research thread bound to an existing project session:

```bash
mycodex research create \
  --project icrl-llm \
  --name reasoning-directions \
  --thread-id THREAD_ID \
  --question "Find and refine research directions around LLM reasoning." \
  --max-iterations 10
```

Start a research control turn:

```bash
mycodex research start --project icrl-llm --name reasoning-directions
```

Before sending the turn, `mycodex` injects a compact JSON snapshot of the
current source, claim, idea, task, and event ledgers. The bound Codex session is
therefore the LLM decision layer: it reviews coverage, grounding, nearest
threats, novelty risk, feasibility, kill criteria, and next information gain,
then outputs a compact research packet plus a JSON research contract. `mycodex`
parses the contract and updates source, claim, idea, and task ledgers.
Low-risk research actions such as more public-source search, top-source deep
reading, nearest-threat search, claim verification, synthesis, and idea
critique can be queued automatically. High-cost steps such as implementation,
experiments, paid/private sources, or committing to a direction should remain
user-gated.

Run queued tasks without manually prompting the bound session every time:

```bash
# Run the next queued task. Search tasks use Zotero plus public connectors by
# default: Zotero, arXiv, OpenAlex, and Semantic Scholar. Set
# SEMANTIC_SCHOLAR_API_KEY for a higher Semantic Scholar rate limit if needed.
mycodex research run-task --project icrl-llm --name reasoning-directions

# Keep consuming queued tasks. When no queued task remains, this triggers the
# bound Codex control session to synthesize the current ledgers and produce the
# next research contract. Use --no-trigger-control for a pure local worker.
mycodex research supervise \
  --project icrl-llm \
  --name reasoning-directions \
  --max-cycles 5 \
  --interval 300
```

Task types supported by the local worker:

- `concept_preflight`: create the mechanism taxonomy, synonym/query families,
  anchor promotion criteria, and exact/partial/adjacent/irrelevant threat rules
  that must exist before normal search, synthesis, or idea ranking
- `search`, `source_search`, `literature_search`, `triage`: search Zotero and
  public sources, then write candidates to `research_sources`
- `deep_read`, `read`, `paper_read`, `ingest`: ingest a URL or local file into
  source notes; PDF extraction uses local `pdftotext` when available
- `deep_read_top`, `read_top`, `paper_read_top`: select top retained sources
  and ingest their URL, open PDF, or Zotero local attachment into source notes
- `threat_search`, `nearest_threat_search`: search close related work for one
  or more idea ids, then attach threat source ids to the idea metadata
- `idea_critique`, `critique_ideas`: score ideas against evidence, nearest
  threats, feasibility, falsifiability, and information gain; write critique
  notes and revised minimal tests back to the idea ledger
- `synthesize`, `rank_ideas`, `idea_tournament`, `design_experiment`: delegate
  to the bound Codex control session, then parse its contract
- `ask_user`: mark the task blocked and surface the decision on the dashboard

The Zotero connector is local and read-only. It copies
`~/Zotero/zotero.sqlite` at task runtime, queries the copy, and records matching
items as `zotero://select/library/items/...` source entries. If a matching item
has a local PDF attachment under Zotero storage, a later `deep_read` task can
ingest that attachment. Override the data directory with `ZOTERO_DATA_DIR`.

A typical local auto-research pass is:

```bash
mycodex research task-add \
  --project icrl-llm \
  --name reasoning-directions \
  --task-type concept_preflight \
  --payload-json '{"auto_queue_search": true}'

mycodex research supervise \
  --project icrl-llm \
  --name reasoning-directions \
  --max-cycles 5 \
  --no-trigger-control
```

`research create` queues a `concept_preflight` task by default. For one-off
manual updates, you can still queue a search directly; the worker will record a
concept preflight first if one is missing. For example:

```bash
mycodex research task-add \
  --project icrl-llm \
  --name reasoning-directions \
  --task-type search \
  --query "LLM reasoning test-time compute verifier 2025" \
  --payload-json '{"auto_synthesize": true, "auto_deep_read": true, "auto_refine": true}'

mycodex research supervise \
  --project icrl-llm \
  --name reasoning-directions \
  --max-cycles 5 \
  --no-trigger-control
```

This runs search, local synthesis, top-source reading, nearest-threat search,
and idea critique in the outer `mycodex` process. The results are useful for
triage and iteration; final novelty claims should still be checked by a deeper
reading/control turn before committing to a research direction.

Manual ledger updates are also available:

```bash
mycodex research source-add \
  --project icrl-llm \
  --name reasoning-directions \
  --title "Tree of Thoughts" \
  --url https://example.test/tot \
  --role method \
  --status kept

mycodex research claim-add \
  --project icrl-llm \
  --name reasoning-directions \
  --claim "Deliberate search can improve some reasoning tasks." \
  --status supported \
  --support "Tree of Thoughts"

mycodex research idea-add \
  --project icrl-llm \
  --name reasoning-directions \
  --title "Uncertainty-gated deliberation" \
  --hypothesis "Adaptive test-time compute improves efficiency." \
  --minimal-test "Compare fixed vs gated reasoning budget."
```

Inspect state:

```bash
mycodex research list --project icrl-llm
mycodex research status --project icrl-llm --name reasoning-directions
```

Find an older research thread by topic without remembering its exact name:

```bash
mycodex research find \
  --project icrl-llm \
  --query "LLM reasoning verifier budget"
```

Run an incremental update against the best-matching old thread:

```bash
mycodex research update \
  --project icrl-llm \
  --query "LLM reasoning verifier budget 2026" \
  --max-cycles 5
```

`research update` still uses SQLite as the source of truth. It searches the
stored research question, sources, claims, ideas, tasks, and events; queues an
incremental `search` task on the best match; then runs the same supervisor loop
that can trigger the bound Codex control session. If you already know the exact
thread, use `--name reasoning-directions` to skip topic matching.

Every research mutation also writes an auxiliary Markdown snapshot under the
local DB directory:

```text
data/research_markdown/PROJECT/RESEARCH.md
```

This Markdown file is for reading and handoff only. Search, update, control
turns, and dashboard state continue to use SQLite ledgers.

## Todos

The web dashboard stores lightweight per-project todos in the same SQLite database. Todo states are:

```text
open, doing, blocked, done, archived
```

## Data

The default database is:

```text
data/my_codex.sqlite
```

Use another DB with:

```bash
mycodex --db /path/to/my_codex.sqlite projects list
```

## Notes

- `sessions sync` uses `codex app-server thread/list` and filters by exact project cwd.
- `send` resumes the stored default thread when possible. If none exists, it starts a new thread.
- The daemon uses local rules in `data/my_codex.sqlite`; it does not depend on Codex Desktop Automations.
