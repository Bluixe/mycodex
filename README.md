# mycodex

`mycodex` is a local Codex project/session manager. It keeps a small SQLite index of projects, Codex sessions, scheduled rules, and run results, then drives Codex through the local `codex app-server` JSON-RPC interface.

The first version intentionally does not write Codex's internal automation tables. It keeps its own state in `data/my_codex.sqlite` and treats Codex sessions as an external runtime.

## Requirements

- Python 3.11+
- Codex CLI installed and logged in
- macOS/Linux shell environment

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

## Core Commands

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
```

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
