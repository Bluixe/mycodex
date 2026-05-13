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

Run the local web dashboard:

```bash
mycodex web --host 127.0.0.1 --port 8765
```

The dashboard shows connected projects, git/session/run/rule status, project todos, and quick actions for syncing sessions, sending prompts, and running daily summaries. It only listens on localhost by default.

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
