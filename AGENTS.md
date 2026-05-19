# AGENTS.md

## Project Purpose
- `mycodex` is a local Codex project/session manager.
- It tracks projects, Codex sessions, rules, runs, todos, and Telegram bindings in its own SQLite DB.
- Default local DB path: `data/my_codex.sqlite`.
- It drives Codex through `codex app-server --listen stdio://` JSON-RPC.
- It should not write Codex internal automation tables directly.

## Project Memory
- Read `docs/codex-memory.md` before non-trivial planning or implementation work.
- Keep durable decisions, recurring pitfalls, and project background in that file.
- Treat this `AGENTS.md` as the authoritative rule set; treat `docs/codex-memory.md` as maintained context.

## Key Files
- `mycodex/cli.py`: argparse CLI entrypoint for `python3 -m mycodex` and `mycodex`.
- `mycodex/db.py`: SQLite schema plus `Store` persistence methods.
- `mycodex/service.py`: project/session operations, sends, daily summaries, and git snapshots.
- `mycodex/experiment_ops.py`: experiment control-plane helpers for local git, remote launch/status, and W&B polling.
- `mycodex/codex_app.py`: Codex app-server JSON-RPC client.
- `mycodex/daemon.py`: local scheduled rules and daily-summary runner.
- `mycodex/web.py`: FastAPI/Jinja local dashboard.
- `mycodex/templates/` and `mycodex/static/`: dashboard UI assets.
- `mycodex/telegram.py`: Telegram long-polling adapter and shared-thread binding.
- `mycodex/status.py`: project status aggregation for CLI/web views.
- `mycodex/prompts.py`: reusable prompts.
- `tests/`: unittest coverage for store, Codex state discovery, and Telegram parsing.
- `README.md`: user-facing setup and command reference.

## Common Commands
- `python3 -m mycodex --help`
- `python3 -m mycodex projects list`
- `python3 -m mycodex projects discover`
- `python3 -m mycodex sessions sync --project NAME`
- `python3 -m mycodex sessions list --project NAME`
- `python3 -m mycodex send --project NAME "prompt"`
- `python3 -m mycodex daily-summary --project NAME`
- `python3 -m mycodex daemon --once`
- `python3 -m mycodex web --host 127.0.0.1 --port 8765`
- `python3 -m mycodex telegram run --proxy-url http://127.0.0.1:7890`
- Test: `python3 -m unittest discover -v`
- Syntax check: `python3 -m compileall -q mycodex tests`

## Development Notes
- Prefer the existing `Store` and service-layer APIs for new behavior.
- Keep Codex protocol details centralized in `mycodex/codex_app.py`.
- Web, daemon, Telegram, and CLI code should call service/store helpers instead of duplicating protocol logic.
- Monitoring and summaries should be read-only by default.
- For self-iterating experiments, keep SQLite writes in the outer `mycodex` control plane; the bound Codex session should only produce plans, patches, commands, metrics, and decisions.
- When a bound experiment session returns `auto_execute: true` with a complete low-risk preflight, diagnostic patch, monitor-only, or small smoke-training contract, the outer control plane may proceed without another user confirmation.
- Project defaults are intended to be conservative: `sandbox=read-only`, `approval_policy=never`.
- For implementation work, use explicit `workspace-write` project/send options.
- Do not commit local state: `data/`, `logs/`, `*.egg-info`, and `__pycache__/` are ignored.
- Never store Telegram bot tokens in code, SQLite, docs, or committed config; read them from environment variables.
- Telegram tasks should stay serialized per chat/topic/project to avoid concurrent turns and SQLite lock issues.
- Keep the dashboard localhost-first unless a future change explicitly adds authentication/exposure controls.
- If adding DB tables or columns, make schema initialization idempotent and update focused tests.

## Current Known State
- `main` has been pushed to `origin/main`.
- Recent known work includes the local web dashboard and Telegram shared-thread adapter.
- Local runtime files may exist under ignored `data/` and `logs/` for SQLite, pid files, and service logs.
