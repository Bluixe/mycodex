from __future__ import annotations

import hashlib
import os
import plistlib
import subprocess
import sys
from pathlib import Path
from typing import Any


def default_daemon_launch_agent_label(db_path: Path | str) -> str:
    resolved = str(Path(db_path).expanduser().resolve())
    digest = hashlib.sha256(resolved.encode("utf-8")).hexdigest()[:10]
    return f"com.mycodex.daemon.{digest}"


def launch_agents_dir() -> Path:
    return Path.home() / "Library" / "LaunchAgents"


def launch_agent_path(label: str, *, agents_dir: Path | None = None) -> Path:
    label = label.strip()
    if not label:
        raise ValueError("launch agent label is required")
    return (agents_dir or launch_agents_dir()) / f"{label}.plist"


def daemon_launch_agent_plist(
    *,
    label: str,
    db_path: Path | str,
    cwd: Path | str,
    python: Path | str | None = None,
    interval: int = 300,
    timeout: int = 1800,
    allow_full_train: bool = False,
    notify_telegram: bool = True,
    telegram_token_file: str | None = None,
    telegram_token_env: str = "MYCODEX_TELEGRAM_BOT_TOKEN",
    telegram_proxy_url: str | None = None,
    log_dir: Path | str | None = None,
) -> dict[str, Any]:
    if interval < 1:
        raise ValueError("daemon interval must be positive")
    if timeout < 1:
        raise ValueError("daemon timeout must be positive")
    cwd_path = Path(cwd).expanduser().resolve()
    db_resolved = Path(db_path).expanduser().resolve()
    python_path = Path(python or sys.executable).expanduser().resolve()
    logs = Path(log_dir).expanduser().resolve() if log_dir else Path.home() / "Library" / "Logs" / "mycodex"
    program_args = [
        str(python_path),
        "-m",
        "mycodex",
        "--db",
        str(db_resolved),
        "daemon",
        "--interval",
        str(interval),
        "--timeout",
        str(timeout),
    ]
    if allow_full_train:
        program_args.append("--allow-full-train")
    if not notify_telegram:
        program_args.append("--no-telegram-notify")
    if telegram_token_file:
        program_args.extend(["--telegram-token-file", str(Path(telegram_token_file).expanduser().resolve())])
    if telegram_token_env:
        program_args.extend(["--telegram-token-env", telegram_token_env])
    if telegram_proxy_url:
        program_args.extend(["--telegram-proxy-url", telegram_proxy_url])
    return {
        "Label": label,
        "ProgramArguments": program_args,
        "WorkingDirectory": str(cwd_path),
        "RunAtLoad": True,
        "KeepAlive": True,
        "StandardOutPath": str(logs / f"{label}.out.log"),
        "StandardErrorPath": str(logs / f"{label}.err.log"),
        "EnvironmentVariables": {
            "PYTHONUNBUFFERED": "1",
        },
    }


def install_daemon_launch_agent(
    *,
    db_path: Path | str,
    cwd: Path | str,
    label: str | None = None,
    python: Path | str | None = None,
    interval: int = 300,
    timeout: int = 1800,
    allow_full_train: bool = False,
    notify_telegram: bool = True,
    telegram_token_file: str | None = None,
    telegram_token_env: str = "MYCODEX_TELEGRAM_BOT_TOKEN",
    telegram_proxy_url: str | None = None,
    log_dir: Path | str | None = None,
    bootstrap: bool = True,
) -> dict[str, Any]:
    db_resolved = Path(db_path).expanduser().resolve()
    label = label or default_daemon_launch_agent_label(db_resolved)
    path = launch_agent_path(label)
    plist = daemon_launch_agent_plist(
        label=label,
        db_path=db_resolved,
        cwd=cwd,
        python=python,
        interval=interval,
        timeout=timeout,
        allow_full_train=allow_full_train,
        notify_telegram=notify_telegram,
        telegram_token_file=telegram_token_file,
        telegram_token_env=telegram_token_env,
        telegram_proxy_url=telegram_proxy_url,
        log_dir=log_dir,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    Path(plist["StandardOutPath"]).parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as fh:
        plistlib.dump(plist, fh, sort_keys=True)
    commands: list[dict[str, Any]] = []
    if bootstrap:
        gui_domain = f"gui/{os.getuid()}"
        commands.append(_run_launchctl(["bootout", gui_domain, str(path)], check=False))
        commands.append(_run_launchctl(["bootstrap", gui_domain, str(path)], check=False))
        commands.append(_run_launchctl(["enable", f"{gui_domain}/{label}"], check=False))
    return {"label": label, "path": str(path), "plist": plist, "commands": commands}


def uninstall_daemon_launch_agent(
    *,
    db_path: Path | str,
    label: str | None = None,
    remove_file: bool = True,
) -> dict[str, Any]:
    label = label or default_daemon_launch_agent_label(db_path)
    path = launch_agent_path(label)
    gui_domain = f"gui/{os.getuid()}"
    commands = [
        _run_launchctl(["bootout", gui_domain, str(path)], check=False),
        _run_launchctl(["disable", f"{gui_domain}/{label}"], check=False),
    ]
    removed = False
    if remove_file and path.exists():
        path.unlink()
        removed = True
    return {"label": label, "path": str(path), "removed": removed, "commands": commands}


def daemon_launch_agent_status(*, db_path: Path | str, label: str | None = None) -> dict[str, Any]:
    label = label or default_daemon_launch_agent_label(db_path)
    path = launch_agent_path(label)
    gui_domain = f"gui/{os.getuid()}"
    print_result = _run_launchctl(["print", f"{gui_domain}/{label}"], check=False)
    return {
        "label": label,
        "path": str(path),
        "installed": path.exists(),
        "loaded": print_result["returncode"] == 0,
        "launchctl": print_result,
    }


def _run_launchctl(args: list[str], *, check: bool) -> dict[str, Any]:
    try:
        result = subprocess.run(
            ["launchctl", *args],
            text=True,
            capture_output=True,
            check=check,
        )
        return {
            "args": ["launchctl", *args],
            "returncode": result.returncode,
            "stdout": result.stdout[-4000:],
            "stderr": result.stderr[-4000:],
        }
    except FileNotFoundError as exc:
        return {
            "args": ["launchctl", *args],
            "returncode": 127,
            "stdout": "",
            "stderr": str(exc),
        }
