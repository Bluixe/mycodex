"""Opt-in, real SSH acceptance test; no existing keys or SSH configuration.

Linux CI: MYCODEX_TEST_LOOPBACK_SSH=1 python -m unittest tests.test_ssh_integration -v
Requires OpenSSH client/server, git, python3, /run/sshd, and passwordless sudo
when the current runner is not root. Only 127.0.0.1 is contacted. The fixture
uses the existing runner account and destroys its temporary keys and processes.
"""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from mycodex import experiment_ops as ops
from mycodex.experiment_artifacts import canonical_hash, receipt_observation_status


def _isolated_ssh_args(args: list[str], known_hosts: str) -> list[str]:
    """Only the SSH options are adapted; networking and subprocesses stay real."""
    return [args[0], "-F", "/dev/null", "-o", "StrictHostKeyChecking=yes",
            "-o", f"UserKnownHostsFile={known_hosts}",
            "-o", "GlobalKnownHostsFile=/dev/null", "-o", "IdentityAgent=none",
            "-o", "ControlMaster=no", "-o", "ControlPath=none", *args[1:]]


_OBSERVER_SOURCE = r'''
import json, pathlib, sys, time
from unittest.mock import patch
from mycodex import experiment_ops as ops
from tests.test_ssh_integration import _isolated_ssh_args
config = json.loads(pathlib.Path(sys.argv[1]).read_text())
target = ops.RemoteTarget(**config["target"])
original = ops._ssh_args
def temporary_args(target, key, **kwargs):
    return _isolated_ssh_args(original(target, key, **kwargs), config["known_hosts"])
with patch.object(ops, "_ssh_args", temporary_args):
    # Deliberately discard the real SSH response. The parent then kills this
    # observer before it can communicate any launch result to its caller.
    ops.run_remote_script(target, config["script"], config["client_key"], 8)
pathlib.Path(config["ready"]).write_text("response discarded")
time.sleep(30)
'''


@unittest.skipUnless(sys.platform.startswith("linux") and
                     os.environ.get("MYCODEX_TEST_LOOPBACK_SSH") == "1",
                     "Real loopback SSH requires Linux and MYCODEX_TEST_LOOPBACK_SSH=1")
class LoopbackSSHIntegrationTests(unittest.TestCase):
    def setUp(self):
        import pwd

        self.started = time.monotonic()
        self.deadline = self.started + 75
        self.server = None
        self.observer = None
        self.server_log = None
        self.server_pid = None
        self.server_start_ticks = None
        # Use a private sibling of the existing home contents so OpenSSH can
        # retain StrictModes. Inspect directory metadata only, never user keys.
        home = Path.home()
        self.assertTrue(home.is_absolute() and home.is_dir(), "SSH fixture requires an existing absolute home")
        self.assertEqual(home, home.resolve(), "SSH fixture home ancestors must not be symlinks")
        for ancestor in (home, *home.parents):
            mode = ancestor.stat()
            self.assertIn(mode.st_uid, {0, os.getuid()}, f"Unsafe home ancestor owner: {ancestor}")
            self.assertFalse(mode.st_mode & 0o022, f"Writable home ancestor: {ancestor}")
        self.temp = tempfile.TemporaryDirectory(dir=home, prefix=".mycodex-loopback-ssh-")
        self.root = Path(self.temp.name)
        self.repo = self.root / "repository"
        self.repo.mkdir()
        self.old_handler = signal.getsignal(signal.SIGALRM)
        self.old_alarm = signal.getitimer(signal.ITIMER_REAL)
        self.addCleanup(self._restore_alarm)
        self.addCleanup(self._cleanup)
        signal.signal(signal.SIGALRM, self._deadline_expired)
        signal.setitimer(signal.ITIMER_REAL, 75)
        self.username = pwd.getpwuid(os.getuid()).pw_name
        self.sudo = [] if os.geteuid() == 0 else ["sudo", "-n"]
        for tool in ("ssh", "ssh-keygen", "git", "python3"):
            self.assertIsNotNone(shutil.which(tool), f"Opt-in SSH test requires {tool}")
        self.sshd = shutil.which("sshd") or "/usr/sbin/sshd"
        self.assertTrue(Path(self.sshd).is_file(), "Opt-in SSH test requires openssh-server")
        self.assertTrue(Path("/run/sshd").is_dir(), "CI must prepare /run/sshd")

        self.host_key = self.root / "host_ed25519"
        self.client_key = self.root / "client_ed25519"
        for key in (self.host_key, self.client_key):
            self._local(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)])
        authorized = self.root / "authorized_keys"
        authorized.write_text(self.client_key.with_suffix(".pub").read_text())
        authorized.chmod(0o600)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.port = reservation.getsockname()[1]
        self.assertGreater(self.port, 1024)
        self.known_hosts = self.root / "known_hosts"
        self.known_hosts.write_text(f"[127.0.0.1]:{self.port} " + self.host_key.with_suffix(".pub").read_text())
        self.pid_file = self.root / "sshd.pid"
        self.config = self.root / "sshd_config"
        self.config.write_text("\n".join([
            f"Port {self.port}", "ListenAddress 127.0.0.1", "AddressFamily inet",
            f"HostKey {self.host_key}", f"PidFile {self.pid_file}",
            f"AuthorizedKeysFile {authorized}", f"AllowUsers {self.username}",
            "StrictModes yes", "AuthenticationMethods publickey", "PubkeyAuthentication yes",
            "PasswordAuthentication no", "KbdInteractiveAuthentication no", "PermitEmptyPasswords no",
            "PermitRootLogin prohibit-password", "UsePAM yes", "UseDNS no",
            "AllowAgentForwarding no", "AllowTcpForwarding no", "X11Forwarding no",
            "PermitUserEnvironment no", "PrintMotd no", "PrintLastLog no", "LogLevel VERBOSE", "",
        ]))
        self._local([*self.sudo, self.sshd, "-t", "-f", str(self.config)])
        self.server_log = (self.root / "sshd.log").open("w+")
        self.server = subprocess.Popen([*self.sudo, self.sshd, "-D", "-e", "-f", str(self.config)],
                                       stdin=subprocess.DEVNULL, stdout=self.server_log,
                                       stderr=subprocess.STDOUT, start_new_session=True)
        self._wait(lambda: self.pid_file.exists(), "temporary sshd did not start", seconds=5)
        self._capture_server_identity()
        self.target = ops.RemoteTarget(f"{self.username}@127.0.0.1", str(self.repo), port=self.port)
        original = ops._ssh_args

        def temporary_args(target, key, **kwargs):
            self.assertEqual(target.login, f"{self.username}@127.0.0.1")
            self.assertEqual(target.port, self.port)
            self.assertEqual(str(key), str(self.client_key))
            return _isolated_ssh_args(original(target, key, **kwargs), str(self.known_hosts))

        ssh_options = patch.object(ops, "_ssh_args", temporary_args)
        ssh_options.start()
        self.addCleanup(ssh_options.stop)
        probe = self._ssh("printf 'LOOPBACK_SSH_OK\\n'\n")
        self.assertEqual(probe.stdout.strip(), "LOOPBACK_SSH_OK", self._server_diagnostics())
        self._local(["git", "init", "--quiet"], cwd=self.repo)
        (self.repo / "tracked").write_text("loopback fixture\n")
        self._local(["git", "add", "tracked"], cwd=self.repo)
        self._local(["git", "-c", "user.name=Loopback Test", "-c", "user.email=test@example.invalid",
                     "commit", "--quiet", "-m", "temporary fixture"], cwd=self.repo)
        self.commit = self._local(["git", "rev-parse", "HEAD"], cwd=self.repo).stdout.strip()

    @staticmethod
    def _deadline_expired(_signum, _frame):
        raise TimeoutError("Loopback SSH integration exceeded its 75-second execution bound")

    def _restore_alarm(self):
        signal.setitimer(signal.ITIMER_REAL, *self.old_alarm)
        signal.signal(signal.SIGALRM, self.old_handler)

    def _remaining(self, limit=8):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Loopback SSH integration deadline exceeded")
        return min(limit, remaining)

    def _local(self, args, *, cwd=None):
        result = subprocess.run(args, cwd=cwd, capture_output=True, text=True,
                                timeout=self._remaining(), check=False)
        self.assertEqual(result.returncode, 0, f"{args[0]} failed: {result.stderr}")
        return result

    def _ssh(self, script):
        return ops.run_remote_script(self.target, script, str(self.client_key), self._remaining())

    def _server_diagnostics(self):
        try:
            return (self.root / "sshd.log").read_text()[-6000:]
        except OSError:
            return "No temporary sshd log available"

    def _wait(self, predicate, description, *, seconds=5):
        end = time.monotonic() + self._remaining(seconds)
        while time.monotonic() < end:
            value = predicate()
            if value:
                return value
            if self.server is not None and self.server.poll() is not None:
                self.fail(f"temporary sshd exited: {self._server_diagnostics()}")
            if self.observer is not None and self.observer.poll() is not None:
                stdout, stderr = self.observer.communicate(timeout=1)
                self.fail(f"local SSH observer exited unexpectedly: {stdout}\n{stderr}")
            time.sleep(0.025)
        self.fail(f"{description}; {self._server_diagnostics()}")

    def _receipt(self, execution_id, statuses):
        path = self.repo / ".mycodex" / "executions" / execution_id / "receipt.json"

        def read():
            try:
                receipt = json.loads(path.read_text())
                return receipt if receipt.get("status") in statuses else None
            except (OSError, ValueError):
                return None

        return self._wait(read, f"{execution_id} receipt never reached {statuses}")

    def _launch_script(self, execution_id, command, budget):
        manifest = {"execution_id": execution_id, "code_commit": self.commit, "test": "loopback-ssh"}
        return ops._remote_launch_script(
            target=self.target, command=command, commit=self.commit, branch=None, fetch=False,
            log_path=f"logs/{execution_id}.log", pid_path=f"logs/{execution_id}.pid", env={},
            execution_id=execution_id, manifest=manifest, time_budget_seconds=budget,
            termination_grace_seconds=0.2), manifest

    @staticmethod
    def _process_matches(pid, ticks):
        try:
            stat = Path(f"/proc/{int(pid)}/stat").read_text().rsplit(")", 1)[1].split()
            return stat[0] != "Z" and stat[19] == str(ticks)
        except (OSError, ValueError, IndexError, TypeError):
            return False

    def _capture_server_identity(self):
        pid = int(self.pid_file.read_text().strip())
        command = Path(f"/proc/{pid}/cmdline").read_bytes()
        self.assertTrue(str(self.config).encode() in command and b"sshd" in command,
                        "Temporary server PID does not match its exact configuration")
        ticks = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
        self.server_pid, self.server_start_ticks = pid, ticks

    def _server_identity_matches(self):
        if not self._process_matches(self.server_pid, self.server_start_ticks):
            return False
        try:
            command = Path(f"/proc/{self.server_pid}/cmdline").read_bytes()
            return str(self.config).encode() in command and b"sshd" in command
        except OSError:
            return False

    def _stop_observer(self):
        if self.observer is not None:
            if self.observer.poll() is None:
                os.killpg(self.observer.pid, signal.SIGKILL)
            self.observer.communicate(timeout=2)
            self.observer = None

    def _cleanup(self):
        # Cleanup has its own bounded budget and cannot be interrupted by the
        # body watchdog. Every remote PID must match this fixture's start ticks.
        signal.setitimer(signal.ITIMER_REAL, 0)
        errors = []
        try:
            self._stop_observer()
        except Exception as exc:
            errors.append(f"observer cleanup: {exc}")
        for path in self.repo.glob(".mycodex/executions/*/receipt.json"):
            try:
                receipt = json.loads(path.read_text())
                for pid_key, ticks_key, group in (("pid", "pid_start_ticks", True),
                                                  ("supervisor_pid", "supervisor_start_ticks", False)):
                    pid = receipt.get(pid_key)
                    if self._process_matches(pid, receipt.get(ticks_key)):
                        try:
                            (os.killpg if group else os.kill)(int(pid), signal.SIGKILL)
                        except ProcessLookupError:
                            pass
            except Exception as exc:
                errors.append(f"workload cleanup: {exc}")
        if self.server is not None:
            try:
                if self.server_start_ticks is None and self.pid_file.exists():
                    self._capture_server_identity()
                if self.server.poll() is None:
                    if not self._server_identity_matches():
                        raise RuntimeError("sshd identity unavailable or changed; refusing TERM")
                    subprocess.run([*self.sudo, "kill", "-TERM", str(self.server_pid)],
                                   timeout=2, capture_output=True, check=False)
                self.server.wait(timeout=2)
            except Exception as exc:
                errors.append(f"sshd cleanup: {exc}")
                try:
                    if self.server.poll() is None:
                        if not self._server_identity_matches():
                            raise RuntimeError("sshd identity unavailable or changed; refusing KILL")
                        subprocess.run([*self.sudo, "kill", "-KILL", str(self.server_pid)],
                                       timeout=2, capture_output=True, check=False)
                    self.server.wait(timeout=2)
                except Exception as fallback_exc:
                    errors.append(f"sshd fallback cleanup: {fallback_exc}")
        if self.server_log is not None:
            self.server_log.close()
        self.temp.cleanup()
        self.assertFalse(errors, "; ".join(errors))

    def test_real_ssh_receipt_retry_timeout_and_dead_supervisor(self):
        with self.subTest("lost observer response reuses exactly one workload"):
            script, manifest = self._launch_script("lost-response", "echo once >> once.count; sleep 1.5", 5)
            ready = self.root / "observer-ready"
            config = self.root / "observer.json"
            config.write_text(json.dumps({"target": {"login": self.target.login, "path": self.target.path,
                                                    "port": self.target.port},
                                          "known_hosts": str(self.known_hosts), "client_key": str(self.client_key),
                                          "script": script, "ready": str(ready)}))
            self.observer = subprocess.Popen([sys.executable, "-c", _OBSERVER_SOURCE, str(config)],
                                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                             start_new_session=True)
            self._wait(ready.exists, "observer did not finish real SSH launch", seconds=10)
            self.assertIsNone(self.observer.poll())
            self._stop_observer()
            retry = self._ssh(script)
            parsed = ops._parse_key_values(retry.stdout)
            self.assertEqual(parsed["REMOTE_REUSED"], "1")
            receipt = json.loads(base64.b64decode(parsed["REMOTE_RECEIPT"]))
            self.assertEqual(receipt["manifest_sha256"], canonical_hash(manifest))
            self._receipt("lost-response", {"completed"})
            self.assertEqual((self.repo / "once.count").read_text().splitlines(), ["once"])

        with self.subTest("numeric budget kills real workload and forbids retry"):
            script, _ = self._launch_script("timeout", "echo once >> timeout.count; sleep 30", 0.8)
            self._ssh(script)
            receipt = self._receipt("timeout", {"timed_out"})
            self.assertEqual(receipt["exit_code"], 124)
            self.assertFalse(self._process_matches(receipt["pid"], receipt["pid_start_ticks"]))
            with self.assertRaises(ops.RemoteCommandError) as retry:
                self._ssh(script)
            self.assertIn("previous execution is timed_out", str(retry.exception))
            self.assertEqual((self.repo / "timeout.count").read_text().splitlines(), ["once"])

        with self.subTest("dead supervisor requires reconciliation and never duplicates child"):
            script, manifest = self._launch_script("dead-supervisor", "echo once >> orphan.count; sleep 30", 15)
            self._ssh(script)
            receipt = self._receipt("dead-supervisor", {"running"})
            self._wait(lambda: (self.repo / "orphan.count").exists(), "orphan workload did not start")
            self.assertTrue(self._process_matches(receipt["supervisor_pid"], receipt["supervisor_start_ticks"]))
            os.kill(receipt["supervisor_pid"], signal.SIGKILL)
            self._wait(lambda: not self._process_matches(receipt["supervisor_pid"], receipt["supervisor_start_ticks"]),
                       "supervisor did not stop")
            self.assertTrue(self._process_matches(receipt["pid"], receipt["pid_start_ticks"]))
            execution_root = self.repo / ".mycodex/executions/dead-supervisor"
            observed = self._ssh(ops._remote_status_script(str(receipt["pid"]), receipt["log_path"], 5,
                                  receipt_path=str(execution_root / "receipt.json")))
            values = ops._parse_key_values(observed.stdout)
            observed_receipt = json.loads(base64.b64decode(values["REMOTE_RECEIPT"]))
            self.assertEqual(observed_receipt["manifest_sha256"], canonical_hash(manifest))
            self.assertEqual(receipt_observation_status(observed_receipt, values), "needs_reconciliation")
            with self.assertRaises(ops.RemoteCommandError) as retry:
                self._ssh(script)
            self.assertIn("supervisor is absent", str(retry.exception))
            restart = "python3 " + shlex.quote(str(execution_root / "supervisor.py")) + " " + shlex.quote(str(execution_root))
            with self.assertRaises(ops.RemoteCommandError) as direct_restart:
                self._ssh(restart)
            self.assertIn("duplicate workload refused", str(direct_restart.exception))
            self.assertEqual((self.repo / "orphan.count").read_text().splitlines(), ["once"])


if __name__ == "__main__":
    unittest.main()
