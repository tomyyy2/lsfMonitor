# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path


SERVICE_NAME = 'lsfmonitor-sample-daemon'


@dataclass
class DaemonPaths:
    state_dir: Path
    log_dir: Path
    env_file: Path
    service_file: Path
    pid_file: Path
    runner_log: Path


class SampleDaemonManager:
    def __init__(self, install_path: Path, interval_seconds: int = 300):
        self.install_path = install_path
        self.interval_seconds = max(interval_seconds, 60)

        state_dir = Path.home() / '.lsfMonitor' / 'daemon'
        log_dir = Path.home() / '.lsfMonitor' / 'logs'
        self.paths = DaemonPaths(
            state_dir=state_dir,
            log_dir=log_dir,
            env_file=state_dir / 'sample-daemon.env',
            service_file=state_dir / f'{SERVICE_NAME}.service',
            pid_file=state_dir / 'sample-daemon.pid',
            runner_log=log_dir / 'sample-daemon.log',
        )

    def ensure_dirs(self) -> None:
        self.paths.state_dir.mkdir(parents=True, exist_ok=True)
        self.paths.log_dir.mkdir(parents=True, exist_ok=True)

    def write_env_file(self) -> None:
        self.ensure_dirs()
        keep_keys = {
            'HOME',
            'USER',
            'SHELL',
            'PATH',
            'LD_LIBRARY_PATH',
            'PYTHONPATH',
            'LSFMONITOR_INSTALL_PATH',
            'XDG_RUNTIME_DIR',
        }

        lines = [f'LSFMONITOR_INSTALL_PATH={self.install_path}']

        for key in sorted(os.environ.keys()):
            if key in keep_keys or key.startswith('LSF') or key.startswith('LSB') or key.startswith('LIM'):
                value = str(os.environ.get(key, '')).replace('"', '\\"')
                lines.append(f'{key}="{value}"')

        self.paths.env_file.write_text('\n'.join(lines) + '\n', encoding='utf-8')

    def _service_content(self) -> str:
        return f"""[Unit]
Description=lsfMonitor sample daemon
After=default.target

[Service]
Type=simple
EnvironmentFile={self.paths.env_file}
ExecStart={self.install_path / 'monitor' / 'bin' / 'bmon'} sample daemon run-loop --interval {self.interval_seconds}
Restart=always
RestartSec=15
StandardOutput=append:{self.paths.runner_log}
StandardError=append:{self.paths.runner_log}

[Install]
WantedBy=default.target
"""

    def write_service_file(self) -> None:
        self.ensure_dirs()
        self.paths.service_file.write_text(self._service_content(), encoding='utf-8')

    @staticmethod
    def parse_interval_to_seconds(text: str) -> int:
        value = str(text or '').strip().lower()
        if not value:
            raise ValueError('interval is empty')

        if value.isdigit():
            num = int(value)
            return max(num, 60)

        unit = value[-1]
        num_text = value[:-1]
        if not num_text.isdigit() or unit not in {'s', 'm', 'h'}:
            raise ValueError('invalid interval, expected formats like 300, 5m, 1h')

        num = int(num_text)
        factor = {'s': 1, 'm': 60, 'h': 3600}[unit]
        return max(num * factor, 60)

    def systemd_available(self) -> bool:
        if os.name != 'posix':
            return False

        try:
            version = subprocess.run(
                ['systemctl', '--user', '--version'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
                timeout=10,
            )
            if version.returncode != 0:
                return False

            # systemctl binary may exist while user D-Bus session is unavailable
            # (common on headless/root shells). Treat that case as unavailable so
            # caller can use fallback mode.
            bus = subprocess.run(
                ['systemctl', '--user', 'show-environment'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
                timeout=10,
            )
            if bus.returncode != 0:
                stderr = (bus.stderr or '').lower()
                stdout = (bus.stdout or '').lower()
                if 'failed to get d-bus connection' in stderr or 'failed to connect to bus' in stderr:
                    return False
                if 'failed to get d-bus connection' in stdout or 'failed to connect to bus' in stdout:
                    return False

            return True
        except Exception:
            return False

    def _run(self, cmd: list[str], timeout: int = 20) -> subprocess.CompletedProcess:
        return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False, timeout=timeout)

    def _is_pid_running(self, pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def _fallback_start(self) -> str:
        if self.paths.pid_file.exists():
            pid_text = self.paths.pid_file.read_text(encoding='utf-8').strip()
            if pid_text.isdigit() and self._is_pid_running(int(pid_text)):
                return f'already running (pid={pid_text})'

        self.ensure_dirs()
        self.write_env_file()

        cmd = [
            str(self.install_path / 'monitor' / 'bin' / 'bmon'),
            'sample',
            'daemon',
            'run-loop',
            '--interval',
            str(self.interval_seconds),
        ]

        with self.paths.runner_log.open('a', encoding='utf-8') as log_fp:
            proc = subprocess.Popen(
                cmd,
                stdout=log_fp,
                stderr=log_fp,
                preexec_fn=os.setsid,
                env=dict(os.environ),
            )

        self.paths.pid_file.write_text(str(proc.pid), encoding='utf-8')
        return f'started (pid={proc.pid})'

    def _fallback_stop(self) -> str:
        if not self.paths.pid_file.exists():
            return 'not running'

        pid_text = self.paths.pid_file.read_text(encoding='utf-8').strip()
        if not pid_text.isdigit():
            self.paths.pid_file.unlink(missing_ok=True)
            return 'not running'

        pid = int(pid_text)
        if not self._is_pid_running(pid):
            self.paths.pid_file.unlink(missing_ok=True)
            return 'not running'

        os.killpg(pid, signal.SIGTERM)
        self.paths.pid_file.unlink(missing_ok=True)
        return f'stopped (pid={pid})'

    def _fallback_status(self) -> str:
        if not self.paths.pid_file.exists():
            return 'not running'

        pid_text = self.paths.pid_file.read_text(encoding='utf-8').strip()
        if not pid_text.isdigit():
            return 'not running'

        pid = int(pid_text)
        if self._is_pid_running(pid):
            return f'running (pid={pid})'

        return 'not running'

    def install(self) -> str:
        self.write_env_file()
        self.write_service_file()

        if self.systemd_available():
            self._run(['systemctl', '--user', 'daemon-reload'])
            return f'installed with systemd: {self.paths.service_file}'

        return f'installed with fallback mode (nohup/pidfile): {self.paths.state_dir}'

    def start(self) -> str:
        self.write_env_file()

        if self.systemd_available():
            self.write_service_file()
            self._run(['systemctl', '--user', 'daemon-reload'])
            enabled = self._run(['systemctl', '--user', 'enable', '--now', SERVICE_NAME])
            if enabled.returncode == 0:
                return 'started with systemd'

            error_text = (enabled.stderr or enabled.stdout or '').strip()
            lowered = error_text.lower()
            if ('failed to get d-bus connection' in lowered) or ('failed to connect to bus' in lowered):
                fallback = self._fallback_start()
                return f'systemd unavailable at runtime, fallback mode: {fallback}'

            return f'failed to start with systemd: {error_text}'

        return self._fallback_start()

    def stop(self) -> str:
        if self.systemd_available():
            result = self._run(['systemctl', '--user', 'stop', SERVICE_NAME])
            if result.returncode != 0 and 'not loaded' in (result.stderr or '').lower():
                return 'service not installed'
            return 'stopped with systemd'

        return self._fallback_stop()

    def status(self) -> str:
        if self.systemd_available():
            result = self._run(['systemctl', '--user', 'is-active', SERVICE_NAME])
            if result.returncode == 0:
                return 'running (systemd)'

            load_info = self._run(['systemctl', '--user', 'status', SERVICE_NAME])
            if 'could not be found' in (load_info.stderr or '').lower() or 'not-found' in (load_info.stdout or '').lower():
                return 'not installed'
            return 'not running (systemd)'

        return self._fallback_status()

    def uninstall(self) -> str:
        if self.systemd_available():
            self._run(['systemctl', '--user', 'disable', '--now', SERVICE_NAME])
            self._run(['systemctl', '--user', 'daemon-reload'])

        else:
            self._fallback_stop()

        if self.paths.service_file.exists():
            self.paths.service_file.unlink()

        if self.paths.pid_file.exists():
            self.paths.pid_file.unlink(missing_ok=True)

        if self.paths.env_file.exists():
            self.paths.env_file.unlink(missing_ok=True)

        return 'uninstalled'


def run_sample_once(install_path: Path, log_file: Path) -> int:
    bsample = install_path / 'monitor' / 'bin' / 'bsample'
    commands = [
        [str(bsample), '-q', '-l', '-U'],
        [str(bsample), '-u'],
    ]

    retry = 2

    with log_file.open('a', encoding='utf-8') as fp:
        for cmd in commands:
            ok = False
            for attempt in range(1, retry + 2):
                ts = time.strftime('%Y-%m-%d %H:%M:%S')
                fp.write(f'[{ts}] run: {" ".join(cmd)} (attempt {attempt})\n')
                fp.flush()
                result = subprocess.run(cmd, stdout=fp, stderr=fp, check=False)
                if result.returncode == 0:
                    ok = True
                    break
                time.sleep(min(5 * attempt, 15))

            if not ok:
                return 1

    return 0


def run_loop(install_path: Path, interval_seconds: int, log_file: Path) -> int:
    while True:
        run_sample_once(install_path=install_path, log_file=log_file)
        time.sleep(max(interval_seconds, 60))
