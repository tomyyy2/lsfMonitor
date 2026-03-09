# -*- coding: utf-8 -*-

"""Engineer-focused CLI for lsfMonitor (M1 MVP).

Supported commands:
- bmon jobs [uid]
- bmon mem [uid] --days <N>
- bmon advise --job <JOBID>
"""

from __future__ import annotations

import argparse
import datetime
import getpass
import importlib.util
import os
import re
import statistics
import sys
from pathlib import Path


def _resolve_install_path() -> Path:
    install_path = os.environ.get('LSFMONITOR_INSTALL_PATH', '').strip()

    if install_path:
        return Path(install_path)

    # Fallback for source-tree execution:
    # <repo>/monitor/bin/lsfmon.py -> <repo>
    return Path(__file__).resolve().parents[2]


LSFMONITOR_INSTALL_PATH = _resolve_install_path()
os.environ.setdefault('LSFMONITOR_INSTALL_PATH', str(LSFMONITOR_INSTALL_PATH))
sys.path.append(str(LSFMONITOR_INSTALL_PATH / 'monitor'))

from common import common  # noqa: E402
from common import common_lsf  # noqa: E402
from common import common_sqlite3  # noqa: E402
from common import sample_daemon  # noqa: E402


def _load_config():
    """Load user/local config if possible, otherwise provide a safe fallback."""
    local_config_dir = Path.home() / '.lsfMonitor' / 'conf'
    local_config_file = local_config_dir / 'config.py'

    if local_config_file.exists():
        sys.path.append(str(local_config_dir))
        import config as local_cfg  # type: ignore

        return local_cfg

    try:
        from conf import config as bundled_cfg  # type: ignore

        return bundled_cfg
    except Exception:
        class _FallbackConfig:
            db_path = str(LSFMONITOR_INSTALL_PATH / 'db')

        return _FallbackConfig()


config = _load_config()


def _build_parser() -> argparse.ArgumentParser:
    prog_name = Path(sys.argv[0]).name or 'bmon'
    parser = argparse.ArgumentParser(
        prog=prog_name,
        description='lsfMonitor engineer CLI',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            'Examples:\n'
            f'  {prog_name} jobs\n'
            f'  {prog_name} jobs alice -a\n'
            f'  {prog_name} jobs alice --max-col-width 50\n'
            f'  {prog_name} mem --days 7\n'
            f'  {prog_name} advise --job 12345\n'
            f'  {prog_name} sample daemon install --interval 5m\n'
        ),
    )
    parser.add_argument(
        '--db-path',
        default=str(getattr(config, 'db_path', LSFMONITOR_INSTALL_PATH / 'db')),
        help='Path to sqlite database root (default: from config db_path).',
    )

    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True

    jobs_parser = subparsers.add_parser(
        'jobs',
        help='Show active jobs (default user=current user).',
        description='Show active jobs with normalized hosts/units/time fields.',
    )
    jobs_parser.add_argument('user', nargs='?', default=None, help='Target user (optional).')
    jobs_parser.add_argument('-a', '--all', action='store_true', help='Show full text (no truncation for long columns).')
    jobs_parser.add_argument('--full-text', action='store_true', help='Advanced: same as --all; keep for compatibility.')
    jobs_parser.add_argument(
        '--max-col-width',
        type=int,
        default=None,
        help='Advanced: max width for long columns (default: 30; <=0 means unlimited).',
    )
    jobs_parser.set_defaults(handler=_handle_jobs)

    mem_parser = subparsers.add_parser(
        'mem',
        help='Show memory usage summary. Default stats include done/exit jobs; use -r to merge running jobs.',
        description='Show memory usage summary. Default: finished jobs (done/exit) from sampled DB; optional -r merges current RUN jobs.',
    )
    mem_parser.add_argument('user', nargs='?', default=None, help='Target user (optional).')
    mem_parser.add_argument('--days', type=int, default=7, help='Lookback days (default: 7).')
    mem_parser.add_argument('-r', '--running', action='store_true', help='Merge current RUN jobs into summary.')
    mem_parser.set_defaults(handler=_handle_mem)

    advise_parser = subparsers.add_parser('advise', help='Show memory suggestion for one job.')
    advise_parser.add_argument('job_id', nargs='?', default=None, help='Job ID (positional shortcut).')
    advise_parser.add_argument('-j', '--job', dest='job', default=None, help='Job ID.')
    advise_parser.set_defaults(handler=_handle_advise, require_lsf=True)

    sample_parser = subparsers.add_parser('sample', help='Sampling operations.')
    sample_subparsers = sample_parser.add_subparsers(dest='sample_cmd')
    sample_subparsers.required = True

    daemon_parser = sample_subparsers.add_parser('daemon', help='Manage sampling daemon service.')
    daemon_subparsers = daemon_parser.add_subparsers(dest='daemon_cmd')
    daemon_subparsers.required = True

    daemon_install = daemon_subparsers.add_parser('install', help='Install sampling daemon service.')
    daemon_install.add_argument('--interval', default='5m', help='Sampling interval, e.g. 5m / 300 / 1h.')
    daemon_install.set_defaults(handler=_handle_sample_daemon_install, require_lsf=True)

    daemon_start = daemon_subparsers.add_parser('start', help='Start sampling daemon service.')
    daemon_start.add_argument('--interval', default='5m', help='Sampling interval, e.g. 5m / 300 / 1h.')
    daemon_start.set_defaults(handler=_handle_sample_daemon_start, require_lsf=True)

    daemon_stop = daemon_subparsers.add_parser('stop', help='Stop sampling daemon service.')
    daemon_stop.set_defaults(handler=_handle_sample_daemon_stop, require_lsf=False)

    daemon_status = daemon_subparsers.add_parser('status', help='Show sampling daemon status.')
    daemon_status.set_defaults(handler=_handle_sample_daemon_status, require_lsf=False)

    daemon_uninstall = daemon_subparsers.add_parser('uninstall', help='Uninstall sampling daemon service.')
    daemon_uninstall.set_defaults(handler=_handle_sample_daemon_uninstall, require_lsf=False)

    daemon_run_loop = daemon_subparsers.add_parser('run-loop', help=argparse.SUPPRESS)
    daemon_run_loop.add_argument('--interval', default='5m', help='Sampling interval, e.g. 5m / 300 / 1h.')
    daemon_run_loop.set_defaults(handler=_handle_sample_daemon_run_loop, require_lsf=False)

    return parser


def _ensure_lsf_environment() -> tuple[str, str]:
    """Return (tool, cluster). Exit-friendly caller handles empty tool."""
    if os.environ.get('LSFMONITOR_FAKE_RUN', '') == 'True':
        return 'LSF', 'FAKE_CLUSTER'

    tool, _tool_version, cluster, _master = common_lsf.get_lsid_info()

    if not tool:
        return '', ''

    return tool, cluster


def _format_float(value: float | None) -> str:
    if value is None:
        return 'N/A'

    return f'{value:.1f}'


def _safe_float(value) -> float | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    value_str = str(value).strip()

    if (not value_str) or (value_str in {'N/A', '-', 'None'}):
        return None

    try:
        return float(value_str)
    except Exception:
        return None


def _strip_ansi(text: str) -> str:
    return re.sub(r'\x1b\[[0-9;]*m', '', str(text))


def _print_table(headers: list[str], rows: list[list[str]], right_align_headers: set[str] | None = None) -> None:
    if not rows:
        print('(no data)')
        return

    align_right = right_align_headers or set()
    widths = [len(str(header)) for header in headers]

    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(_strip_ansi(str(cell))))

    def _format_cell(header: str, value: str, width: int) -> str:
        raw = str(value)
        text_len = len(_strip_ansi(raw))
        padding = max(width - text_len, 0)
        if header in align_right:
            return (' ' * padding) + raw
        return raw + (' ' * padding)

    header_line = '  '.join(_format_cell(str(header), str(header), widths[index]) for index, header in enumerate(headers))
    split_line = '  '.join('-' * width for width in widths)

    print(header_line)
    print(split_line)

    for row in rows:
        print('  '.join(_format_cell(str(headers[index]), str(cell), widths[index]) for index, cell in enumerate(row)))


def _resolve_db_path(cluster: str, db_root_override: str | None = None) -> Path:
    if db_root_override:
        db_root = Path(db_root_override).expanduser()
    else:
        db_root = Path(getattr(config, 'db_path', str(LSFMONITOR_INSTALL_PATH / 'db')))

    if cluster:
        return db_root / cluster

    return db_root / 'monitor'


def _iter_recent_dates(days: int) -> list[str]:
    today = datetime.date.today()
    date_list = []

    for offset in range(days):
        day = today - datetime.timedelta(days=offset)
        date_list.append(day.strftime('%Y%m%d'))

    # Oldest -> newest for better readability.
    date_list.reverse()

    return date_list


def _parse_lsf_datetime(value: str) -> datetime.datetime | None:
    value = (value or '').strip()

    if not value:
        return None

    now = datetime.datetime.now()

    for fmt in ('%a %b %d %H:%M:%S', '%b %d %H:%M:%S', '%b %d %H:%M'):
        try:
            parsed = datetime.datetime.strptime(value, fmt).replace(year=now.year)
            if parsed > (now + datetime.timedelta(days=1)):
                parsed = parsed.replace(year=now.year - 1)
            return parsed
        except Exception:
            continue

    return None


def _format_duration(delta_seconds: float | int | None, with_seconds: bool = True) -> str:
    if delta_seconds is None:
        return 'N/A'

    total = int(max(float(delta_seconds), 0))
    hours, rem = divmod(total, 3600)
    minutes, seconds = divmod(rem, 60)
    if with_seconds:
        return f'{hours:02d}:{minutes:02d}:{seconds:02d}'
    return f'{hours:02d}:{minutes:02d}'


def _format_submit_time(value: str) -> str:
    parsed = _parse_lsf_datetime(value)
    if parsed is None:
        return 'N/A' if not str(value or '').strip() else str(value)
    return parsed.strftime('%Y-%m-%d %H:%M:%S')


def _normalize_exec_hosts(value: str) -> str:
    text = str(value or '').strip()
    if not text:
        return 'N/A'

    parts = [token for token in re.split(r'[,:+\s]+', text) if token]
    if not parts:
        return text

    host_counts: dict[str, int] = {}
    host_order: list[str] = []

    for part in parts:
        match = re.match(r'^(?:(\d+)\*)?(.+)$', part)
        if not match:
            continue
        count = int(match.group(1) or '1')
        host = (match.group(2) or '').strip()
        if not host:
            continue
        if host not in host_counts:
            host_order.append(host)
            host_counts[host] = 0
        host_counts[host] += count

    if not host_counts:
        return text

    normalized = []
    for host in host_order:
        count = host_counts[host]
        normalized.append(f'{count}*{host}' if count > 1 else host)

    return ' '.join(normalized)


def _format_scaled_memory(value, input_unit: str = 'MB') -> str:
    number = _safe_float(value)
    if number is None:
        return 'N/A'

    factor_from_unit = {
        'B': 1,
        'K': 1024,
        'KB': 1024,
        'M': 1024**2,
        'MB': 1024**2,
        'G': 1024**3,
        'GB': 1024**3,
        'T': 1024**4,
        'TB': 1024**4,
    }
    unit = str(input_unit or 'MB').upper()
    base = factor_from_unit.get(unit, 1024**2)
    bytes_value = number * base

    for target_unit, scale in (('T', 1024**4), ('G', 1024**3), ('M', 1024**2), ('K', 1024)):
        if bytes_value >= scale:
            converted = bytes_value / scale
            if abs(converted - round(converted)) < 1e-9:
                text = f'{int(round(converted))}'
            else:
                text = f'{converted:.1f}'.rstrip('0').rstrip('.')
            return f'{text}{target_unit}'

    return f'{int(bytes_value)}B'


def _format_memory_g(value, input_unit: str = 'MB') -> str:
    number = _safe_float(value)
    if number is None:
        return 'N/A'

    factor_from_unit = {
        'B': 1 / (1024**3),
        'K': 1 / (1024**2),
        'KB': 1 / (1024**2),
        'M': 1 / 1024,
        'MB': 1 / 1024,
        'G': 1,
        'GB': 1,
        'T': 1024,
        'TB': 1024,
    }
    multiplier = factor_from_unit.get(str(input_unit or 'MB').upper(), 1 / 1024)
    g_value = number * multiplier
    text = f'{g_value:.1f}'.rstrip('0').rstrip('.')
    return f'{text}G'


def _supports_ansi_color() -> bool:
    if os.environ.get('NO_COLOR'):
        return False
    term = os.environ.get('TERM', '').lower()
    if term in {'', 'dumb'}:
        return False
    return bool(getattr(sys.stdout, 'isatty', lambda: False)())


def _colorize(text: str, color: str) -> str:
    if not _supports_ansi_color():
        return text

    color_map = {
        'red': '\033[31m',
        'green': '\033[32m',
        'yellow': '\033[33m',
        'reset': '\033[0m',
    }
    prefix = color_map.get(color)
    reset = color_map['reset']
    if not prefix:
        return text
    return f'{prefix}{text}{reset}'


def _get_mem_util_thresholds() -> tuple[float, float, float]:
    defaults = (40.0, 90.0, 100.0)  # waste_lt, ok_lt, warn_lte
    raw = getattr(config, 'mem_util_thresholds', None)

    if isinstance(raw, dict):
        try:
            waste_lt = float(raw.get('waste_lt', defaults[0]))
            ok_lt = float(raw.get('ok_lt', defaults[1]))
            warn_lte = float(raw.get('warn_lte', defaults[2]))
            if waste_lt < ok_lt <= warn_lte:
                return (waste_lt, ok_lt, warn_lte)
        except Exception:
            return defaults

    return defaults


def _format_mem_util(requested_mb: float | None, used_mb: float | None) -> str:
    if used_mb is None:
        return 'N/A'

    if (requested_mb is None) or (requested_mb <= 0):
        if used_mb >= 20 * 1024:
            return 'N/A (NEED_RUSAGE)'
        return 'N/A'

    util = (used_mb / requested_mb) * 100.0
    waste_lt, ok_lt, warn_lte = _get_mem_util_thresholds()

    if util < waste_lt:
        tag = 'WASTE'
        color = 'yellow'
    elif util < ok_lt:
        tag = 'OK'
        color = 'green'
    elif util <= warn_lte:
        tag = 'WARN'
        color = 'yellow'
    else:
        tag = 'EXCEED'
        color = 'red'

    text = f'{util:.1f}% ({tag})'
    return _colorize(text, color)


def _parse_time_left_seconds(value) -> float | None:
    text = str(value or '').strip()
    if (not text) or (text in {'-', 'N/A', 'None'}):
        return None

    # Some LSF outputs append state suffix (e.g. 483:13L / 483.15L); strip trailing letters first.
    text = re.sub(r'[A-Za-z]+$', '', text).strip()
    if (not text) or (text in {'-', 'N/A', 'None'}):
        return None

    # Common LSF style: HHH:MM (e.g. 483:13)
    hhmm_match = re.match(r'^\s*(\d+):(\d{1,2})\s*$', text)
    if hhmm_match:
        hours = int(hhmm_match.group(1))
        minutes = int(hhmm_match.group(2))
        return float(hours * 3600 + minutes * 60)

    # Also accept HH:MM:SS
    hms_match = re.match(r'^\s*(\d+):(\d{1,2}):(\d{1,2})\s*$', text)
    if hms_match:
        hours = int(hms_match.group(1))
        minutes = int(hms_match.group(2))
        seconds = int(hms_match.group(3))
        return float(hours * 3600 + minutes * 60 + seconds)

    # Some LSF environments render as HHH.MM where MM is minute field (e.g. 483.15 -> 483h15m)
    hh_dot_mm_match = re.match(r'^\s*(\d+)\.(\d{1,2})\s*$', text)
    if hh_dot_mm_match:
        hours = int(hh_dot_mm_match.group(1))
        minutes = int(hh_dot_mm_match.group(2))
        if 0 <= minutes < 60:
            return float(hours * 3600 + minutes * 60)

    match = re.search(r'([0-9]+(?:\.[0-9]+)?)', text)
    if not match:
        return None

    number = float(match.group(1))
    lowered = text.lower()

    if 'sec' in lowered or lowered.endswith('s'):
        return number
    if 'hour' in lowered or lowered.endswith('h'):
        return number * 3600.0
    if 'min' in lowered or lowered.endswith('m'):
        return number * 60.0

    # Plain numeric fallback: keep historical assumption as minutes.
    return number * 60.0


def _query_time_left_seconds(job_id: str) -> float | None:
    if not job_id:
        return None

    return_code, stdout, _stderr = common.run_command(f'bjobs -o "time_left" -noheader {job_id}')
    if return_code != 0:
        return None

    text = stdout.decode('utf-8', 'ignore').strip()
    return _parse_time_left_seconds(text)


def _query_time_left_map(user: str) -> dict[str, float]:
    mapping: dict[str, float] = {}
    return_code, stdout, _stderr = common.run_command(f'bjobs -u {user} -o "jobid time_left" -noheader')
    if return_code != 0:
        return mapping

    for raw_line in stdout.decode('utf-8', 'ignore').splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if not parts:
            continue
        job_id = parts[0].strip()
        time_text = parts[1].strip() if len(parts) > 1 else ''
        seconds = _parse_time_left_seconds(time_text)
        if seconds is not None:
            mapping[job_id] = seconds

    return mapping


def _truncate_text(value: str, max_width: int | None) -> str:
    text = str(value)
    if (max_width is None) or (max_width <= 0):
        return text
    if len(text) <= max_width:
        return text
    if max_width <= 3:
        return text[:max_width]
    return f'{text[: max_width - 3]}...'


def _parse_cpu_time_seconds(value) -> float | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if (not text) or (text in {'N/A', '-', 'None'}):
        return None

    number_match = re.search(r'([0-9]+(?:\.[0-9]+)?)', text)
    if not number_match:
        return None

    number = float(number_match.group(1))
    lowered = text.lower()

    if 'hour' in lowered or lowered.endswith('h'):
        return number * 3600
    if 'min' in lowered or lowered.endswith('m'):
        return number * 60

    return number


def _handle_jobs(args, _tool: str, _cluster: str) -> int:
    user = str(getattr(args, 'user', '') or getpass.getuser()).strip()
    bjobs_dic = common_lsf.get_bjobs_info(command=f'bjobs -u {user} -w')

    if (not bjobs_dic) or ('JOBID' not in bjobs_dic):
        print(f'No active jobs found for user "{user}".')
        return 0

    headers = ['JOBID', 'USER', 'STAT', 'QUEUE', 'EXEC_HOST', 'JOB_NAME', 'PWD', 'REQ_CPU', 'CPU', 'REQ_MEM', 'MEM', 'MEM_UTIL', 'SUBMIT_TIME', 'RUN_TIME', 'LEFT_TIME']
    rows: list[list[str]] = []

    # Performance: prefetch job details and time_left in batch to avoid per-job scheduler calls.
    job_info_batch = common_lsf.get_bjobs_uf_info(command=f'bjobs -u {user} -UF')
    time_left_map = _query_time_left_map(user)

    long_col_default = 30
    max_col_width = getattr(args, 'max_col_width', None)
    if max_col_width is None:
        max_col_width = -1 if (getattr(args, 'all', False) or getattr(args, 'full_text', False)) else long_col_default

    job_count = len(bjobs_dic.get('JOBID', []))

    for index in range(job_count):
        values = {k: bjobs_dic.get(k, []) for k in ['JOBID', 'USER', 'STAT', 'QUEUE', 'EXEC_HOST', 'JOB_NAME', 'SUBMIT_TIME']}
        job_id = str(values['JOBID'][index]) if index < len(values['JOBID']) else ''
        user_text = str(values['USER'][index]) if index < len(values['USER']) else ''
        stat = str(values['STAT'][index]) if index < len(values['STAT']) else ''
        queue = str(values['QUEUE'][index]) if index < len(values['QUEUE']) else ''
        exec_host = str(values['EXEC_HOST'][index]) if index < len(values['EXEC_HOST']) else ''
        job_name = str(values['JOB_NAME'][index]) if index < len(values['JOB_NAME']) else ''
        submit_time = str(values['SUBMIT_TIME'][index]) if index < len(values['SUBMIT_TIME']) else ''

        picked = _pick_job_info(job_info_batch, job_id) or {}
        if (not picked) and job_id:
            job_info = common_lsf.get_bjobs_uf_info(command=f'bjobs -UF {job_id}')
            if (not job_info) and job_id:
                job_info = common_lsf.get_bjobs_uf_info(command=f'bjobs -d -UF {job_id}')
            picked = _pick_job_info(job_info, job_id) or {}
        req_cpu = str(picked.get('processors_requested', 'N/A') or 'N/A')
        req_mem_mb = _safe_float(picked.get('rusage_mem'))
        used_mem_mb = _safe_float(picked.get('mem'))
        if used_mem_mb is None:
            used_mem_mb = _safe_float(picked.get('max_mem'))

        req_mem = _format_memory_g(req_mem_mb, input_unit='MB')
        mem_used = _format_memory_g(used_mem_mb, input_unit='MB')
        mem_util = _format_mem_util(req_mem_mb, used_mem_mb)

        started_dt = _parse_lsf_datetime(str(picked.get('started_time', '')))
        runtime_seconds = (datetime.datetime.now() - started_dt).total_seconds() if started_dt else None
        run_time = _format_duration(runtime_seconds, with_seconds=False)

        left_seconds = _parse_time_left_seconds(picked.get('time_left'))
        if left_seconds is None:
            left_seconds = time_left_map.get(job_id)
        if left_seconds is None:
            left_seconds = _query_time_left_seconds(job_id)
        left_time = _format_duration(left_seconds, with_seconds=False)

        cpu_time_seconds = _parse_cpu_time_seconds(picked.get('cpu_time'))
        cpu_util = None
        if runtime_seconds and runtime_seconds > 0 and cpu_time_seconds is not None:
            cpu_util = max(0.0, cpu_time_seconds / runtime_seconds) * 100.0

        cpu_text = 'N/A' if cpu_util is None else f'{cpu_util:.1f}%'
        pwd = str(picked.get('cwd', 'N/A') or 'N/A')

        rows.append([
            job_id,
            user_text,
            stat,
            queue,
            _truncate_text(_normalize_exec_hosts(exec_host), max_col_width),
            _truncate_text(job_name, max_col_width),
            _truncate_text(pwd, max_col_width),
            req_cpu,
            cpu_text,
            req_mem,
            mem_used,
            mem_util,
            _format_submit_time(submit_time),
            run_time,
            left_time,
        ])

    print(f'User: {user}')
    print(f'Active jobs: {job_count}')
    _print_table(headers, rows, right_align_headers={'REQ_CPU', 'CPU', 'REQ_MEM', 'MEM', 'MEM_UTIL'})

    return 0


def _collect_user_mem_rows(db_file: Path, table_name: str):
    result, db_conn = common_sqlite3.connect_db_file(str(db_file), mode='read')

    if result != 'passed':
        return []

    try:
        table_list = common_sqlite3.get_sql_table_list(str(db_file), db_conn)

        if table_name not in table_list:
            return []

        data_dic = common_sqlite3.get_sql_table_data(
            str(db_file),
            db_conn,
            table_name,
            ['job', 'status', 'queue', 'project', 'rusage_mem', 'max_mem'],
        )

        jobs = data_dic.get('job', [])
        status = data_dic.get('status', [])
        queue = data_dic.get('queue', [])
        project = data_dic.get('project', [])
        rusage_mem = data_dic.get('rusage_mem', [])
        max_mem = data_dic.get('max_mem', [])

        rows = []

        for index in range(len(jobs)):
            rows.append(
                {
                    'job': jobs[index],
                    'status': status[index] if index < len(status) else '',
                    'queue': queue[index] if index < len(queue) else '',
                    'project': project[index] if index < len(project) else '',
                    'rusage_mem': rusage_mem[index] if index < len(rusage_mem) else '',
                    'max_mem': max_mem[index] if index < len(max_mem) else '',
                }
            )

        return rows
    finally:
        try:
            db_conn.close()
        except Exception:
            pass


def _handle_mem(args, _tool: str, cluster: str) -> int:
    if args.days <= 0:
        print('Error: --days must be a positive integer.', file=sys.stderr)
        return 1

    user = str(getattr(args, 'user', '') or getpass.getuser()).strip()
    include_running = bool(getattr(args, 'running', False))
    db_path = _resolve_db_path(cluster, args.db_path)
    user_db_path = db_path / 'user'
    table_name = f'user_{user}'
    date_list = _iter_recent_dates(args.days)

    summary_rows: list[list[str]] = []
    all_requested: list[float] = []
    all_used: list[float] = []
    all_waste_ratio: list[float] = []

    for date_string in date_list:
        db_file = user_db_path / f'{date_string}.db'

        if not db_file.exists():
            continue

        row_data = _collect_user_mem_rows(db_file, table_name)

        if not row_data:
            continue

        requested = [_safe_float(item['rusage_mem']) for item in row_data]
        used = [_safe_float(item['max_mem']) for item in row_data]

        requested_values = [value for value in requested if value is not None]
        used_values = [value for value in used if value is not None]

        waste_ratio_values = []
        for req, real in zip(requested, used):
            if (req is None) or (real is None) or (req <= 0):
                continue

            waste_ratio_values.append(max(0.0, (req - real) / req))

        all_requested.extend(requested_values)
        all_used.extend(used_values)
        all_waste_ratio.extend(waste_ratio_values)

        summary_rows.append(
            [
                date_string,
                str(len(row_data)),
                _format_scaled_memory(statistics.mean(requested_values) if requested_values else None, input_unit='MB'),
                _format_scaled_memory(statistics.mean(used_values) if used_values else None, input_unit='MB'),
                _format_float(statistics.mean(waste_ratio_values) * 100 if waste_ratio_values else None),
            ]
        )

    if include_running:
        bjobs_dic = common_lsf.get_bjobs_uf_info(command=f'bjobs -u {user} -r -UF')
        running_rows = []
        for job_id, job_info in bjobs_dic.items():
            requested_mb = _safe_float(job_info.get('rusage_mem'))
            used_mb = _safe_float(job_info.get('mem'))
            if used_mb is None:
                used_mb = _safe_float(job_info.get('max_mem'))

            running_rows.append((requested_mb, used_mb))

        running_req = [req for req, _ in running_rows if req is not None]
        running_used = [used for _, used in running_rows if used is not None]
        running_waste = []
        for req, used in running_rows:
            if (req is None) or (used is None) or (req <= 0):
                continue
            running_waste.append(max(0.0, (req - used) / req))

        if running_rows:
            all_requested.extend(running_req)
            all_used.extend(running_used)
            all_waste_ratio.extend(running_waste)
            summary_rows.append([
                'RUNNING',
                str(len(running_rows)),
                _format_scaled_memory(statistics.mean(running_req) if running_req else None, input_unit='MB'),
                _format_scaled_memory(statistics.mean(running_used) if running_used else None, input_unit='MB'),
                _format_float(statistics.mean(running_waste) * 100 if running_waste else None),
            ])

    if not summary_rows:
        print(f'No memory samples found for user "{user}" in last {args.days} day(s).')
        print('Hint: collect user history first, for example: bsample -u')
        return 0

    print(f'User: {user}')
    print(f'Cluster DB: {user_db_path}')
    print(f'Mode: done/exit{", merged RUN" if include_running else ""}')
    _print_table(['Date', 'Jobs', 'AvgReq', 'AvgMax', 'PotentialWaste(%)'], summary_rows)

    overall_req = statistics.mean(all_requested) if all_requested else None
    overall_used = statistics.mean(all_used) if all_used else None
    overall_waste = statistics.mean(all_waste_ratio) * 100 if all_waste_ratio else None

    print('')
    print('Overall')
    print(f'  Avg requested memory : {_format_scaled_memory(overall_req, input_unit="MB")}')
    print(f'  Avg max used memory  : {_format_scaled_memory(overall_used, input_unit="MB")}')
    print(f'  Potential waste      : {_format_float(overall_waste)} %')

    if (overall_req is not None) and (overall_used is not None):
        if overall_req > overall_used * 1.5:
            print('  Advice               : Requested memory appears high; consider reducing rusage[mem].')
        elif overall_req < overall_used * 1.1:
            print('  Advice               : Requested memory is close to usage; keep a small safety buffer.')
        else:
            print('  Advice               : Request/usage looks balanced.')

    return 0


def _pick_job_info(job_dic: dict, job_id: str):
    if job_id in job_dic:
        return job_dic[job_id]

    if not job_dic:
        return None

    # Fallback: first item (some schedulers may return normalized key).
    first_key = list(job_dic.keys())[0]
    return job_dic[first_key]


def _resolve_advise_job_id(args) -> str | None:
    for candidate in [getattr(args, 'job', None), getattr(args, 'job_id', None)]:
        text = str(candidate or '').strip()
        if text:
            return text
    return None


def _analyze_exit_reason(job_info: dict, requested_mb: float | None, used_mb: float | None) -> tuple[str | None, list[str], str | None]:
    status = str(job_info.get('status', '') or '').upper()
    if status != 'EXIT':
        return None, [], None

    term_signal = str(job_info.get('term_signal', '') or '').strip()
    exit_code = str(job_info.get('exit_code', '') or '').strip()
    evidence: list[str] = []

    if term_signal:
        evidence.append(f'term_signal={term_signal}')
    if exit_code:
        evidence.append(f'exit_code={exit_code}')

    if term_signal == 'TERM_MEMLIMIT':
        advice = 'Exit reason is MEMLIMIT. Increase rusage[mem] and align select[mem>] with observed max memory.'
        if (requested_mb is not None) and (used_mb is not None):
            over = used_mb - requested_mb
            if over > 0:
                advice += f' Observed max exceeded request by {_format_scaled_memory(over, input_unit="MB")}. '
        return 'TERM_MEMLIMIT', evidence, advice

    if (requested_mb is not None) and (used_mb is not None) and (used_mb > requested_mb):
        evidence.append('observed_max > requested')
        return 'MEMORY_EXCEEDED_REQUEST', evidence, 'Memory usage exceeded requested rusage[mem]; increase memory request to avoid EXIT.'

    if term_signal:
        return term_signal, evidence, f'Job exited with {term_signal}; investigate scheduler/command logs for detailed cause.'

    return 'EXIT', evidence, 'Job exited; check command stderr/stdout and scheduler logs for root cause.'


def _handle_advise(args, _tool: str, _cluster: str) -> int:
    job_id = _resolve_advise_job_id(args)
    if not job_id:
        print('Error: please provide a job id, e.g. `bmon advise 12345` or `bmon advise -j 12345`.', file=sys.stderr)
        return 1

    job_dic = common_lsf.get_bjobs_uf_info(command=f'bjobs -UF {job_id}')

    if not job_dic:
        job_dic = common_lsf.get_bjobs_uf_info(command=f'bjobs -d -UF {job_id}')

    job_info = _pick_job_info(job_dic, job_id)

    if not job_info:
        print(f'Error: failed to find job "{job_id}" for advice.', file=sys.stderr)
        return 1

    requested_mb = _safe_float(job_info.get('rusage_mem'))
    used_mb = _safe_float(job_info.get('max_mem'))

    if used_mb is None:
        used_mb = _safe_float(job_info.get('mem'))

    print(f'Job: {job_id}')
    print(f'  User             : {job_info.get("user", "N/A")}')
    print(f'  Status           : {job_info.get("status", "N/A")}')
    print(f'  Queue            : {job_info.get("queue", "N/A")}')
    print(f'  Requested        : {_format_scaled_memory(requested_mb, input_unit="MB")}')
    print(f'  Observed Max     : {_format_scaled_memory(used_mb, input_unit="MB")}')
    print(f'  Mem Util         : {_format_mem_util(requested_mb, used_mb)}')

    exit_reason, evidence, exit_advice = _analyze_exit_reason(job_info, requested_mb, used_mb)
    if exit_reason:
        print(f'  Exit reason      : {exit_reason}')
        if evidence:
            print(f'  Evidence         : {"; ".join(evidence)}')

    if (requested_mb is None) and (used_mb is None):
        print('  Advice           : Not enough memory data yet. Run sampling first (bsample -j / -u / -m).')
        return 0

    if used_mb is None and requested_mb is not None:
        suggest_low = max(64.0, requested_mb * 0.8)
        suggest_high = max(suggest_low, requested_mb * 1.2)
        print(f'  Suggested rusage[mem]: {_format_scaled_memory(suggest_low, "MB")} ~ {_format_scaled_memory(suggest_high, "MB")}')
        print('  Advice           : No observed max memory yet; range is based on current request.')
        return 0

    if requested_mb is None and used_mb is not None:
        suggest_low = max(64.0, used_mb * 1.2)
        suggest_high = max(suggest_low, used_mb * 1.5)
        print(f'  Suggested rusage[mem]: {_format_scaled_memory(suggest_low, "MB")} ~ {_format_scaled_memory(suggest_high, "MB")}')
        print('  Advice           : Request value missing; range is estimated from observed memory.')
        return 0

    assert requested_mb is not None and used_mb is not None

    suggest_low = max(64.0, used_mb * 1.2)
    suggest_high = max(suggest_low, used_mb * 1.5)
    print(f'  Suggested rusage[mem]: {_format_scaled_memory(suggest_low, "MB")} ~ {_format_scaled_memory(suggest_high, "MB")}')

    if exit_advice:
        print(f'  Advice           : {exit_advice}')
    elif requested_mb > used_mb * 1.8:
        print('  Advice           : Request appears over-sized; reduce memory to improve cluster efficiency.')
    elif requested_mb < used_mb * 1.05:
        print('  Advice           : Request is very tight; increase memory buffer to reduce EXIT risk.')
    else:
        print('  Advice           : Request looks reasonable.')

    return 0


def _build_sample_daemon_manager(interval_text: str) -> sample_daemon.SampleDaemonManager:
    interval_seconds = sample_daemon.SampleDaemonManager.parse_interval_to_seconds(interval_text)
    return sample_daemon.SampleDaemonManager(
        install_path=LSFMONITOR_INSTALL_PATH,
        interval_seconds=interval_seconds,
    )


def _handle_sample_daemon_install(args, tool, cluster) -> int:
    _ = (tool, cluster)
    try:
        manager = _build_sample_daemon_manager(args.interval)
    except ValueError as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1

    message = manager.install()
    print(f'[bmon sample daemon] {message}')
    print(f'[bmon sample daemon] log: {manager.paths.runner_log}')
    return 0


def _handle_sample_daemon_start(args, tool, cluster) -> int:
    _ = (tool, cluster)
    try:
        manager = _build_sample_daemon_manager(args.interval)
    except ValueError as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1

    message = manager.start()
    print(f'[bmon sample daemon] {message}')
    print(f'[bmon sample daemon] log: {manager.paths.runner_log}')
    return 0


def _handle_sample_daemon_stop(args, tool, cluster) -> int:
    _ = (args, tool, cluster)
    manager = sample_daemon.SampleDaemonManager(install_path=LSFMONITOR_INSTALL_PATH)
    message = manager.stop()
    print(f'[bmon sample daemon] {message}')
    return 0


def _handle_sample_daemon_status(args, tool, cluster) -> int:
    _ = (args, tool, cluster)
    manager = sample_daemon.SampleDaemonManager(install_path=LSFMONITOR_INSTALL_PATH)
    message = manager.status()
    print(f'[bmon sample daemon] {message}')
    print(f'[bmon sample daemon] log: {manager.paths.runner_log}')
    return 0


def _handle_sample_daemon_uninstall(args, tool, cluster) -> int:
    _ = (args, tool, cluster)
    manager = sample_daemon.SampleDaemonManager(install_path=LSFMONITOR_INSTALL_PATH)
    message = manager.uninstall()
    print(f'[bmon sample daemon] {message}')
    return 0


def _handle_sample_daemon_run_loop(args, tool, cluster) -> int:
    _ = (tool, cluster)
    try:
        manager = _build_sample_daemon_manager(args.interval)
    except ValueError as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1

    manager.ensure_dirs()
    return sample_daemon.run_loop(
        install_path=LSFMONITOR_INSTALL_PATH,
        interval_seconds=manager.interval_seconds,
        log_file=manager.paths.runner_log,
    )


_ADMIN_COMMANDS = {'mgmt', 'report'}


def _first_top_level_command(argv: list[str]) -> str | None:
    index = 0

    while index < len(argv):
        token = str(argv[index]).strip()

        if token == '--db-path':
            index += 2
            continue

        if token.startswith('--db-path='):
            index += 1
            continue

        if token.startswith('-'):
            index += 1
            continue

        return token

    return None


def _delegate_to_admin_cli(argv: list[str]) -> int:
    admin_cli = LSFMONITOR_INSTALL_PATH / 'lsfmon.py'

    if not admin_cli.exists():
        print(f'Error: admin CLI entry not found: {admin_cli}', file=sys.stderr)
        return 1

    spec = importlib.util.spec_from_file_location('_lsfmon_admin_cli', admin_cli)
    if spec is None or spec.loader is None:
        print(f'Error: failed to load admin CLI entry: {admin_cli}', file=sys.stderr)
        return 1

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    delegate_main = getattr(module, 'main', None)
    if not callable(delegate_main):
        print(f'Error: admin CLI entry has no callable main(): {admin_cli}', file=sys.stderr)
        return 1

    return int(delegate_main(list(argv)))


def main(argv: list[str] | None = None) -> int:
    argv_list = list(argv) if argv is not None else list(sys.argv[1:])
    first_command = _first_top_level_command(argv_list)

    if first_command in _ADMIN_COMMANDS:
        return _delegate_to_admin_cli(argv_list)

    parser = _build_parser()
    args = parser.parse_args(argv_list)

    require_lsf = bool(getattr(args, 'require_lsf', True))

    tool, cluster = '', ''
    if require_lsf:
        tool, cluster = _ensure_lsf_environment()

        if not tool:
            print(
                'Error: No LSF/Volclava/Openlava environment detected. '\
                'Please source scheduler environment first and retry.',
                file=sys.stderr,
            )
            return 2

    return args.handler(args, tool, cluster)


if __name__ == '__main__':
    sys.exit(main())
