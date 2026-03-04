# -*- coding: utf-8 -*-

"""Engineer-focused CLI for lsfMonitor (M1 MVP).

Supported commands:
- lsfmon my jobs
- lsfmon my mem --days <N>
- lsfmon advise --job <JOBID>
"""

from __future__ import annotations

import argparse
import datetime
import getpass
import importlib.util
import os
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
    parser = argparse.ArgumentParser(prog=prog_name, description='lsfMonitor engineer CLI (M1 MVP).')
    parser.add_argument(
        '--db-path',
        default=str(getattr(config, 'db_path', LSFMONITOR_INSTALL_PATH / 'db')),
        help='Path to sqlite database root. Default: user/local config db_path.',
    )

    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True

    jobs_parser = subparsers.add_parser('jobs', help='Show active jobs. Default: current user; optional: specify user.')
    jobs_parser.add_argument('user', nargs='?', default=None, help='Target user (optional).')
    jobs_parser.set_defaults(handler=_handle_jobs)

    mem_parser = subparsers.add_parser('mem', help='Show memory usage summary. Default: current user; optional: specify user.')
    mem_parser.add_argument('user', nargs='?', default=None, help='Target user (optional).')
    mem_parser.add_argument('--days', type=int, default=7, help='Lookback days (default: 7).')
    mem_parser.set_defaults(handler=_handle_mem)

    # Backward compatible alias: bmon my jobs / bmon my mem
    my_parser = subparsers.add_parser('my', help=argparse.SUPPRESS)
    my_subparsers = my_parser.add_subparsers(dest='my_command')
    my_subparsers.required = True

    my_jobs_parser = my_subparsers.add_parser('jobs', help=argparse.SUPPRESS)
    my_jobs_parser.set_defaults(handler=_handle_jobs, user=None)

    my_mem_parser = my_subparsers.add_parser('mem', help=argparse.SUPPRESS)
    my_mem_parser.add_argument('--days', type=int, default=7, help='Lookback days (default: 7).')
    my_mem_parser.set_defaults(handler=_handle_mem, user=None)

    advise_parser = subparsers.add_parser('advise', help='Show memory suggestion for one job.')
    advise_parser.add_argument('--job', required=True, help='Job ID.')
    advise_parser.set_defaults(handler=_handle_advise)

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


def _print_table(headers: list[str], rows: list[list[str]]) -> None:
    if not rows:
        print('(no data)')
        return

    widths = [len(str(header)) for header in headers]

    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(str(cell)))

    header_line = '  '.join(str(header).ljust(widths[index]) for index, header in enumerate(headers))
    split_line = '  '.join('-' * width for width in widths)

    print(header_line)
    print(split_line)

    for row in rows:
        print('  '.join(str(cell).ljust(widths[index]) for index, cell in enumerate(row)))


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

    for fmt in ('%a %b %d %H:%M:%S', '%b %d %H:%M'):
        try:
            parsed = datetime.datetime.strptime(value, fmt).replace(year=now.year)
            if parsed > (now + datetime.timedelta(days=1)):
                parsed = parsed.replace(year=now.year - 1)
            return parsed
        except Exception:
            continue

    return None


def _format_duration(delta_seconds: float | int | None) -> str:
    if delta_seconds is None:
        return 'N/A'

    total = int(max(float(delta_seconds), 0))
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)

    if days > 0:
        return f'{days}d{hours:02d}h{minutes:02d}m'

    return f'{hours:02d}:{minutes:02d}:{seconds:02d}'


def _handle_jobs(args, _tool: str, _cluster: str) -> int:
    user = str(getattr(args, 'user', '') or getpass.getuser()).strip()
    bjobs_dic = common_lsf.get_bjobs_info(command=f'bjobs -u {user} -w')

    if (not bjobs_dic) or ('JOBID' not in bjobs_dic):
        print(f'No active jobs found for user "{user}".')
        return 0

    base_keys = ['JOBID', 'USER', 'STAT', 'QUEUE', 'EXEC_HOST', 'JOB_NAME', 'SUBMIT_TIME']
    keys = base_keys + ['req_cor', 'req_mem(MB)', 'ava_cpus', 'mem(MB)', 'run_time']
    rows: list[list[str]] = []

    job_count = len(bjobs_dic.get('JOBID', []))

    for index in range(job_count):
        row = []

        for key in base_keys:
            values = bjobs_dic.get(key, [])
            row.append(str(values[index]) if index < len(values) else '')

        job_id = row[0] if row else ''
        job_info = common_lsf.get_bjobs_uf_info(command=f'bjobs -UF {job_id}') if job_id else {}

        if (not job_info) and job_id:
            job_info = common_lsf.get_bjobs_uf_info(command=f'bjobs -d -UF {job_id}')

        picked = _pick_job_info(job_info, job_id) or {}
        req_core = str(picked.get('processors_requested', 'N/A') or 'N/A')
        req_mem = _format_float(_safe_float(picked.get('rusage_mem')))
        ava_cpus = req_core
        mem_used = _format_float(_safe_float(picked.get('mem')))

        started_dt = _parse_lsf_datetime(str(picked.get('started_time', '')))
        if started_dt:
            run_time = _format_duration((datetime.datetime.now() - started_dt).total_seconds())
        else:
            run_time = 'N/A'

        row.extend([req_core, req_mem, ava_cpus, mem_used, run_time])
        rows.append(row)

    print(f'User: {user}')
    print(f'Active jobs: {job_count}')
    _print_table(keys, rows)

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
                _format_float(statistics.mean(requested_values) if requested_values else None),
                _format_float(statistics.mean(used_values) if used_values else None),
                _format_float(statistics.mean(waste_ratio_values) * 100 if waste_ratio_values else None),
            ]
        )

    if not summary_rows:
        print(f'No memory samples found for user "{user}" in last {args.days} day(s).')
        print('Hint: collect user history first, for example: bsample -u')
        return 0

    print(f'User: {user}')
    print(f'Cluster DB: {user_db_path}')
    _print_table(['Date', 'Jobs', 'AvgReq(MB)', 'AvgMax(MB)', 'PotentialWaste(%)'], summary_rows)

    overall_req = statistics.mean(all_requested) if all_requested else None
    overall_used = statistics.mean(all_used) if all_used else None
    overall_waste = statistics.mean(all_waste_ratio) * 100 if all_waste_ratio else None

    print('')
    print('Overall')
    print(f'  Avg requested memory : {_format_float(overall_req)} MB')
    print(f'  Avg max used memory  : {_format_float(overall_used)} MB')
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


def _handle_advise(args, _tool: str, _cluster: str) -> int:
    job_id = str(args.job)

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
    print(f'  Requested (MB)   : {_format_float(requested_mb)}')
    print(f'  Observed Max (MB): {_format_float(used_mb)}')

    if (requested_mb is None) and (used_mb is None):
        print('  Advice           : Not enough memory data yet. Run sampling first (bsample -j / -u / -m).')
        return 0

    if used_mb is None and requested_mb is not None:
        suggest_low = max(64.0, requested_mb * 0.8)
        suggest_high = max(suggest_low, requested_mb * 1.2)
        print(f'  Suggested rusage[mem] (MB): {suggest_low:.1f} ~ {suggest_high:.1f}')
        print('  Advice           : No observed max memory yet; range is based on current request.')
        return 0

    if requested_mb is None and used_mb is not None:
        suggest_low = max(64.0, used_mb * 1.2)
        suggest_high = max(suggest_low, used_mb * 1.5)
        print(f'  Suggested rusage[mem] (MB): {suggest_low:.1f} ~ {suggest_high:.1f}')
        print('  Advice           : Request value missing; range is estimated from observed memory.')
        return 0

    assert requested_mb is not None and used_mb is not None

    suggest_low = max(64.0, used_mb * 1.2)
    suggest_high = max(suggest_low, used_mb * 1.5)
    print(f'  Suggested rusage[mem] (MB): {suggest_low:.1f} ~ {suggest_high:.1f}')

    if requested_mb > used_mb * 1.8:
        print('  Advice           : Request appears over-sized; reduce memory to improve cluster efficiency.')
    elif requested_mb < used_mb * 1.05:
        print('  Advice           : Request is very tight; increase memory buffer to reduce EXIT risk.')
    else:
        print('  Advice           : Request looks reasonable.')

    return 0


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
