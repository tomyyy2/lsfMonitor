# -*- coding: utf-8 -*-

"""lsfmon admin CLI (MVP).

Provides:
- lsfmon mgmt overview --range <Nd|Nw|Nm|Ny>
- lsfmon mgmt trend --range <Nd|Nw|Nm|Ny>
- lsfmon report weekly --export csv,md

This implementation intentionally focuses on a runnable MVP:
- reads existing sqlite data produced by bsample
- prints concise summaries
- handles empty/no-data scenarios with friendly messages
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


_RANGE_UNIT_TO_DAYS = {
    "d": 1,
    "w": 7,
    "m": 30,
    "y": 365,
}


def _friendly_print(message: str) -> None:
    print(message)


def resolve_default_db_path() -> Path:
    """Resolve default db path in a tolerant way."""
    env_db = os.environ.get("LSFMON_DB_PATH")
    if env_db:
        return Path(env_db).expanduser().resolve()

    install_path = os.environ.get("LSFMONITOR_INSTALL_PATH")
    if install_path:
        config_path = Path(install_path) / "monitor" / "conf" / "config.py"
        if config_path.exists():
            config_ns: Dict[str, object] = {}
            try:
                exec(config_path.read_text(encoding="utf-8"), config_ns)
            except Exception:
                pass
            else:
                db_path = config_ns.get("db_path")
                if isinstance(db_path, str) and db_path.strip():
                    return Path(db_path).expanduser().resolve()

    return (Path.cwd() / "db").resolve()


def parse_range_to_start(range_text: str, now: Optional[dt.datetime] = None) -> Tuple[dt.datetime, int, str]:
    """Parse range text like 7d/4w/3m/1y and return start datetime/epoch/date-string."""
    now = now or dt.datetime.now()
    match = re.match(r"^\s*(\d+)\s*([dwmy])\s*$", range_text.lower())

    if not match:
        raise ValueError('Invalid --range format, expected like "7d", "4w", "3m", "1y".')

    amount = int(match.group(1))
    unit = match.group(2)

    if amount <= 0:
        raise ValueError("--range must be greater than 0.")

    start_dt = now - dt.timedelta(days=amount * _RANGE_UNIT_TO_DAYS[unit])
    return start_dt, int(start_dt.timestamp()), start_dt.strftime("%Y%m%d")


def _safe_float(value: object) -> Optional[float]:
    if value is None:
        return None

    text = str(value).strip()
    if not text or text.upper() == "N/A":
        return None

    text = text.replace("%", "")

    try:
        return float(text)
    except Exception:
        return None


def _safe_int(value: object) -> Optional[int]:
    parsed = _safe_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _detect_data_roots(db_path: Path) -> List[Path]:
    """Detect candidate db roots.

    Supports layouts like:
    - <db>/queue.db
    - <db>/monitor/queue.db
    - <db>/<cluster>/queue.db
    """

    def _looks_like_root(path: Path) -> bool:
        return (path / "queue.db").exists() or (path / "utilization_day.db").exists() or (path / "job").exists()

    base = db_path.expanduser().resolve()
    candidates: List[Path] = []

    if _looks_like_root(base):
        candidates.append(base)

    monitor_dir = base / "monitor"
    if _looks_like_root(monitor_dir):
        candidates.append(monitor_dir)

    if base.exists() and base.is_dir():
        for child in base.iterdir():
            if child.is_dir() and _looks_like_root(child):
                candidates.append(child)

    # De-duplicate while preserving order.
    unique: List[Path] = []
    seen = set()

    for c in candidates:
        if c not in seen:
            unique.append(c)
            seen.add(c)

    return unique or [base]


def _list_tables(conn: sqlite3.Connection, like_prefix: Optional[str] = None) -> List[str]:
    cursor = conn.cursor()

    if like_prefix:
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ? ORDER BY name",
            (f"{like_prefix}%",),
        )
    else:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")

    return [row[0] for row in cursor.fetchall()]


def _fetch_queue_rows(data_roots: Iterable[Path], start_ts: int) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []

    for root in data_roots:
        queue_db = root / "queue.db"
        if not queue_db.exists():
            continue

        conn = sqlite3.connect(str(queue_db))
        try:
            tables = _list_tables(conn, like_prefix="queue_")
            if not tables:
                continue

            target_table = "queue_ALL" if "queue_ALL" in tables else tables[0]
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT sample_second, sample_time, TOTAL, NJOBS, PEND, RUN, SUSP
                FROM "{target_table}"
                WHERE CAST(sample_second AS INTEGER) >= ?
                ORDER BY CAST(sample_second AS INTEGER) ASC
                """,
                (start_ts,),
            )

            for sample_second, sample_time, total, njobs, pend, run, susp in cursor.fetchall():
                rows.append(
                    {
                        "sample_second": _safe_int(sample_second),
                        "sample_time": str(sample_time or ""),
                        "total": _safe_int(total),
                        "njobs": _safe_int(njobs),
                        "pend": _safe_int(pend),
                        "run": _safe_int(run),
                        "susp": _safe_int(susp),
                    }
                )
        finally:
            conn.close()

    return rows


def _extract_date_from_row(sample_second: Optional[int], sample_time: str) -> str:
    # Preferred format from bsample: YYYYMMDD_HHMMSS
    if sample_time and re.match(r"^\d{8}_\d{6}$", sample_time):
        return sample_time.split("_")[0]

    # Try first 8 continuous digits.
    if sample_time:
        match = re.search(r"(\d{8})", sample_time)
        if match:
            return match.group(1)

    if sample_second:
        return dt.datetime.fromtimestamp(sample_second).strftime("%Y%m%d")

    return "unknown"


def _build_queue_daily(rows: Iterable[Dict[str, object]]) -> Dict[str, Dict[str, Optional[float]]]:
    bucket: Dict[str, Dict[str, float]] = defaultdict(lambda: {"pend_sum": 0.0, "run_sum": 0.0, "njobs_sum": 0.0, "count": 0.0})

    for row in rows:
        day = _extract_date_from_row(row.get("sample_second"), str(row.get("sample_time") or ""))
        pend = row.get("pend")
        run = row.get("run")
        njobs = row.get("njobs")

        if any(v is not None for v in (pend, run, njobs)):
            bucket[day]["count"] += 1.0
            bucket[day]["pend_sum"] += float(pend or 0)
            bucket[day]["run_sum"] += float(run or 0)
            bucket[day]["njobs_sum"] += float(njobs or 0)

    daily: Dict[str, Dict[str, Optional[float]]] = {}

    for day, info in bucket.items():
        count = info["count"]
        if count <= 0:
            continue

        daily[day] = {
            "avg_pend": round(info["pend_sum"] / count, 2),
            "avg_run": round(info["run_sum"] / count, 2),
            "avg_njobs": round(info["njobs_sum"] / count, 2),
        }

    return daily


def _fetch_utilization_daily(data_roots: Iterable[Path], start_date: str) -> Dict[str, Dict[str, float]]:
    bucket: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {"slot_sum": 0.0, "cpu_sum": 0.0, "mem_sum": 0.0, "count": 0.0}
    )

    for root in data_roots:
        db_file = root / "utilization_day.db"
        if not db_file.exists():
            continue

        conn = sqlite3.connect(str(db_file))
        try:
            tables = _list_tables(conn, like_prefix="utilization_")
            cursor = conn.cursor()

            for table in tables:
                cursor.execute(
                    f"""
                    SELECT sample_date, slot, cpu, mem
                    FROM "{table}"
                    WHERE sample_date >= ?
                    ORDER BY sample_date ASC
                    """,
                    (start_date,),
                )

                for sample_date, slot, cpu, mem in cursor.fetchall():
                    day = str(sample_date)
                    slot_v = _safe_float(slot)
                    cpu_v = _safe_float(cpu)
                    mem_v = _safe_float(mem)

                    if slot_v is None and cpu_v is None and mem_v is None:
                        continue

                    bucket[day]["count"] += 1.0
                    bucket[day]["slot_sum"] += slot_v or 0.0
                    bucket[day]["cpu_sum"] += cpu_v or 0.0
                    bucket[day]["mem_sum"] += mem_v or 0.0
        finally:
            conn.close()

    daily: Dict[str, Dict[str, float]] = {}

    for day, info in bucket.items():
        count = info["count"]
        if count <= 0:
            continue

        daily[day] = {
            "slot": round(info["slot_sum"] / count, 2),
            "cpu": round(info["cpu_sum"] / count, 2),
            "mem": round(info["mem_sum"] / count, 2),
        }

    return daily


def _fetch_job_daily(data_roots: Iterable[Path], start_date: str) -> Dict[str, Dict[str, int]]:
    daily: Dict[str, Dict[str, int]] = defaultdict(lambda: {"total": 0, "done": 0, "exit": 0})

    for root in data_roots:
        job_dir = root / "job"
        if not job_dir.exists() or not job_dir.is_dir():
            continue

        for db_file in sorted(job_dir.glob("*.db")):
            match = re.match(r"^(\d{8})\.db$", db_file.name)
            if not match:
                continue

            day = match.group(1)
            if day < start_date:
                continue

            conn = sqlite3.connect(str(db_file))
            try:
                tables = _list_tables(conn)
                if "job" not in tables:
                    continue

                cursor = conn.cursor()
                cursor.execute("SELECT status, COUNT(*) FROM job GROUP BY status")
                rows = cursor.fetchall()

                for status, count in rows:
                    status_text = str(status or "").strip().upper()
                    c = int(count or 0)
                    daily[day]["total"] += c

                    if status_text == "DONE":
                        daily[day]["done"] += c
                    elif status_text == "EXIT":
                        daily[day]["exit"] += c
            finally:
                conn.close()

    return dict(daily)


def _overview_payload(data_roots: Iterable[Path], start_ts: int, start_date: str) -> Dict[str, object]:
    queue_rows = _fetch_queue_rows(data_roots, start_ts)
    queue_latest: Optional[Dict[str, object]] = None

    if queue_rows:
        queue_latest = max(
            queue_rows,
            key=lambda item: item.get("sample_second") if item.get("sample_second") is not None else -1,
        )

    utilization_daily = _fetch_utilization_daily(data_roots, start_date)
    util_latest: Optional[Tuple[str, Dict[str, float]]] = None
    if utilization_daily:
        latest_day = sorted(utilization_daily.keys())[-1]
        util_latest = (latest_day, utilization_daily[latest_day])

    job_daily = _fetch_job_daily(data_roots, start_date)
    job_summary: Optional[Dict[str, object]] = None

    if job_daily:
        total = sum(v["total"] for v in job_daily.values())
        done = sum(v["done"] for v in job_daily.values())
        exit_count = sum(v["exit"] for v in job_daily.values())
        success_rate = round((done / total) * 100, 2) if total else 0.0
        job_summary = {
            "total": total,
            "done": done,
            "exit": exit_count,
            "success_rate": success_rate,
        }

    return {
        "queue_latest": queue_latest,
        "util_latest": util_latest,
        "job_summary": job_summary,
        "queue_daily": _build_queue_daily(queue_rows),
        "util_daily": utilization_daily,
        "job_daily": job_daily,
    }


def _format_day(day_text: str) -> str:
    if re.match(r"^\d{8}$", day_text):
        return f"{day_text[0:4]}-{day_text[4:6]}-{day_text[6:8]}"
    return day_text


def _print_overview(payload: Dict[str, object], range_text: str, roots: List[Path]) -> None:
    _friendly_print(f"[lsfmon] Management overview (range={range_text})")
    _friendly_print(f"Data roots: {', '.join(str(p) for p in roots)}")

    queue_latest = payload.get("queue_latest")
    util_latest = payload.get("util_latest")
    job_summary = payload.get("job_summary")

    if not queue_latest and not util_latest and not job_summary:
        _friendly_print("No data found in sqlite database for this range. Try running bsample first.")
        return

    if queue_latest:
        _friendly_print("- Queue snapshot:")
        _friendly_print(
            "  sample={sample} total={total} njobs={njobs} run={run} pend={pend} susp={susp}".format(
                sample=queue_latest.get("sample_time") or "(unknown)",
                total=queue_latest.get("total", "N/A"),
                njobs=queue_latest.get("njobs", "N/A"),
                run=queue_latest.get("run", "N/A"),
                pend=queue_latest.get("pend", "N/A"),
                susp=queue_latest.get("susp", "N/A"),
            )
        )
    else:
        _friendly_print("- Queue snapshot: no data")

    if util_latest:
        latest_day, util = util_latest
        _friendly_print(
            f"- Utilization(day): { _format_day(latest_day) } slot={util['slot']}% cpu={util['cpu']}% mem={util['mem']}%"
        )
    else:
        _friendly_print("- Utilization(day): no data")

    if job_summary:
        _friendly_print(
            "- Finished jobs: total={total} done={done} exit={exit} success={success_rate}%".format(
                **job_summary
            )
        )
    else:
        _friendly_print("- Finished jobs: no data")


def _print_trend(payload: Dict[str, object], range_text: str, roots: List[Path]) -> None:
    _friendly_print(f"[lsfmon] Management trend (range={range_text})")
    _friendly_print(f"Data roots: {', '.join(str(p) for p in roots)}")

    queue_daily: Dict[str, Dict[str, float]] = payload.get("queue_daily", {})  # type: ignore[assignment]
    util_daily: Dict[str, Dict[str, float]] = payload.get("util_daily", {})  # type: ignore[assignment]
    job_daily: Dict[str, Dict[str, int]] = payload.get("job_daily", {})  # type: ignore[assignment]

    if not queue_daily and not util_daily and not job_daily:
        _friendly_print("No trend data found in sqlite database for this range. Try running bsample first.")
        return

    if queue_daily:
        _friendly_print("- Queue daily trend (avg_njobs / avg_run / avg_pend):")
        for day in sorted(queue_daily.keys()):
            info = queue_daily[day]
            _friendly_print(
                f"  {_format_day(day)} : {info['avg_njobs']} / {info['avg_run']} / {info['avg_pend']}"
            )
    else:
        _friendly_print("- Queue daily trend: no data")

    if util_daily:
        _friendly_print("- Utilization daily trend (slot% / cpu% / mem%):")
        for day in sorted(util_daily.keys()):
            info = util_daily[day]
            _friendly_print(f"  {_format_day(day)} : {info['slot']} / {info['cpu']} / {info['mem']}")
    else:
        _friendly_print("- Utilization daily trend: no data")

    if job_daily:
        _friendly_print("- Job daily trend (total / done / exit / success%):")
        for day in sorted(job_daily.keys()):
            info = job_daily[day]
            total = info.get("total", 0)
            done = info.get("done", 0)
            exit_count = info.get("exit", 0)
            success_rate = round((done / total) * 100, 2) if total else 0.0
            _friendly_print(f"  {_format_day(day)} : {total} / {done} / {exit_count} / {success_rate}")
    else:
        _friendly_print("- Job daily trend: no data")


def _merge_weekly_rows(payload: Dict[str, object]) -> List[Dict[str, object]]:
    queue_daily: Dict[str, Dict[str, float]] = payload.get("queue_daily", {})  # type: ignore[assignment]
    util_daily: Dict[str, Dict[str, float]] = payload.get("util_daily", {})  # type: ignore[assignment]
    job_daily: Dict[str, Dict[str, int]] = payload.get("job_daily", {})  # type: ignore[assignment]

    all_days = sorted(set(queue_daily.keys()) | set(util_daily.keys()) | set(job_daily.keys()))
    rows: List[Dict[str, object]] = []

    for day in all_days:
        q = queue_daily.get(day, {})
        u = util_daily.get(day, {})
        j = job_daily.get(day, {})

        total = int(j.get("total", 0)) if j else 0
        done = int(j.get("done", 0)) if j else 0
        exit_count = int(j.get("exit", 0)) if j else 0
        success_rate = round((done / total) * 100, 2) if total else 0.0

        rows.append(
            {
                "date": _format_day(day),
                "jobs_total": total,
                "jobs_done": done,
                "jobs_exit": exit_count,
                "success_rate": success_rate,
                "avg_njobs": q.get("avg_njobs", ""),
                "avg_run": q.get("avg_run", ""),
                "avg_pend": q.get("avg_pend", ""),
                "slot_util": u.get("slot", ""),
                "cpu_util": u.get("cpu", ""),
                "mem_util": u.get("mem", ""),
            }
        )

    return rows


def _parse_export_targets(raw_export: str) -> List[str]:
    values = [item.strip().lower() for item in raw_export.split(",") if item.strip()]

    if not values:
        return ["md"]

    supported = {"csv", "md"}
    unknown = [item for item in values if item not in supported]

    if unknown:
        raise ValueError(f"Unsupported export format(s): {', '.join(unknown)}. Supported: csv, md")

    # Keep order, de-dup.
    result: List[str] = []
    seen = set()

    for item in values:
        if item not in seen:
            result.append(item)
            seen.add(item)

    return result


def _export_weekly_csv(rows: List[Dict[str, object]], output_dir: Path, stamp: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"lsfmon_weekly_{stamp}.csv"

    columns = [
        "date",
        "jobs_total",
        "jobs_done",
        "jobs_exit",
        "success_rate",
        "avg_njobs",
        "avg_run",
        "avg_pend",
        "slot_util",
        "cpu_util",
        "mem_util",
    ]

    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    return file_path


def _export_weekly_md(rows: List[Dict[str, object]], output_dir: Path, stamp: str, range_text: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"lsfmon_weekly_{stamp}.md"

    lines = [
        "# lsfmon Weekly Report",
        "",
        f"- Generated at: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Range: {range_text}",
        "",
        "| Date | Jobs(total/done/exit) | Success% | Queue(avg_njobs/run/pend) | Util(slot/cpu/mem)% |",
        "|---|---:|---:|---:|---:|",
    ]

    for row in rows:
        lines.append(
            "| {date} | {jobs_total}/{jobs_done}/{jobs_exit} | {success_rate} | {avg_njobs}/{avg_run}/{avg_pend} | {slot_util}/{cpu_util}/{mem_util} |".format(
                **row
            )
        )

    lines.append("")
    file_path.write_text("\n".join(lines), encoding="utf-8")
    return file_path


def _cmd_mgmt_overview(args: argparse.Namespace) -> int:
    try:
        _, start_ts, start_date = parse_range_to_start(args.range)
    except ValueError as error:
        _friendly_print(f"Error: {error}")
        return 1

    roots = _detect_data_roots(Path(args.db_path))
    payload = _overview_payload(roots, start_ts, start_date)
    _print_overview(payload, args.range, roots)
    return 0


def _cmd_mgmt_trend(args: argparse.Namespace) -> int:
    try:
        _, start_ts, start_date = parse_range_to_start(args.range)
    except ValueError as error:
        _friendly_print(f"Error: {error}")
        return 1

    roots = _detect_data_roots(Path(args.db_path))
    payload = _overview_payload(roots, start_ts, start_date)
    _print_trend(payload, args.range, roots)
    return 0


def _cmd_report_weekly(args: argparse.Namespace) -> int:
    try:
        _, start_ts, start_date = parse_range_to_start(args.range)
        export_targets = _parse_export_targets(args.export)
    except ValueError as error:
        _friendly_print(f"Error: {error}")
        return 1

    roots = _detect_data_roots(Path(args.db_path))
    payload = _overview_payload(roots, start_ts, start_date)
    rows = _merge_weekly_rows(payload)

    _friendly_print(f"[lsfmon] Weekly report (range={args.range})")
    _friendly_print(f"Data roots: {', '.join(str(p) for p in roots)}")

    if not rows:
        _friendly_print("No weekly data found in sqlite database for this range. Try running bsample first.")
        return 0

    output_dir = Path(args.output_dir).expanduser().resolve()
    stamp = dt.datetime.now().strftime("%Y%m%d")
    exported_files: List[Path] = []

    if "csv" in export_targets:
        exported_files.append(_export_weekly_csv(rows, output_dir, stamp))

    if "md" in export_targets:
        exported_files.append(_export_weekly_md(rows, output_dir, stamp, args.range))

    _friendly_print(f"Rows: {len(rows)}")
    for path in exported_files:
        _friendly_print(f"Exported: {path}")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lsfmon")
    parser.add_argument(
        "--db-path",
        default=str(resolve_default_db_path()),
        help="Path to sqlite database root. Default: auto-detect from env/config/current db directory.",
    )

    subparsers = parser.add_subparsers(dest="group")

    # lsfmon mgmt ...
    mgmt_parser = subparsers.add_parser("mgmt", help="Management commands")
    mgmt_subparsers = mgmt_parser.add_subparsers(dest="mgmt_cmd")

    mgmt_overview = mgmt_subparsers.add_parser("overview", help="Show management overview")
    mgmt_overview.add_argument("--range", default="7d", help="Time range, e.g. 7d, 4w, 3m, 1y")
    mgmt_overview.set_defaults(handler=_cmd_mgmt_overview)

    mgmt_trend = mgmt_subparsers.add_parser("trend", help="Show management trend")
    mgmt_trend.add_argument("--range", default="90d", help="Time range, e.g. 7d, 4w, 3m, 1y")
    mgmt_trend.set_defaults(handler=_cmd_mgmt_trend)

    # lsfmon report ...
    report_parser = subparsers.add_parser("report", help="Report commands")
    report_subparsers = report_parser.add_subparsers(dest="report_cmd")

    report_weekly = report_subparsers.add_parser("weekly", help="Generate weekly report")
    report_weekly.add_argument("--range", default="7d", help="Time range, default weekly = 7d")
    report_weekly.add_argument("--export", default="md", help="Export formats, e.g. csv,md")
    report_weekly.add_argument("--output-dir", default=".", help="Directory for exported files")
    report_weekly.set_defaults(handler=_cmd_report_weekly)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    handler = getattr(args, "handler", None)
    if not handler:
        parser.print_help()
        return 1

    return int(handler(args))


if __name__ == "__main__":
    sys.exit(main())
