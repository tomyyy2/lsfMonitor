# -*- coding: utf-8 -*-

"""lsfmon admin CLI (MVP).

Provides:
- lsfmon mgmt overview --range <Nd|Nw|Nm|Ny>
- lsfmon mgmt trend --range <7d|30d|90d> [--export csv]
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
import importlib.util
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

_SUPPORTED_MGMT_TREND_RANGE_TO_DAYS = {
    "7d": 7,
    "30d": 30,
    "90d": 90,
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


def _normalize_mgmt_trend_range(range_text: str) -> Tuple[str, int]:
    normalized = str(range_text or "").strip().lower()

    if normalized not in _SUPPORTED_MGMT_TREND_RANGE_TO_DAYS:
        raise ValueError('Invalid --range for mgmt trend. Supported values: 7d, 30d, 90d.')

    return normalized, _SUPPORTED_MGMT_TREND_RANGE_TO_DAYS[normalized]


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


def _list_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    cursor = conn.cursor()
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    return [str(row[1]) for row in cursor.fetchall()]


def _parse_datetime_text(value: object, default_year: Optional[int] = None) -> Optional[dt.datetime]:
    if value is None:
        return None

    text = str(value).strip()
    if not text or text.upper() == "N/A":
        return None

    if re.match(r"^\d{8}_\d{6}$", text):
        try:
            return dt.datetime.strptime(text, "%Y%m%d_%H%M%S")
        except Exception:
            return None

    if re.match(r"^\d{14}$", text):
        try:
            return dt.datetime.strptime(text, "%Y%m%d%H%M%S")
        except Exception:
            return None

    if re.match(r"^\d{10}(\.\d+)?$", text):
        try:
            return dt.datetime.fromtimestamp(float(text))
        except Exception:
            return None

    if re.match(r"^\d{13}$", text):
        try:
            return dt.datetime.fromtimestamp(float(text) / 1000.0)
        except Exception:
            return None

    candidates = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%a %b %d %H:%M:%S",
        "%a %b %d %H:%M",
        "%b %d %H:%M:%S",
        "%b %d %H:%M",
    ]

    for fmt in candidates:
        try:
            parsed = dt.datetime.strptime(text, fmt)
        except Exception:
            continue

        if "%Y" not in fmt and default_year is not None:
            parsed = parsed.replace(year=default_year)

        return parsed

    return None


def _format_percent(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{round(value, 2)}%"


def _format_wait_seconds(value: Optional[float]) -> str:
    if value is None:
        return "N/A"

    if value < 60:
        return f"{round(value, 1)} sec"

    return f"{round(value / 60.0, 1)} min"


def _summarize_utilization(util_daily: Dict[str, Dict[str, float]]) -> Optional[Dict[str, float]]:
    if not util_daily:
        return None

    slot_values = [float(info.get("slot")) for info in util_daily.values() if info.get("slot") is not None]
    cpu_values = [float(info.get("cpu")) for info in util_daily.values() if info.get("cpu") is not None]
    mem_values = [float(info.get("mem")) for info in util_daily.values() if info.get("mem") is not None]

    if not slot_values and not cpu_values and not mem_values:
        return None

    result: Dict[str, float] = {}

    if slot_values:
        result["slot"] = round(sum(slot_values) / len(slot_values), 2)
    if cpu_values:
        result["cpu"] = round(sum(cpu_values) / len(cpu_values), 2)
    if mem_values:
        result["mem"] = round(sum(mem_values) / len(mem_values), 2)

    return result


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


def _fetch_job_daily(data_roots: Iterable[Path], start_date: str) -> Dict[str, Dict[str, float]]:
    daily: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {
            "total": 0.0,
            "done": 0.0,
            "exit": 0.0,
            "requested_mem_sum": 0.0,
            "mem_waste_sum": 0.0,
            "mem_pair_count": 0.0,
            "wait_seconds_sum": 0.0,
            "wait_count": 0.0,
        }
    )

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

                columns = set(_list_table_columns(conn, "job"))

                select_columns = ["status"]
                for optional_col in ["rusage_mem", "max_mem", "submitted_time", "started_time"]:
                    if optional_col in columns:
                        select_columns.append(optional_col)
                    else:
                        select_columns.append(f"NULL AS {optional_col}")

                cursor = conn.cursor()
                cursor.execute(f"SELECT {', '.join(select_columns)} FROM job")

                fallback_year = int(day[:4]) if re.match(r"^\d{8}$", day) else None

                for status, rusage_mem, max_mem, submitted_time, started_time in cursor.fetchall():
                    status_text = str(status or "").strip().upper()
                    daily[day]["total"] += 1.0

                    if status_text == "DONE":
                        daily[day]["done"] += 1.0
                    elif status_text == "EXIT":
                        daily[day]["exit"] += 1.0

                    request_mem = _safe_float(rusage_mem)
                    used_mem = _safe_float(max_mem)

                    if request_mem is not None and used_mem is not None and request_mem > 0:
                        daily[day]["requested_mem_sum"] += request_mem
                        daily[day]["mem_waste_sum"] += max(0.0, request_mem - used_mem)
                        daily[day]["mem_pair_count"] += 1.0

                    submitted_dt = _parse_datetime_text(submitted_time, default_year=fallback_year)
                    started_dt = _parse_datetime_text(started_time, default_year=fallback_year)

                    if submitted_dt and started_dt:
                        wait_seconds = (started_dt - submitted_dt).total_seconds()
                        if wait_seconds >= 0:
                            daily[day]["wait_seconds_sum"] += wait_seconds
                            daily[day]["wait_count"] += 1.0
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

    util_summary = _summarize_utilization(utilization_daily)

    job_daily = _fetch_job_daily(data_roots, start_date)
    job_summary: Optional[Dict[str, object]] = None

    if job_daily:
        total = int(sum(v.get("total", 0.0) for v in job_daily.values()))
        done = int(sum(v.get("done", 0.0) for v in job_daily.values()))
        exit_count = int(sum(v.get("exit", 0.0) for v in job_daily.values()))
        success_rate = round((done / total) * 100, 2) if total else None

        requested_mem_sum = sum(v.get("requested_mem_sum", 0.0) for v in job_daily.values())
        mem_waste_sum = sum(v.get("mem_waste_sum", 0.0) for v in job_daily.values())
        wait_seconds_sum = sum(v.get("wait_seconds_sum", 0.0) for v in job_daily.values())
        wait_count = sum(v.get("wait_count", 0.0) for v in job_daily.values())

        mem_waste_rate = round((mem_waste_sum / requested_mem_sum) * 100, 2) if requested_mem_sum > 0 else None
        avg_wait_seconds = round(wait_seconds_sum / wait_count, 2) if wait_count > 0 else None

        job_summary = {
            "total": total,
            "done": done,
            "exit": exit_count,
            "success_rate": success_rate,
            "mem_waste_rate": mem_waste_rate,
            "avg_wait_seconds": avg_wait_seconds,
        }

    return {
        "queue_latest": queue_latest,
        "util_latest": util_latest,
        "util_summary": util_summary,
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
    util_summary = payload.get("util_summary")
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
        success_text = _format_percent(job_summary.get("success_rate"))
        _friendly_print(
            "- Finished jobs: total={total} done={done} exit={exit} success={success}".format(
                total=job_summary.get("total", 0),
                done=job_summary.get("done", 0),
                exit=job_summary.get("exit", 0),
                success=success_text,
            )
        )
    else:
        _friendly_print("- Finished jobs: no data")

    _friendly_print("- Core metrics:")
    success_rate = _format_percent(job_summary.get("success_rate")) if isinstance(job_summary, dict) else "N/A"
    mem_waste_rate = _format_percent(job_summary.get("mem_waste_rate")) if isinstance(job_summary, dict) else "N/A"
    queue_wait = _format_wait_seconds(job_summary.get("avg_wait_seconds")) if isinstance(job_summary, dict) else "N/A"

    slot_util = "N/A"
    cpu_util = "N/A"
    mem_util = "N/A"

    if isinstance(util_summary, dict):
        slot_util = _format_percent(util_summary.get("slot"))
        cpu_util = _format_percent(util_summary.get("cpu"))
        mem_util = _format_percent(util_summary.get("mem"))

    _friendly_print(f"  Job success rate        : {success_rate}")
    _friendly_print(f"  Memory waste rate       : {mem_waste_rate}")
    _friendly_print(f"  Queue avg waiting time  : {queue_wait}")
    _friendly_print(f"  Slot/CPU/MEM utilization: {slot_util} / {cpu_util} / {mem_util}")


def _parse_mgmt_trend_export_targets(raw_export: str) -> List[str]:
    values = [item.strip().lower() for item in str(raw_export or "").split(",") if item.strip()]

    if not values or values == ["none"]:
        return []

    supported = {"csv"}
    unknown = [item for item in values if item not in supported]

    if unknown:
        raise ValueError(f"Unsupported mgmt trend export format(s): {', '.join(unknown)}. Supported: csv")

    # Keep order, de-dup.
    result: List[str] = []
    seen = set()

    for item in values:
        if item not in seen:
            result.append(item)
            seen.add(item)

    return result


def _build_mgmt_trend_daily_rows(payload: Dict[str, object]) -> List[Dict[str, object]]:
    queue_daily: Dict[str, Dict[str, float]] = payload.get("queue_daily", {})  # type: ignore[assignment]
    util_daily: Dict[str, Dict[str, float]] = payload.get("util_daily", {})  # type: ignore[assignment]
    job_daily: Dict[str, Dict[str, float]] = payload.get("job_daily", {})  # type: ignore[assignment]

    all_days = sorted(set(queue_daily.keys()) | set(util_daily.keys()) | set(job_daily.keys()))
    rows: List[Dict[str, object]] = []

    for day in all_days:
        queue_info = queue_daily.get(day, {})
        util_info = util_daily.get(day, {})
        job_info = job_daily.get(day, {})

        avg_njobs = _safe_float(queue_info.get("avg_njobs"))
        avg_run = _safe_float(queue_info.get("avg_run"))
        avg_pend = _safe_float(queue_info.get("avg_pend"))

        congestion_pct: Optional[float] = None
        if avg_run is not None and avg_pend is not None and (avg_run + avg_pend) > 0:
            congestion_pct = round((avg_pend / (avg_run + avg_pend)) * 100, 2)

        slot_util = _safe_float(util_info.get("slot"))
        cpu_util = _safe_float(util_info.get("cpu"))
        mem_util = _safe_float(util_info.get("mem"))

        util_values = [value for value in (slot_util, cpu_util, mem_util) if value is not None]
        utilization_pct = round(sum(util_values) / len(util_values), 2) if util_values else None

        jobs_total = int(job_info.get("total", 0)) if job_info else 0
        jobs_done = int(job_info.get("done", 0)) if job_info else 0
        jobs_exit = int(job_info.get("exit", 0)) if job_info else 0
        failure_rate_pct = round((jobs_exit / jobs_total) * 100, 2) if jobs_total > 0 else None

        rows.append(
            {
                "period_key": day,
                "period": _format_day(day),
                "avg_njobs": avg_njobs,
                "avg_run": avg_run,
                "avg_pend": avg_pend,
                "queue_congestion_pct": congestion_pct,
                "slot_util": slot_util,
                "cpu_util": cpu_util,
                "mem_util": mem_util,
                "utilization_pct": utilization_pct,
                "jobs_total": jobs_total,
                "jobs_done": jobs_done,
                "jobs_exit": jobs_exit,
                "failure_rate_pct": failure_rate_pct,
            }
        )

    return rows


def _week_bucket_label(period_key: str) -> Tuple[str, str]:
    day = dt.datetime.strptime(period_key, "%Y%m%d").date()
    week_start = day - dt.timedelta(days=day.weekday())
    week_end = week_start + dt.timedelta(days=6)

    return week_start.strftime("%Y%m%d"), f"{week_start:%Y-%m-%d}~{week_end:%Y-%m-%d}"


def _build_mgmt_trend_weekly_rows(daily_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    bucket: Dict[str, Dict[str, object]] = defaultdict(
        lambda: {
            "period": "",
            "avg_njobs_sum": 0.0,
            "avg_njobs_count": 0,
            "avg_run_sum": 0.0,
            "avg_run_count": 0,
            "avg_pend_sum": 0.0,
            "avg_pend_count": 0,
            "slot_util_sum": 0.0,
            "slot_util_count": 0,
            "cpu_util_sum": 0.0,
            "cpu_util_count": 0,
            "mem_util_sum": 0.0,
            "mem_util_count": 0,
            "jobs_total": 0,
            "jobs_done": 0,
            "jobs_exit": 0,
        }
    )

    for row in daily_rows:
        period_key = str(row.get("period_key") or "")
        if not re.match(r"^\d{8}$", period_key):
            continue

        week_key, week_label = _week_bucket_label(period_key)
        item = bucket[week_key]
        item["period"] = week_label

        for field in ("avg_njobs", "avg_run", "avg_pend", "slot_util", "cpu_util", "mem_util"):
            value = _safe_float(row.get(field))
            if value is None:
                continue

            item[f"{field}_sum"] = float(item.get(f"{field}_sum", 0.0)) + value
            item[f"{field}_count"] = int(item.get(f"{field}_count", 0)) + 1

        item["jobs_total"] = int(item.get("jobs_total", 0)) + int(row.get("jobs_total") or 0)
        item["jobs_done"] = int(item.get("jobs_done", 0)) + int(row.get("jobs_done") or 0)
        item["jobs_exit"] = int(item.get("jobs_exit", 0)) + int(row.get("jobs_exit") or 0)

    rows: List[Dict[str, object]] = []

    for week_key in sorted(bucket.keys()):
        item = bucket[week_key]

        def avg_value(field: str) -> Optional[float]:
            count = int(item.get(f"{field}_count", 0))
            if count <= 0:
                return None
            return round(float(item.get(f"{field}_sum", 0.0)) / count, 2)

        avg_njobs = avg_value("avg_njobs")
        avg_run = avg_value("avg_run")
        avg_pend = avg_value("avg_pend")
        slot_util = avg_value("slot_util")
        cpu_util = avg_value("cpu_util")
        mem_util = avg_value("mem_util")

        queue_congestion_pct: Optional[float] = None
        if avg_run is not None and avg_pend is not None and (avg_run + avg_pend) > 0:
            queue_congestion_pct = round((avg_pend / (avg_run + avg_pend)) * 100, 2)

        util_values = [value for value in (slot_util, cpu_util, mem_util) if value is not None]
        utilization_pct = round(sum(util_values) / len(util_values), 2) if util_values else None

        jobs_total = int(item.get("jobs_total", 0))
        jobs_done = int(item.get("jobs_done", 0))
        jobs_exit = int(item.get("jobs_exit", 0))
        failure_rate_pct = round((jobs_exit / jobs_total) * 100, 2) if jobs_total > 0 else None

        rows.append(
            {
                "period_key": week_key,
                "period": item.get("period", week_key),
                "avg_njobs": avg_njobs,
                "avg_run": avg_run,
                "avg_pend": avg_pend,
                "queue_congestion_pct": queue_congestion_pct,
                "slot_util": slot_util,
                "cpu_util": cpu_util,
                "mem_util": mem_util,
                "utilization_pct": utilization_pct,
                "jobs_total": jobs_total,
                "jobs_done": jobs_done,
                "jobs_exit": jobs_exit,
                "failure_rate_pct": failure_rate_pct,
            }
        )

    return rows


def _trend_change_summary(rows: List[Dict[str, object]], field: str) -> Optional[Tuple[float, float, float]]:
    values = [float(value) for value in (row.get(field) for row in rows) if value is not None]

    if not values:
        return None

    first = values[0]
    last = values[-1]
    delta = round(last - first, 2)

    return first, last, delta


def _format_pct(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{value:.2f}%"


def _format_pp(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}pp"


def _print_mgmt_trend(rows: List[Dict[str, object]], range_text: str, roots: List[Path], aggregation: str) -> None:
    _friendly_print(f"[lsfmon] Management trend (range={range_text}, aggregation={aggregation})")
    _friendly_print(f"Data roots: {', '.join(str(p) for p in roots)}")

    if not rows:
        _friendly_print("No trend data found in sqlite database for this range. Try running bsample first.")
        return

    queue_change = _trend_change_summary(rows, "queue_congestion_pct")
    util_change = _trend_change_summary(rows, "utilization_pct")
    failure_change = _trend_change_summary(rows, "failure_rate_pct")

    _friendly_print("- Trend summary:")

    if queue_change:
        _friendly_print(
            f"  queue_congestion: {_format_pct(queue_change[0])} -> {_format_pct(queue_change[1])} ({_format_pp(queue_change[2])})"
        )
    else:
        _friendly_print("  queue_congestion: N/A")

    if util_change:
        _friendly_print(
            f"  utilization: {_format_pct(util_change[0])} -> {_format_pct(util_change[1])} ({_format_pp(util_change[2])})"
        )
    else:
        _friendly_print("  utilization: N/A")

    if failure_change:
        _friendly_print(
            f"  failure_rate: {_format_pct(failure_change[0])} -> {_format_pct(failure_change[1])} ({_format_pp(failure_change[2])})"
        )
    else:
        _friendly_print("  failure_rate: N/A")

    _friendly_print(f"- {aggregation.capitalize()} points:")
    for row in rows:
        _friendly_print(
            "  {period}: congestion={congestion}, util={util}, failure={failure}, jobs={jobs_exit}/{jobs_total}".format(
                period=row.get("period", "unknown"),
                congestion=_format_pct(_safe_float(row.get("queue_congestion_pct"))),
                util=_format_pct(_safe_float(row.get("utilization_pct"))),
                failure=_format_pct(_safe_float(row.get("failure_rate_pct"))),
                jobs_exit=int(row.get("jobs_exit") or 0),
                jobs_total=int(row.get("jobs_total") or 0),
            )
        )


def _export_mgmt_trend_csv(
    rows: List[Dict[str, object]], output_dir: Path, stamp: str, range_text: str, aggregation: str
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"lsfmon_mgmt_trend_{range_text}_{aggregation}_{stamp}.csv"

    columns = [
        "period",
        "avg_njobs",
        "avg_run",
        "avg_pend",
        "queue_congestion_pct",
        "slot_util",
        "cpu_util",
        "mem_util",
        "utilization_pct",
        "jobs_total",
        "jobs_done",
        "jobs_exit",
        "failure_rate_pct",
    ]

    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()

        for row in rows:
            writer.writerow({column: "" if row.get(column) is None else row.get(column) for column in columns})

    return file_path


def _merge_weekly_rows(payload: Dict[str, object]) -> List[Dict[str, object]]:
    queue_daily: Dict[str, Dict[str, float]] = payload.get("queue_daily", {})  # type: ignore[assignment]
    util_daily: Dict[str, Dict[str, float]] = payload.get("util_daily", {})  # type: ignore[assignment]
    job_daily: Dict[str, Dict[str, float]] = payload.get("job_daily", {})  # type: ignore[assignment]

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


def _row_float(row: Dict[str, object], key: str) -> Optional[float]:
    return _safe_float(row.get(key))


def _average(values: Iterable[float]) -> Optional[float]:
    nums = list(values)
    if not nums:
        return None
    return round(sum(nums) / len(nums), 2)


def _format_metric_number(value: Optional[float], *, suffix: str = "") -> str:
    if value is None:
        return "N/A"

    if float(value).is_integer():
        return f"{int(value)}{suffix}"

    return f"{value:.2f}{suffix}"


def _build_key_metrics(rows: List[Dict[str, object]]) -> List[Dict[str, str]]:
    if not rows:
        return []

    days = [str(row.get("date", "")) for row in rows]

    jobs_total = sum(int(row.get("jobs_total", 0) or 0) for row in rows)
    jobs_done = sum(int(row.get("jobs_done", 0) or 0) for row in rows)
    jobs_exit = sum(int(row.get("jobs_exit", 0) or 0) for row in rows)
    overall_success = round((jobs_done / jobs_total) * 100, 2) if jobs_total else 0.0

    avg_njobs = _average(v for v in (_row_float(row, "avg_njobs") for row in rows) if v is not None)
    avg_run = _average(v for v in (_row_float(row, "avg_run") for row in rows) if v is not None)
    avg_pend = _average(v for v in (_row_float(row, "avg_pend") for row in rows) if v is not None)

    avg_slot = _average(v for v in (_row_float(row, "slot_util") for row in rows) if v is not None)
    avg_cpu = _average(v for v in (_row_float(row, "cpu_util") for row in rows) if v is not None)
    avg_mem = _average(v for v in (_row_float(row, "mem_util") for row in rows) if v is not None)

    pend_rows = [row for row in rows if _row_float(row, "avg_pend") is not None]
    peak_pend = max(pend_rows, key=lambda item: _row_float(item, "avg_pend") or -1) if pend_rows else None

    peak_exit = max(rows, key=lambda item: int(item.get("jobs_exit", 0) or 0))

    success_rows = [row for row in rows if int(row.get("jobs_total", 0) or 0) > 0]
    lowest_success = (
        min(success_rows, key=lambda item: float(item.get("success_rate", 0) or 0)) if success_rows else None
    )

    metrics = [
        {
            "metric": "report_days",
            "value": str(len(rows)),
            "note": f"{days[0]} ~ {days[-1]}",
        },
        {
            "metric": "jobs_total",
            "value": str(jobs_total),
            "note": "sum of all finished jobs in range",
        },
        {
            "metric": "jobs_done",
            "value": str(jobs_done),
            "note": "status == DONE",
        },
        {
            "metric": "jobs_exit",
            "value": str(jobs_exit),
            "note": "status == EXIT",
        },
        {
            "metric": "overall_success_rate",
            "value": _format_metric_number(overall_success, suffix="%"),
            "note": "jobs_done / jobs_total",
        },
        {
            "metric": "avg_queue_njobs",
            "value": _format_metric_number(avg_njobs),
            "note": "daily mean of avg_njobs",
        },
        {
            "metric": "avg_queue_run",
            "value": _format_metric_number(avg_run),
            "note": "daily mean of avg_run",
        },
        {
            "metric": "avg_queue_pend",
            "value": _format_metric_number(avg_pend),
            "note": "daily mean of avg_pend",
        },
        {
            "metric": "avg_slot_util",
            "value": _format_metric_number(avg_slot, suffix="%"),
            "note": "daily mean slot utilization",
        },
        {
            "metric": "avg_cpu_util",
            "value": _format_metric_number(avg_cpu, suffix="%"),
            "note": "daily mean cpu utilization",
        },
        {
            "metric": "avg_mem_util",
            "value": _format_metric_number(avg_mem, suffix="%"),
            "note": "daily mean mem utilization",
        },
        {
            "metric": "peak_pend_day",
            "value": (
                f"{peak_pend.get('date')} ({_format_metric_number(_row_float(peak_pend, 'avg_pend'))})"
                if peak_pend
                else "N/A"
            ),
            "note": "day with highest queue pending",
        },
        {
            "metric": "peak_exit_day",
            "value": f"{peak_exit.get('date')} ({int(peak_exit.get('jobs_exit', 0) or 0)})",
            "note": "day with highest EXIT jobs",
        },
        {
            "metric": "lowest_success_day",
            "value": (
                f"{lowest_success.get('date')} ({_format_metric_number(_safe_float(lowest_success.get('success_rate')), suffix='%')})"
                if lowest_success
                else "N/A"
            ),
            "note": "day with lowest success rate",
        },
    ]

    return metrics


def _build_anomaly_top(rows: List[Dict[str, object]], top_n: int = 5) -> List[Dict[str, object]]:
    anomalies: List[Dict[str, object]] = []

    for row in rows:
        jobs_total = int(row.get("jobs_total", 0) or 0)
        jobs_exit = int(row.get("jobs_exit", 0) or 0)
        success_rate = _safe_float(row.get("success_rate")) or 0.0
        avg_pend = _row_float(row, "avg_pend")
        avg_run = _row_float(row, "avg_run")

        score = 0.0
        reasons: List[str] = []
        details: List[str] = []

        if jobs_total > 0 and success_rate < 95:
            gap = 95 - success_rate
            score += gap
            reasons.append("low_success_rate")
            details.append(f"success={_format_metric_number(success_rate, suffix='%')} (<95%)")

        if jobs_total > 0 and jobs_exit > 0:
            exit_rate = (jobs_exit / jobs_total) * 100
            score += exit_rate * 1.2
            reasons.append("high_exit_rate")
            details.append(f"exit={jobs_exit}/{jobs_total} ({_format_metric_number(exit_rate, suffix='%')})")

        if avg_pend is not None and avg_run is not None and avg_pend > avg_run:
            backlog = avg_pend - avg_run
            score += backlog
            reasons.append("queue_backlog")
            details.append(f"pend={_format_metric_number(avg_pend)} > run={_format_metric_number(avg_run)}")

        for util_key, util_name in (("slot_util", "slot"), ("cpu_util", "cpu"), ("mem_util", "mem")):
            util = _row_float(row, util_key)
            if util is not None and util >= 90:
                score += (util - 90) * 0.5
                reasons.append(f"high_{util_name}_util")
                details.append(f"{util_name}={_format_metric_number(util, suffix='%')} (>=90%)")

        if score <= 0:
            continue

        anomalies.append(
            {
                "date": row.get("date", ""),
                "score": round(score, 2),
                "reasons": ",".join(reasons),
                "summary": "; ".join(details),
            }
        )

    ranked = sorted(anomalies, key=lambda item: (-float(item["score"]), str(item["date"])))[:top_n]
    for index, item in enumerate(ranked, start=1):
        item["rank"] = index

    return ranked


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


def _export_weekly_csv(
    rows: List[Dict[str, object]],
    key_metrics: List[Dict[str, str]],
    anomaly_top: List[Dict[str, object]],
    output_dir: Path,
    stamp: str,
) -> List[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    daily_file = output_dir / f"lsfmon_weekly_{stamp}.csv"
    metrics_file = output_dir / f"lsfmon_weekly_metrics_{stamp}.csv"
    anomaly_file = output_dir / f"lsfmon_weekly_anomaly_top_{stamp}.csv"

    daily_columns = [
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

    with daily_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=daily_columns)
        writer.writeheader()
        writer.writerows(rows)

    with metrics_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["metric", "value", "note"])
        writer.writeheader()
        writer.writerows(key_metrics)

    with anomaly_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["rank", "date", "score", "reasons", "summary"])
        writer.writeheader()
        writer.writerows(anomaly_top)

    return [daily_file, metrics_file, anomaly_file]


def _export_weekly_md(
    rows: List[Dict[str, object]],
    key_metrics: List[Dict[str, str]],
    anomaly_top: List[Dict[str, object]],
    output_dir: Path,
    stamp: str,
    range_text: str,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"lsfmon_weekly_{stamp}.md"

    lines = [
        "# lsfmon Weekly Report",
        "",
        f"- Generated at: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Range: {range_text}",
        "",
        "## 关键指标表 (Key Metrics)",
        "",
        "| Metric | Value | Note |",
        "|---|---:|---|",
    ]

    for item in key_metrics:
        lines.append("| {metric} | {value} | {note} |".format(**item))

    lines.extend(
        [
            "",
            "## 异常TOP项",
            "",
        ]
    )

    if anomaly_top:
        lines.extend(
            [
                "| Rank | Date | Score | Reasons | Summary |",
                "|---:|---|---:|---|---|",
            ]
        )
        for item in anomaly_top:
            lines.append(
                "| {rank} | {date} | {score} | {reasons} | {summary} |".format(
                    rank=item.get("rank", ""),
                    date=item.get("date", ""),
                    score=item.get("score", ""),
                    reasons=item.get("reasons", ""),
                    summary=item.get("summary", ""),
                )
            )
    else:
        lines.append("No anomalies detected in this range.")

    lines.extend(
        [
            "",
            "## Daily Details",
            "",
            "| Date | Jobs(total/done/exit) | Success% | Queue(avg_njobs/run/pend) | Util(slot/cpu/mem)% |",
            "|---|---:|---:|---:|---:|",
        ]
    )

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
        normalized_range, range_days = _normalize_mgmt_trend_range(args.range)
        _, start_ts, start_date = parse_range_to_start(normalized_range)
        export_targets = _parse_mgmt_trend_export_targets(args.export)
    except ValueError as error:
        _friendly_print(f"Error: {error}")
        return 1

    roots = _detect_data_roots(Path(args.db_path))
    payload = _overview_payload(roots, start_ts, start_date)
    daily_rows = _build_mgmt_trend_daily_rows(payload)

    aggregation = "daily" if range_days == 7 else "weekly"
    trend_rows = daily_rows if aggregation == "daily" else _build_mgmt_trend_weekly_rows(daily_rows)

    _print_mgmt_trend(trend_rows, normalized_range, roots, aggregation)

    if not trend_rows or "csv" not in export_targets:
        return 0

    output_dir = Path(args.output_dir).expanduser().resolve()
    stamp = dt.datetime.now().strftime("%Y%m%d")
    output_file = _export_mgmt_trend_csv(trend_rows, output_dir, stamp, normalized_range, aggregation)
    _friendly_print(f"Exported: {output_file}")

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

    key_metrics = _build_key_metrics(rows)
    anomaly_top = _build_anomaly_top(rows)

    output_dir = Path(args.output_dir).expanduser().resolve()
    stamp = dt.datetime.now().strftime("%Y%m%d")
    exported_files: List[Path] = []

    if "csv" in export_targets:
        exported_files.extend(_export_weekly_csv(rows, key_metrics, anomaly_top, output_dir, stamp))

    if "md" in export_targets:
        exported_files.append(_export_weekly_md(rows, key_metrics, anomaly_top, output_dir, stamp, args.range))

    _friendly_print(f"Rows: {len(rows)}")
    for path in exported_files:
        _friendly_print(f"Exported: {path}")

    return 0


_ENGINEER_COMMANDS = {"my", "advise"}


def _first_top_level_command(argv: List[str]) -> Optional[str]:
    index = 0

    while index < len(argv):
        token = str(argv[index]).strip()

        if token == "--db-path":
            index += 2
            continue

        if token.startswith("--db-path="):
            index += 1
            continue

        if token.startswith("-"):
            index += 1
            continue

        return token

    return None


def _delegate_to_engineer_cli(argv: List[str]) -> int:
    engineer_cli = Path(__file__).resolve().parent / "monitor" / "bin" / "lsfmon.py"

    if not engineer_cli.exists():
        _friendly_print(f"Error: engineer CLI entry not found: {engineer_cli}")
        return 1

    spec = importlib.util.spec_from_file_location("_lsfmon_engineer_cli", engineer_cli)
    if spec is None or spec.loader is None:
        _friendly_print(f"Error: failed to load engineer CLI entry: {engineer_cli}")
        return 1

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    delegate_main = getattr(module, "main", None)
    if not callable(delegate_main):
        _friendly_print(f"Error: engineer CLI entry has no callable main(): {engineer_cli}")
        return 1

    return int(delegate_main(list(argv)))


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
    mgmt_trend.add_argument("--range", default="30d", help="Trend range, supported: 7d, 30d, 90d")
    mgmt_trend.add_argument("--export", default="none", help="Export format, currently supports: csv")
    mgmt_trend.add_argument("--output-dir", default=".", help="Directory for exported trend files")
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
    argv_list = list(argv) if argv is not None else list(sys.argv[1:])
    first_command = _first_top_level_command(argv_list)

    if first_command in _ENGINEER_COMMANDS:
        return _delegate_to_engineer_cli(argv_list)

    parser = build_parser()
    args = parser.parse_args(argv_list)

    handler = getattr(args, "handler", None)
    if not handler:
        parser.print_help()
        return 1

    return int(handler(args))


if __name__ == "__main__":
    sys.exit(main())
