import sqlite3
import time
from pathlib import Path

import lsfmon


def _prepare_queue_db(db_root: Path, now_ts: int):
    conn = sqlite3.connect(str(db_root / "queue.db"))
    try:
        conn.execute(
            """
            CREATE TABLE queue_ALL (
                sample_second INTEGER PRIMARY KEY,
                sample_time TEXT,
                TOTAL TEXT,
                NJOBS TEXT,
                PEND TEXT,
                RUN TEXT,
                SUSP TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO queue_ALL VALUES (?, ?, ?, ?, ?, ?, ?)",
            (now_ts - 3600, "20260303_100000", "200", "90", "30", "60", "0"),
        )
        conn.execute(
            "INSERT INTO queue_ALL VALUES (?, ?, ?, ?, ?, ?, ?)",
            (now_ts, "20260303_110000", "200", "120", "40", "80", "0"),
        )
        conn.commit()
    finally:
        conn.close()


def _prepare_utilization_day_db(db_root: Path):
    conn = sqlite3.connect(str(db_root / "utilization_day.db"))
    try:
        conn.execute(
            """
            CREATE TABLE utilization_hostA (
                sample_date TEXT PRIMARY KEY,
                slot TEXT,
                cpu TEXT,
                mem TEXT
            )
            """
        )
        conn.execute("INSERT INTO utilization_hostA VALUES ('20260302', '71.1', '65.2', '62.3')")
        conn.execute("INSERT INTO utilization_hostA VALUES ('20260303', '75.0', '69.0', '66.0')")
        conn.commit()
    finally:
        conn.close()


def _prepare_job_db(db_root: Path):
    job_dir = db_root / "job"
    job_dir.mkdir(parents=True, exist_ok=True)

    for day, statuses in {
        "20260302": ["DONE", "EXIT", "DONE"],
        "20260303": ["DONE", "DONE", "EXIT", "DONE"],
    }.items():
        conn = sqlite3.connect(str(job_dir / f"{day}.db"))
        try:
            conn.execute("CREATE TABLE job (job TEXT PRIMARY KEY, status TEXT)")
            for idx, status in enumerate(statuses):
                conn.execute(
                    "INSERT INTO job (job, status) VALUES (?, ?)",
                    (f"{day}_{idx}", status),
                )
            conn.commit()
        finally:
            conn.close()


def test_mgmt_overview_no_data_prints_friendly_message(tmp_path, capsys):
    rc = lsfmon.main(["--db-path", str(tmp_path), "mgmt", "overview", "--range", "7d"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "No data found" in out


def test_report_weekly_exports_csv_and_md(tmp_path, capsys):
    now_ts = int(time.time())
    _prepare_queue_db(tmp_path, now_ts)
    _prepare_utilization_day_db(tmp_path)
    _prepare_job_db(tmp_path)

    output_dir = tmp_path / "reports"
    rc = lsfmon.main(
        [
            "--db-path",
            str(tmp_path),
            "report",
            "weekly",
            "--range",
            "3650d",
            "--export",
            "csv,md",
            "--output-dir",
            str(output_dir),
        ]
    )

    out = capsys.readouterr().out
    assert rc == 0
    assert "Exported:" in out

    csv_files = list(output_dir.glob("lsfmon_weekly_*.csv"))
    md_files = list(output_dir.glob("lsfmon_weekly_*.md"))

    assert len(csv_files) == 1
    assert len(md_files) == 1

    csv_text = csv_files[0].read_text(encoding="utf-8")
    md_text = md_files[0].read_text(encoding="utf-8")

    assert "date,jobs_total,jobs_done,jobs_exit" in csv_text
    assert "# lsfmon Weekly Report" in md_text
