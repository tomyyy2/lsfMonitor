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

    daily_csv_files = list(output_dir.glob("lsfmon_weekly_[0-9]*.csv"))
    metrics_csv_files = list(output_dir.glob("lsfmon_weekly_metrics_*.csv"))
    anomaly_csv_files = list(output_dir.glob("lsfmon_weekly_anomaly_top_*.csv"))
    md_files = list(output_dir.glob("lsfmon_weekly_[0-9]*.md"))

    assert len(daily_csv_files) == 1
    assert len(metrics_csv_files) == 1
    assert len(anomaly_csv_files) == 1
    assert len(md_files) == 1

    daily_csv_text = daily_csv_files[0].read_text(encoding="utf-8")
    metrics_csv_text = metrics_csv_files[0].read_text(encoding="utf-8")
    anomaly_csv_text = anomaly_csv_files[0].read_text(encoding="utf-8")
    md_text = md_files[0].read_text(encoding="utf-8")

    assert "date,jobs_total,jobs_done,jobs_exit" in daily_csv_text
    assert "metric,value,note" in metrics_csv_text
    assert "overall_success_rate" in metrics_csv_text
    assert "rank,date,score,reasons,summary" in anomaly_csv_text
    assert "# lsfmon Weekly Report" in md_text
    assert "## 关键指标表 (Key Metrics)" in md_text
    assert "## 异常TOP项" in md_text


def _prepare_trend_range_data(db_root: Path, now_ts: int):
    queue_conn = sqlite3.connect(str(db_root / "queue.db"))
    try:
        queue_conn.execute(
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

        queue_rows = [
            (now_ts - 26 * 86400, "20260204_120000", "200", "90", "20", "70", "0"),
            (now_ts - 23 * 86400, "20260207_120000", "200", "100", "25", "70", "0"),
            (now_ts - 12 * 86400, "20260218_120000", "200", "130", "55", "75", "0"),
            (now_ts - 9 * 86400, "20260221_120000", "200", "150", "65", "75", "0"),
        ]

        queue_conn.executemany("INSERT INTO queue_ALL VALUES (?, ?, ?, ?, ?, ?, ?)", queue_rows)
        queue_conn.commit()
    finally:
        queue_conn.close()

    util_conn = sqlite3.connect(str(db_root / "utilization_day.db"))
    try:
        util_conn.execute(
            """
            CREATE TABLE utilization_hostA (
                sample_date TEXT PRIMARY KEY,
                slot TEXT,
                cpu TEXT,
                mem TEXT
            )
            """
        )

        util_conn.executemany(
            "INSERT INTO utilization_hostA VALUES (?, ?, ?, ?)",
            [
                ("20260204", "55", "50", "48"),
                ("20260207", "57", "52", "50"),
                ("20260218", "70", "68", "66"),
                ("20260221", "75", "72", "70"),
            ],
        )
        util_conn.commit()
    finally:
        util_conn.close()

    job_dir = db_root / "job"
    job_dir.mkdir(parents=True, exist_ok=True)

    for day, done_count, exit_count in [
        ("20260204", 9, 1),
        ("20260207", 8, 2),
        ("20260218", 7, 3),
        ("20260221", 6, 4),
    ]:
        conn = sqlite3.connect(str(job_dir / f"{day}.db"))
        try:
            conn.execute("CREATE TABLE job (job TEXT PRIMARY KEY, status TEXT)")

            index = 0
            for _ in range(done_count):
                conn.execute("INSERT INTO job (job, status) VALUES (?, ?)", (f"{day}_DONE_{index}", "DONE"))
                index += 1

            for _ in range(exit_count):
                conn.execute("INSERT INTO job (job, status) VALUES (?, ?)", (f"{day}_EXIT_{index}", "EXIT"))
                index += 1

            conn.commit()
        finally:
            conn.close()


def test_mgmt_trend_rejects_unsupported_range(tmp_path, capsys):
    rc = lsfmon.main(["--db-path", str(tmp_path), "mgmt", "trend", "--range", "14d"])
    out = capsys.readouterr().out

    assert rc == 1
    assert "Supported values: 7d, 30d, 90d" in out


def test_mgmt_trend_weekly_summary_and_csv_export(tmp_path, capsys):
    now_ts = int(time.time())
    _prepare_trend_range_data(tmp_path, now_ts)

    output_dir = tmp_path / "trend_reports"
    rc = lsfmon.main(
        [
            "--db-path",
            str(tmp_path),
            "mgmt",
            "trend",
            "--range",
            "30d",
            "--export",
            "csv",
            "--output-dir",
            str(output_dir),
        ]
    )

    out = capsys.readouterr().out
    assert rc == 0
    assert "aggregation=weekly" in out
    assert "queue_congestion" in out
    assert "utilization" in out
    assert "failure_rate" in out
    assert "Exported:" in out

    csv_files = list(output_dir.glob("lsfmon_mgmt_trend_30d_weekly_*.csv"))
    assert len(csv_files) == 1

    csv_text = csv_files[0].read_text(encoding="utf-8")
    assert "period,avg_njobs,avg_run,avg_pend,queue_congestion_pct" in csv_text
    assert "failure_rate_pct" in csv_text


def _prepare_job_db_for_overview_metrics(db_root: Path):
    job_dir = db_root / "job"
    job_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(job_dir / "20260303.db"))
    try:
        conn.execute(
            """
            CREATE TABLE job (
                job TEXT PRIMARY KEY,
                status TEXT,
                rusage_mem TEXT,
                max_mem TEXT,
                submitted_time TEXT,
                started_time TEXT
            )
            """
        )

        rows = [
            ("j1", "DONE", "100", "80", "2026-03-03 10:00:00", "2026-03-03 10:10:00"),
            ("j2", "DONE", "200", "100", "2026-03-03 10:00:00", "2026-03-03 10:30:00"),
            ("j3", "EXIT", "300", "360", "2026-03-03 09:00:00", ""),
            ("j4", "DONE", "0", "10", "2026-03-03 11:00:00", "2026-03-03 10:50:00"),
        ]

        conn.executemany("INSERT INTO job VALUES (?, ?, ?, ?, ?, ?)", rows)
        conn.commit()
    finally:
        conn.close()



def test_mgmt_overview_prints_core_metrics(tmp_path, capsys):
    now_ts = int(time.time())
    _prepare_queue_db(tmp_path, now_ts)
    _prepare_utilization_day_db(tmp_path)
    _prepare_job_db_for_overview_metrics(tmp_path)

    rc = lsfmon.main(["--db-path", str(tmp_path), "mgmt", "overview", "--range", "3650d"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "- Core metrics:" in out
    assert "Job success rate        : 75.0%" in out
    assert "Memory waste rate       : 20.0%" in out
    assert "Queue avg waiting time  : 20.0 min" in out
    assert "Slot/CPU/MEM utilization: 73.05% / 67.1% / 64.15%" in out



def test_mgmt_overview_degrades_core_metrics_when_optional_columns_missing(tmp_path, capsys):
    job_dir = tmp_path / "job"
    job_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(job_dir / "20260303.db"))
    try:
        conn.execute("CREATE TABLE job (job TEXT PRIMARY KEY, status TEXT)")
        conn.executemany(
            "INSERT INTO job (job, status) VALUES (?, ?)",
            [
                ("d1", "DONE"),
                ("d2", "DONE"),
                ("e1", "EXIT"),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    rc = lsfmon.main(["--db-path", str(tmp_path), "mgmt", "overview", "--range", "3650d"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "Job success rate        : 66.67%" in out
    assert "Memory waste rate       : N/A" in out
    assert "Queue avg waiting time  : N/A" in out
    assert "Slot/CPU/MEM utilization: N/A / N/A / N/A" in out


def test_mgmt_overview_wait_time_can_show_seconds(tmp_path, capsys):
    job_dir = tmp_path / "job"
    job_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(job_dir / "20260303.db"))
    try:
        conn.execute(
            """
            CREATE TABLE job (
                job TEXT PRIMARY KEY,
                status TEXT,
                rusage_mem TEXT,
                max_mem TEXT,
                submitted_time TEXT,
                started_time TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO job VALUES (?, ?, ?, ?, ?, ?)",
            ("j1", "DONE", "100", "80", "2026-03-03 10:00:00", "2026-03-03 10:00:30"),
        )
        conn.commit()
    finally:
        conn.close()

    rc = lsfmon.main(["--db-path", str(tmp_path), "mgmt", "overview", "--range", "3650d"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "Queue avg waiting time  : 30.0 sec" in out


def test_mgmt_overview_memory_waste_rate_clamps_negative_values(tmp_path, capsys):
    job_dir = tmp_path / "job"
    job_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(job_dir / "20260303.db"))
    try:
        conn.execute(
            """
            CREATE TABLE job (
                job TEXT PRIMARY KEY,
                status TEXT,
                rusage_mem TEXT,
                max_mem TEXT,
                submitted_time TEXT,
                started_time TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO job VALUES (?, ?, ?, ?, ?, ?)",
            ("j1", "DONE", "100", "150", "2026-03-03 10:00:00", "2026-03-03 10:10:00"),
        )
        conn.commit()
    finally:
        conn.close()

    rc = lsfmon.main(["--db-path", str(tmp_path), "mgmt", "overview", "--range", "3650d"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "Memory waste rate       : 0.0%" in out
