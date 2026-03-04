from __future__ import annotations

import datetime as dt
import importlib.util
import pathlib
import sys
import types
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LSFMON_PATH = ROOT / 'monitor' / 'bin' / 'lsfmon.py'
ADMIN_LSFMON_PATH = ROOT / 'lsfmon.py'


def _load_lsfmon_module(monkeypatch, lsid_info=('LSF', '10.1', 'clusterA', 'masterA')):
    monkeypatch.setenv('LSFMONITOR_INSTALL_PATH', str(ROOT))

    # Keep config loading deterministic for test runtime.
    monkeypatch.setattr(pathlib.Path, 'home', classmethod(lambda cls: ROOT))

    common_pkg = types.ModuleType('common')
    common_mod = types.ModuleType('common.common')
    common_mod.bprint = lambda *args, **kwargs: None

    common_lsf_mod = types.ModuleType('common.common_lsf')
    common_lsf_mod.get_lsid_info = lambda: lsid_info
    common_lsf_mod.get_bjobs_info = lambda *args, **kwargs: {}
    common_lsf_mod.get_bjobs_uf_info = lambda *args, **kwargs: {}

    common_sqlite3_mod = types.ModuleType('common.common_sqlite3')
    common_sqlite3_mod.connect_db_file = lambda *args, **kwargs: ('failed', None)
    common_sqlite3_mod.get_sql_table_list = lambda *args, **kwargs: []
    common_sqlite3_mod.get_sql_table_data = lambda *args, **kwargs: {}

    common_pkg.common = common_mod
    common_pkg.common_lsf = common_lsf_mod
    common_pkg.common_sqlite3 = common_sqlite3_mod

    conf_pkg = types.ModuleType('conf')
    config_mod = types.ModuleType('conf.config')
    config_mod.db_path = str(ROOT / 'db')
    conf_pkg.config = config_mod

    monkeypatch.setitem(sys.modules, 'common', common_pkg)
    monkeypatch.setitem(sys.modules, 'common.common', common_mod)
    monkeypatch.setitem(sys.modules, 'common.common_lsf', common_lsf_mod)
    monkeypatch.setitem(sys.modules, 'common.common_sqlite3', common_sqlite3_mod)
    monkeypatch.setitem(sys.modules, 'conf', conf_pkg)
    monkeypatch.setitem(sys.modules, 'conf.config', config_mod)

    module_name = f'_test_lsfmon_{uuid.uuid4().hex}'
    spec = importlib.util.spec_from_file_location(module_name, LSFMON_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)

    return module


def test_main_reports_clear_error_when_lsf_missing(monkeypatch, capsys):
    lsfmon = _load_lsfmon_module(monkeypatch, lsid_info=('', '', '', ''))

    rc = lsfmon.main(['jobs'])

    captured = capsys.readouterr()
    assert rc == 2
    assert 'No LSF/Volclava/Openlava environment detected' in captured.err


def test_jobs_prints_current_user_jobs(monkeypatch, capsys):
    lsfmon = _load_lsfmon_module(monkeypatch)
    monkeypatch.setattr(lsfmon.getpass, 'getuser', lambda: 'alice')

    monkeypatch.setattr(
        lsfmon.common_lsf,
        'get_bjobs_info',
        lambda **kwargs: {
            'JOBID': ['123'],
            'USER': ['alice'],
            'STAT': ['RUN'],
            'QUEUE': ['normal'],
            'EXEC_HOST': ['host01'],
            'JOB_NAME': ['demo_job'],
            'SUBMIT_TIME': ['Mar 03 11:20'],
        },
    )

    monkeypatch.setattr(
        lsfmon.common_lsf,
        'get_bjobs_uf_info',
        lambda **kwargs: {
            '123': {
                'processors_requested': '4',
                'rusage_mem': '2048',
                'mem': '1024',
                'cpu_time': '1800',
                'started_time': 'Mar 03 11:20:00',
            }
        },
    )

    rc = lsfmon.main(['jobs'])

    captured = capsys.readouterr()
    assert rc == 0
    assert 'User: alice' in captured.out
    assert 'demo_job' in captured.out
    assert 'idle_factor' in captured.out
    assert 'cpu_util(%)' in captured.out


def test_mem_reads_sampled_db_and_prints_summary(monkeypatch, tmp_path, capsys):
    lsfmon = _load_lsfmon_module(monkeypatch, lsid_info=('LSF', '10.1', 'clusterX', 'masterX'))
    monkeypatch.setattr(lsfmon.getpass, 'getuser', lambda: 'alice')
    monkeypatch.setattr(lsfmon, '_iter_recent_dates', lambda days: ['20260303'])

    lsfmon.config.db_path = str(tmp_path)
    user_db_dir = tmp_path / 'clusterX' / 'user'
    user_db_dir.mkdir(parents=True)
    db_file = user_db_dir / '20260303.db'
    db_file.write_text('', encoding='utf-8')

    class FakeConn:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    fake_conn = FakeConn()

    monkeypatch.setattr(lsfmon.common_sqlite3, 'connect_db_file', lambda *args, **kwargs: ('passed', fake_conn))
    monkeypatch.setattr(lsfmon.common_sqlite3, 'get_sql_table_list', lambda *args, **kwargs: ['user_alice'])
    monkeypatch.setattr(
        lsfmon.common_sqlite3,
        'get_sql_table_data',
        lambda *args, **kwargs: {
            'job': ['1001', '1002'],
            'status': ['DONE', 'DONE'],
            'queue': ['normal', 'normal'],
            'project': ['p1', 'p1'],
            'rusage_mem': ['1000', '2000'],
            'max_mem': ['500', '1000'],
        },
    )

    rc = lsfmon.main(['mem', '--days', '1'])

    captured = capsys.readouterr()
    assert rc == 0
    assert fake_conn.closed is True
    assert 'Overall' in captured.out
    assert 'PotentialWaste(%)' in captured.out


def test_advise_job_prints_memory_suggestion(monkeypatch, capsys):
    lsfmon = _load_lsfmon_module(monkeypatch)

    monkeypatch.setattr(
        lsfmon.common_lsf,
        'get_bjobs_uf_info',
        lambda **kwargs: {
            '123': {
                'user': 'alice',
                'status': 'RUN',
                'queue': 'normal',
                'rusage_mem': '2000',
                'max_mem': '1000',
                'mem': '1000',
            }
        },
    )

    rc = lsfmon.main(['advise', '--job', '123'])

    captured = capsys.readouterr()
    assert rc == 0
    assert 'Job: 123' in captured.out
    assert 'Suggested rusage[mem]' in captured.out


def test_engineer_entrypoint_can_delegate_admin_commands(monkeypatch, tmp_path, capsys):
    lsfmon = _load_lsfmon_module(monkeypatch)

    rc = lsfmon.main(['--db-path', str(tmp_path), 'mgmt', 'overview', '--range', '7d'])

    captured = capsys.readouterr()
    assert rc == 0
    assert '[lsfmon] Management overview' in captured.out
    assert 'No data found in sqlite database for this range.' in captured.out


def test_admin_entrypoint_rejects_engineer_commands(monkeypatch, capsys):
    _load_lsfmon_module(monkeypatch)

    module_name = f'_test_admin_lsfmon_{uuid.uuid4().hex}'
    spec = importlib.util.spec_from_file_location(module_name, ADMIN_LSFMON_PATH)
    admin_lsfmon = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(admin_lsfmon)

    import pytest

    with pytest.raises(SystemExit) as exc_info:
        admin_lsfmon.main(['--db-path', str(ROOT / 'db'), 'jobs'])

    captured = capsys.readouterr()
    assert exc_info.value.code == 2
    assert 'usage:' in captured.err
