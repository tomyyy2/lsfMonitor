from __future__ import annotations

import importlib.util
import sys
import types
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BSAMPLE_PATH = ROOT / 'monitor' / 'bin' / 'bsample.py'


def _load_bsample_module(monkeypatch):
    """Load monitor/bin/bsample.py with lightweight dependency stubs."""
    monkeypatch.setenv('LSFMONITOR_INSTALL_PATH', str(ROOT))
    monkeypatch.setenv('HOME', str(ROOT))

    common_pkg = types.ModuleType('common')
    common_mod = types.ModuleType('common.common')
    common_mod.bprint = lambda *args, **kwargs: None
    common_mod.create_dir = lambda *args, **kwargs: None

    common_lsf_mod = types.ModuleType('common.common_lsf')
    common_lsf_mod.get_lsid_info = lambda: ('LSF', '10.1', 'cluster', 'master')

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

    module_name = f'_test_bsample_{uuid.uuid4().hex}'
    spec = importlib.util.spec_from_file_location(module_name, BSAMPLE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)

    return module


def _build_sampling_instance(bsample):
    sampling = bsample.Sampling.__new__(bsample.Sampling)
    sampling.cleanup = False
    sampling.job_sampling = False
    sampling.job_mem_sampling = False
    sampling.queue_sampling = False
    sampling.host_sampling = False
    sampling.load_sampling = False
    sampling.user_sampling = False
    sampling.utilization_sampling = False
    sampling.utilization_day_sampling = False
    sampling.sample_job_info = lambda: None
    sampling.sample_job_mem_info = lambda: None
    sampling.sample_queue_info = lambda: None
    sampling.sample_host_info = lambda: None
    sampling.sample_load_info = lambda: None
    sampling.sample_user_info = lambda: None
    sampling.sample_utilization_info = lambda: None
    sampling.count_utilization_day_info = lambda: None
    return sampling


def test_sampling_joins_every_started_process(monkeypatch):
    bsample = _load_bsample_module(monkeypatch)

    class FakeProcess:
        instances = []

        def __init__(self, target):
            self.target = target
            self.started = False
            self.joined = False
            self.exitcode = 0
            self.pid = 1000 + len(FakeProcess.instances)
            FakeProcess.instances.append(self)

        def start(self):
            self.started = True

        def join(self):
            self.joined = True

    monkeypatch.setattr(bsample, 'Process', FakeProcess)

    sampling = _build_sampling_instance(bsample)
    sampling.job_sampling = True
    sampling.host_sampling = True
    sampling.utilization_sampling = True

    sampling.sampling()

    assert len(FakeProcess.instances) == 3
    assert all(process.started for process in FakeProcess.instances)
    assert all(process.joined for process in FakeProcess.instances)


def test_sampling_exits_when_any_subprocess_failed(monkeypatch):
    bsample = _load_bsample_module(monkeypatch)

    class FakeProcess:
        instances = []

        def __init__(self, target):
            self.target = target
            self.started = False
            self.joined = False
            self.pid = 2000 + len(FakeProcess.instances)
            # Mark the 2nd process as failed.
            self.exitcode = 0 if len(FakeProcess.instances) == 0 else 2
            FakeProcess.instances.append(self)

        def start(self):
            self.started = True

        def join(self):
            self.joined = True

    monkeypatch.setattr(bsample, 'Process', FakeProcess)

    sampling = _build_sampling_instance(bsample)
    sampling.job_sampling = True
    sampling.host_sampling = True

    with pytest.raises(SystemExit) as exc_info:
        sampling.sampling()

    assert exc_info.value.code == 1
    assert all(process.joined for process in FakeProcess.instances)


def test_get_utilization_day_info_closes_connection(monkeypatch):
    bsample = _load_bsample_module(monkeypatch)

    class FakeConn:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    fake_conn = FakeConn()

    monkeypatch.setattr(bsample.common_sqlite3, 'connect_db_file', lambda *args, **kwargs: ('passed', fake_conn))
    monkeypatch.setattr(bsample.common_sqlite3, 'get_sql_table_list', lambda *args, **kwargs: ['utilization_hostA'])
    monkeypatch.setattr(
        bsample.common_sqlite3,
        'get_sql_table_data',
        lambda *args, **kwargs: {'slot': ['10', '20'], 'cpu': ['40', '60'], 'mem': ['70', '90']},
    )

    sampling = _build_sampling_instance(bsample)
    sampling.sample_date = '20260303'
    sampling.db_path = str(ROOT / 'db')

    utilization_day_dic = sampling.get_utilization_day_info()

    assert fake_conn.closed is True
    assert utilization_day_dic['utilization_hostA'] == {'slot': 15.0, 'cpu': 50.0, 'mem': 80.0}
