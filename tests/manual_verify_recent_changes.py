"""Fallback verification script for recent runtime changes.

Usage:
    python tests/manual_verify_recent_changes.py
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BSAMPLE_PATH = ROOT / 'monitor' / 'bin' / 'bsample.py'
BMONITOR_PATH = ROOT / 'monitor' / 'bin' / 'bmonitor.py'
RUNTIME_UTILS_PATH = ROOT / 'monitor' / 'common' / 'runtime_utils.py'

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class _Dummy:
    def __init__(self, *args, **kwargs):
        pass


def _load_runtime_utils_module(module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, RUNTIME_UTILS_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def load_bsample_module():
    os.environ['LSFMONITOR_INSTALL_PATH'] = str(ROOT)
    os.environ['HOME'] = str(ROOT)

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

    sys.modules['common'] = common_pkg
    sys.modules['common.common'] = common_mod
    sys.modules['common.common_lsf'] = common_lsf_mod
    sys.modules['common.common_sqlite3'] = common_sqlite3_mod
    sys.modules['conf'] = conf_pkg
    sys.modules['conf.config'] = config_mod

    module_name = f'_manual_bsample_{uuid.uuid4().hex}'
    spec = importlib.util.spec_from_file_location(module_name, BSAMPLE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)

    return module


def load_bmonitor_module():
    os.environ['LSFMONITOR_INSTALL_PATH'] = str(ROOT)
    os.environ['HOME'] = str(ROOT)

    qdarkstyle_mod = types.ModuleType('qdarkstyle')
    qdarkstyle_mod.load_stylesheet_pyqt5 = lambda: ''

    pyqt5_mod = types.ModuleType('PyQt5')
    qtcore_mod = types.ModuleType('PyQt5.QtCore')
    qtcore_mod.QDate = _Dummy
    qtcore_mod.Qt = types.SimpleNamespace()
    qtcore_mod.QThread = _Dummy

    qtgui_mod = types.ModuleType('PyQt5.QtGui')
    qtgui_mod.QBrush = _Dummy
    qtgui_mod.QFont = _Dummy
    qtgui_mod.QIcon = _Dummy

    qtwidgets_mod = types.ModuleType('PyQt5.QtWidgets')
    for name in [
        'QAction',
        'QApplication',
        'QDateEdit',
        'QFileDialog',
        'QFrame',
        'QGridLayout',
        'QHeaderView',
        'QLabel',
        'QLineEdit',
        'QMainWindow',
        'QMenu',
        'QMessageBox',
        'QPushButton',
        'QTabWidget',
        'QTableWidget',
        'QTableWidgetItem',
        'QTextEdit',
        'QWidget',
    ]:
        setattr(qtwidgets_mod, name, _Dummy)

    qtwidgets_mod.qApp = types.SimpleNamespace(quit=lambda: None)

    common_pkg = types.ModuleType('common')
    common_mod = types.ModuleType('common.common')
    common_mod.bprint = lambda *args, **kwargs: None
    common_mod.create_dir = lambda *args, **kwargs: None
    common_mod.SaveLog = _Dummy

    common_lsf_mod = types.ModuleType('common.common_lsf')
    common_lsf_mod.get_lsid_info = lambda: ('LSF', '10.1', 'cluster', 'master')

    common_license_mod = types.ModuleType('common.common_license')
    common_pyqt5_mod = types.ModuleType('common.common_pyqt5')
    common_sqlite3_mod = types.ModuleType('common.common_sqlite3')

    runtime_utils_mod = _load_runtime_utils_module(f'_manual_runtime_utils_{uuid.uuid4().hex}')

    common_pkg.common = common_mod
    common_pkg.common_lsf = common_lsf_mod
    common_pkg.common_license = common_license_mod
    common_pkg.common_pyqt5 = common_pyqt5_mod
    common_pkg.common_sqlite3 = common_sqlite3_mod
    common_pkg.runtime_utils = runtime_utils_mod

    conf_pkg = types.ModuleType('conf')
    config_mod = types.ModuleType('conf.config')
    config_mod.db_path = str(ROOT / 'db')
    config_mod.license_administrators = ''
    config_mod.excluded_license_servers = ''
    config_mod.lmstat_path = ''
    config_mod.lmstat_bsub_command = ''
    conf_pkg.config = config_mod

    sys.modules['qdarkstyle'] = qdarkstyle_mod
    sys.modules['PyQt5'] = pyqt5_mod
    sys.modules['PyQt5.QtCore'] = qtcore_mod
    sys.modules['PyQt5.QtGui'] = qtgui_mod
    sys.modules['PyQt5.QtWidgets'] = qtwidgets_mod

    sys.modules['common'] = common_pkg
    sys.modules['common.common'] = common_mod
    sys.modules['common.common_lsf'] = common_lsf_mod
    sys.modules['common.common_license'] = common_license_mod
    sys.modules['common.common_pyqt5'] = common_pyqt5_mod
    sys.modules['common.common_sqlite3'] = common_sqlite3_mod
    sys.modules['common.runtime_utils'] = runtime_utils_mod

    sys.modules['conf'] = conf_pkg
    sys.modules['conf.config'] = config_mod

    module_name = f'_manual_bmonitor_{uuid.uuid4().hex}'
    spec = importlib.util.spec_from_file_location(module_name, BMONITOR_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)

    return module


def build_sampling_instance(bsample):
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


def verify_runtime_utils():
    from monitor.common.runtime_utils import resolve_monitor_tab, resolve_switch_tab

    assert resolve_monitor_tab(jobid=1, user='u', feature='f', explicit_tab='HOSTS') == 'HOSTS'
    assert resolve_monitor_tab(jobid=1, user='u', feature='f', explicit_tab='') == 'JOB'
    assert resolve_monitor_tab(jobid=None, user='u', feature='f', explicit_tab='') == 'JOBS'
    assert resolve_monitor_tab(jobid=None, user='', feature='f', explicit_tab='') == 'LICENSE'
    assert resolve_monitor_tab(jobid=None, user='', feature='', explicit_tab='') == 'JOBS'

    assert resolve_switch_tab('LICENSE', license_tab_available=True) == 'LICENSE'
    assert resolve_switch_tab('LICENSE', license_tab_available=False) == 'JOBS'
    assert resolve_switch_tab('UNKNOWN', license_tab_available=True) == 'JOBS'


def verify_cli_argument_parsing():
    original_argv = sys.argv[:]

    try:
        bsample = load_bsample_module()
        sys.argv = ['bsample.py', '-UD']
        assert bsample.read_args() == (False, False, False, False, False, False, False, False, True)

        sys.argv = ['bsample.py']
        try:
            bsample.read_args()
            raise AssertionError('bsample.read_args() should exit when no sampling flags are provided.')
        except SystemExit as error:
            assert error.code == 1

        bmonitor = load_bmonitor_module()
        sys.argv = ['bmonitor.py', '-j', '123', '-t', 'HOSTS']
        assert bmonitor.read_args() == (123, '', '', 'HOSTS', False, False)

        sys.argv = ['bmonitor.py', '-f', 'verdi']
        assert bmonitor.read_args() == (None, '', 'verdi', 'LICENSE', False, False)
    finally:
        sys.argv = original_argv


def verify_sampling_join_all_processes():
    bsample = load_bsample_module()

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

    bsample.Process = FakeProcess

    sampling = build_sampling_instance(bsample)
    sampling.job_sampling = True
    sampling.host_sampling = True
    sampling.utilization_sampling = True

    sampling.sampling()

    assert len(FakeProcess.instances) == 3
    assert all(process.started for process in FakeProcess.instances)
    assert all(process.joined for process in FakeProcess.instances)


def verify_get_utilization_day_info_close():
    bsample = load_bsample_module()

    class FakeConn:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    fake_conn = FakeConn()

    bsample.common_sqlite3.connect_db_file = lambda *args, **kwargs: ('passed', fake_conn)
    bsample.common_sqlite3.get_sql_table_list = lambda *args, **kwargs: ['utilization_hostA']
    bsample.common_sqlite3.get_sql_table_data = (
        lambda *args, **kwargs: {'slot': ['10', '20'], 'cpu': ['40', '60'], 'mem': ['70', '90']}
    )

    sampling = build_sampling_instance(bsample)
    sampling.sample_date = '20260303'
    sampling.db_path = str(ROOT / 'db')

    utilization_day_dic = sampling.get_utilization_day_info()

    assert fake_conn.closed is True
    assert utilization_day_dic['utilization_hostA'] == {'slot': 15.0, 'cpu': 50.0, 'mem': 80.0}


def main():
    verify_runtime_utils()
    verify_cli_argument_parsing()
    verify_sampling_join_all_processes()
    verify_get_utilization_day_info_close()
    print('OK: recent runtime changes verified.')


if __name__ == '__main__':
    main()
