from __future__ import annotations

import importlib.util
import sys
import types
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BSAMPLE_PATH = ROOT / 'monitor' / 'bin' / 'bsample.py'
BMONITOR_PATH = ROOT / 'monitor' / 'bin' / 'bmonitor.py'
RUNTIME_UTILS_PATH = ROOT / 'monitor' / 'common' / 'runtime_utils.py'


class _Dummy:
    def __init__(self, *args, **kwargs):
        pass


def _load_runtime_utils_module(module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, RUNTIME_UTILS_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _install_bsample_stubs(monkeypatch):
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


def _load_bsample_module(monkeypatch):
    _install_bsample_stubs(monkeypatch)

    module_name = f'_test_bsample_cli_{uuid.uuid4().hex}'
    spec = importlib.util.spec_from_file_location(module_name, BSAMPLE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)

    return module


def _install_bmonitor_stubs(monkeypatch):
    monkeypatch.setenv('LSFMONITOR_INSTALL_PATH', str(ROOT))
    monkeypatch.setenv('HOME', str(ROOT))

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
        'QInputDialog',
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

    runtime_utils_mod = _load_runtime_utils_module(f'_test_runtime_utils_{uuid.uuid4().hex}')

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

    monkeypatch.setitem(sys.modules, 'qdarkstyle', qdarkstyle_mod)
    monkeypatch.setitem(sys.modules, 'PyQt5', pyqt5_mod)
    monkeypatch.setitem(sys.modules, 'PyQt5.QtCore', qtcore_mod)
    monkeypatch.setitem(sys.modules, 'PyQt5.QtGui', qtgui_mod)
    monkeypatch.setitem(sys.modules, 'PyQt5.QtWidgets', qtwidgets_mod)

    monkeypatch.setitem(sys.modules, 'common', common_pkg)
    monkeypatch.setitem(sys.modules, 'common.common', common_mod)
    monkeypatch.setitem(sys.modules, 'common.common_lsf', common_lsf_mod)
    monkeypatch.setitem(sys.modules, 'common.common_license', common_license_mod)
    monkeypatch.setitem(sys.modules, 'common.common_pyqt5', common_pyqt5_mod)
    monkeypatch.setitem(sys.modules, 'common.common_sqlite3', common_sqlite3_mod)
    monkeypatch.setitem(sys.modules, 'common.runtime_utils', runtime_utils_mod)

    monkeypatch.setitem(sys.modules, 'conf', conf_pkg)
    monkeypatch.setitem(sys.modules, 'conf.config', config_mod)


def _load_bmonitor_module(monkeypatch):
    _install_bmonitor_stubs(monkeypatch)

    module_name = f'_test_bmonitor_cli_{uuid.uuid4().hex}'
    spec = importlib.util.spec_from_file_location(module_name, BMONITOR_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)

    return module


def test_bsample_read_args_parses_utilization_day(monkeypatch):
    bsample = _load_bsample_module(monkeypatch)
    monkeypatch.setattr(sys, 'argv', ['bsample.py', '-UD'])

    args = bsample.read_args()

    assert args == (False, False, False, False, False, False, False, False, True)


def test_bsample_read_args_requires_at_least_one_flag(monkeypatch):
    bsample = _load_bsample_module(monkeypatch)
    monkeypatch.setattr(sys, 'argv', ['bsample.py'])

    with pytest.raises(SystemExit) as exc_info:
        bsample.read_args()

    assert exc_info.value.code == 1


def test_bmonitor_read_args_prefers_explicit_tab(monkeypatch):
    bmonitor = _load_bmonitor_module(monkeypatch)
    monkeypatch.setattr(sys, 'argv', ['bmonitor.py', '-j', '123', '-t', 'HOSTS'])

    parsed = bmonitor.read_args()

    assert parsed == (123, '', '', 'HOSTS', False, False)


def test_bmonitor_read_args_resolves_tab_from_feature(monkeypatch):
    bmonitor = _load_bmonitor_module(monkeypatch)
    monkeypatch.setattr(sys, 'argv', ['bmonitor.py', '-f', 'verdi'])

    parsed = bmonitor.read_args()

    assert parsed == (None, '', 'verdi', 'LICENSE', False, False)
