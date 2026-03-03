from __future__ import annotations

import importlib.util
import json
import os
import sys
import types
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
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
    config_mod.license_administrators = 'all'
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

    module_name = f'_test_bmonitor_report_{uuid.uuid4().hex}'
    spec = importlib.util.spec_from_file_location(module_name, BMONITOR_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _read_history_records(window):
    history_file = window.get_report_export_history_file()

    if not history_file.exists():
        return []

    return [json.loads(line) for line in history_file.read_text(encoding='utf-8').splitlines() if line.strip()]


def test_build_weekly_report_command_contains_required_flags(monkeypatch, tmp_path):
    bmonitor = _load_bmonitor_module(monkeypatch)

    window = bmonitor.MainWindow.__new__(bmonitor.MainWindow)
    window.db_path = str(tmp_path / 'db_root')

    command = window.build_weekly_report_command(tmp_path)

    assert command[0] == sys.executable
    assert command[1].endswith('lsfmon.py')
    assert '--db-path' in command
    assert '--export' in command
    assert command[command.index('--export') + 1] == 'csv,md'
    assert command[command.index('--range') + 1] == '7d'


def test_export_weekly_summary_report_bundle_success(monkeypatch, tmp_path):
    bmonitor = _load_bmonitor_module(monkeypatch)

    output_dir = tmp_path / 'reports'
    output_dir.mkdir(parents=True, exist_ok=True)

    window = bmonitor.MainWindow.__new__(bmonitor.MainWindow)
    window.db_path = str(tmp_path / 'db_root')
    window.select_report_output_dir = lambda: str(output_dir)

    warning_messages = []
    window.gui_warning = lambda message: warning_messages.append(message)

    information_messages = []
    monkeypatch.setattr(
        bmonitor.QMessageBox,
        'information',
        lambda *_args: information_messages.append(_args[2]),
        raising=False,
    )

    captured_command = {}

    def _fake_run(command, stdout=None, stderr=None, text=None):
        captured_command['value'] = command

        (output_dir / 'lsfmon_weekly_20260303.csv').write_text('date,jobs_total\n', encoding='utf-8')
        (output_dir / 'lsfmon_weekly_metrics_20260303.csv').write_text('metric,value,note\n', encoding='utf-8')
        (output_dir / 'lsfmon_weekly_anomaly_top_20260303.csv').write_text('rank,date\n', encoding='utf-8')
        (output_dir / 'lsfmon_weekly_20260303.md').write_text('# Weekly\n', encoding='utf-8')

        return types.SimpleNamespace(returncode=0, stdout='ok', stderr='')

    monkeypatch.setattr(bmonitor.subprocess, 'run', _fake_run)

    window.export_weekly_summary_report_bundle()

    assert warning_messages == []
    assert '--export' in captured_command['value']
    assert captured_command['value'][captured_command['value'].index('--export') + 1] == 'csv,md'
    assert len(information_messages) == 1
    assert 'Weekly summary exported successfully' in information_messages[0]

    history_records = _read_history_records(window)
    assert len(history_records) == 4
    assert all(record['status'] == 'SUCCESS' for record in history_records)

    exported_paths = {Path(record['path']).name for record in history_records}
    assert exported_paths == {
        'lsfmon_weekly_20260303.csv',
        'lsfmon_weekly_metrics_20260303.csv',
        'lsfmon_weekly_anomaly_top_20260303.csv',
        'lsfmon_weekly_20260303.md',
    }


def test_export_weekly_summary_report_bundle_no_new_export(monkeypatch, tmp_path):
    bmonitor = _load_bmonitor_module(monkeypatch)

    output_dir = tmp_path / 'reports'
    output_dir.mkdir(parents=True, exist_ok=True)

    stale_file_list = [
        output_dir / 'lsfmon_weekly_20260228.csv',
        output_dir / 'lsfmon_weekly_metrics_20260228.csv',
        output_dir / 'lsfmon_weekly_anomaly_top_20260228.csv',
        output_dir / 'lsfmon_weekly_20260228.md',
    ]

    for stale_file in stale_file_list:
        stale_file.write_text('stale\n', encoding='utf-8')
        stale_file.touch()

    window = bmonitor.MainWindow.__new__(bmonitor.MainWindow)
    window.db_path = str(tmp_path / 'db_root')
    window.select_report_output_dir = lambda: str(output_dir)

    warning_messages = []
    window.gui_warning = lambda message: warning_messages.append(message)

    information_messages = []
    monkeypatch.setattr(
        bmonitor.QMessageBox,
        'information',
        lambda *_args: information_messages.append(_args[2]),
        raising=False,
    )

    monkeypatch.setattr(bmonitor.time, 'time', lambda: 2000.0)

    for stale_file in stale_file_list:
        # Keep historical files older than command start time.
        os.utime(stale_file, (1000.0, 1000.0))

    monkeypatch.setattr(
        bmonitor.subprocess,
        'run',
        lambda *_args, **_kwargs: types.SimpleNamespace(returncode=0, stdout='ok', stderr=''),
    )

    window.export_weekly_summary_report_bundle()

    assert warning_messages == []
    assert len(information_messages) == 1
    assert '无新导出' in information_messages[0]

    history_records = _read_history_records(window)
    assert len(history_records) == 1
    assert history_records[0]['type'] == 'weekly_summary'
    assert history_records[0]['status'] == 'NO_EXPORT'
    assert 'No new export generated' in history_records[0]['message']



def test_export_weekly_summary_report_bundle_failure(monkeypatch, tmp_path):
    bmonitor = _load_bmonitor_module(monkeypatch)

    output_dir = tmp_path / 'reports'
    output_dir.mkdir(parents=True, exist_ok=True)

    window = bmonitor.MainWindow.__new__(bmonitor.MainWindow)
    window.db_path = str(tmp_path / 'db_root')
    window.select_report_output_dir = lambda: str(output_dir)

    warning_messages = []
    window.gui_warning = lambda message: warning_messages.append(message)

    monkeypatch.setattr(
        bmonitor.subprocess,
        'run',
        lambda *_args, **_kwargs: types.SimpleNamespace(returncode=1, stdout='', stderr='mock export failure'),
    )

    window.export_weekly_summary_report_bundle()

    assert len(warning_messages) == 1
    assert 'Failed to export weekly summary' in warning_messages[0]
    assert 'mock export failure' in warning_messages[0]

    history_records = _read_history_records(window)
    assert len(history_records) == 1
    assert history_records[0]['type'] == 'weekly_summary'
    assert history_records[0]['status'] == 'FAILED'
    assert history_records[0]['message'] == 'mock export failure'


def test_report_export_history_append_and_load_recent_records(monkeypatch, tmp_path):
    bmonitor = _load_bmonitor_module(monkeypatch)

    window = bmonitor.MainWindow.__new__(bmonitor.MainWindow)
    window.db_path = str(tmp_path / 'db_root')

    window.append_report_export_history(export_type='csv', export_path='/tmp/a.csv', status='SUCCESS', message='ok-1')
    window.append_report_export_history(export_type='md', export_path='/tmp/a.md', status='FAILED', message='boom')

    history_file = window.get_report_export_history_file()
    history_file.write_text(history_file.read_text(encoding='utf-8') + 'not-a-json-line\n', encoding='utf-8')

    window.append_report_export_history(export_type='csv', export_path='/tmp/b.csv', status='SUCCESS', message='ok-2')

    latest_two = window.load_recent_report_export_history(limit=2)
    assert [item['path'] for item in latest_two] == ['/tmp/b.csv', '/tmp/a.md']
    assert latest_two[0]['message'] == 'ok-2'
    assert latest_two[1]['status'] == 'FAILED'

    all_records = window.load_recent_report_export_history(limit=0)
    assert [item['path'] for item in all_records] == ['/tmp/b.csv', '/tmp/a.md', '/tmp/a.csv']
