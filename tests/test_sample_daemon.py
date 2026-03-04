from pathlib import Path

from monitor.common import sample_daemon


def test_parse_interval_to_seconds():
    assert sample_daemon.SampleDaemonManager.parse_interval_to_seconds('5m') == 300
    assert sample_daemon.SampleDaemonManager.parse_interval_to_seconds('300') == 300
    assert sample_daemon.SampleDaemonManager.parse_interval_to_seconds('1h') == 3600


def test_parse_interval_has_minimum_60s():
    assert sample_daemon.SampleDaemonManager.parse_interval_to_seconds('10') == 60
    assert sample_daemon.SampleDaemonManager.parse_interval_to_seconds('30s') == 60


def test_parse_interval_invalid():
    import pytest

    with pytest.raises(ValueError):
        sample_daemon.SampleDaemonManager.parse_interval_to_seconds('abc')


def test_service_content_contains_run_loop(tmp_path):
    manager = sample_daemon.SampleDaemonManager(install_path=Path('/opt/lsfMonitor'), interval_seconds=300)
    manager.paths.state_dir = tmp_path / 'state'
    manager.paths.log_dir = tmp_path / 'logs'
    manager.paths.env_file = manager.paths.state_dir / 'sample-daemon.env'
    manager.paths.service_file = manager.paths.state_dir / 'lsfmonitor-sample-daemon.service'
    manager.paths.runner_log = manager.paths.log_dir / 'sample-daemon.log'

    content = manager._service_content()
    assert 'sample daemon run-loop' in content
    assert '--interval 300' in content
