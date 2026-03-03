from monitor.common.runtime_utils import resolve_monitor_tab, resolve_switch_tab


def test_resolve_monitor_tab_prefers_explicit_tab():
    assert resolve_monitor_tab(jobid=1, user='u', feature='f', explicit_tab='HOSTS') == 'HOSTS'


def test_resolve_monitor_tab_job_first_when_no_explicit_tab():
    assert resolve_monitor_tab(jobid=123, user='u', feature='f', explicit_tab='') == 'JOB'


def test_resolve_monitor_tab_user_when_no_job():
    assert resolve_monitor_tab(jobid=None, user='alice', feature='f', explicit_tab='') == 'JOBS'


def test_resolve_monitor_tab_feature_when_only_feature():
    assert resolve_monitor_tab(jobid=None, user='', feature='abc', explicit_tab='') == 'LICENSE'


def test_resolve_monitor_tab_default_jobs():
    assert resolve_monitor_tab(jobid=None, user='', feature='', explicit_tab='') == 'JOBS'


def test_resolve_switch_tab_license_available():
    assert resolve_switch_tab('LICENSE', license_tab_available=True) == 'LICENSE'


def test_resolve_switch_tab_license_missing_fallback_jobs():
    assert resolve_switch_tab('LICENSE', license_tab_available=False) == 'JOBS'


def test_resolve_switch_tab_unknown_fallback_jobs():
    assert resolve_switch_tab('UNKNOWN', license_tab_available=True) == 'JOBS'
