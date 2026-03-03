# -*- coding: utf-8 -*-

"""Runtime helper utilities for monitor tools.

These helpers are intentionally lightweight and pure where possible so they can
be covered by unit tests without requiring a full LSF runtime.
"""


def resolve_monitor_tab(jobid=None, user='', feature='', explicit_tab=''):
    """Resolve the startup tab for bmonitor.

    Priority:
    1) Explicit tab from CLI (if provided)
    2) jobid -> JOB
    3) user -> JOBS
    4) feature -> LICENSE
    5) default -> JOBS
    """
    if explicit_tab:
        return explicit_tab

    if jobid:
        return 'JOB'

    if user:
        return 'JOBS'

    if feature:
        return 'LICENSE'

    return 'JOBS'


def resolve_switch_tab(specified_tab, license_tab_available=False):
    """Resolve a safe target tab key for ``MainWindow.switch_tab``.

    ``LICENSE`` is optional in UI runtime. If the tab is not present
    (non-admin user), fallback to ``JOBS``.
    """
    available_tabs = {'JOB', 'JOBS', 'HOSTS', 'LOAD', 'USERS', 'QUEUES', 'UTILIZATION'}

    if license_tab_available:
        available_tabs.add('LICENSE')

    if specified_tab in available_tabs:
        return specified_tab

    return 'JOBS'
