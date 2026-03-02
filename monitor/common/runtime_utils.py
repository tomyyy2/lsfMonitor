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
