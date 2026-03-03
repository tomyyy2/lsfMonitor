from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci-cd.yml"


def _read_ci_workflow() -> str:
    return CI_WORKFLOW.read_text(encoding="utf-8")


def test_ci_requirements_install_has_fallback_path():
    text = _read_ci_workflow()

    assert "if [ -f requirements.txt ] && python -m pip install -r requirements.txt; then" in text
    assert "echo \"::warning::requirements.txt install failed in CI, using minimal dependency set.\"" in text
    assert "python -m pip install pandas pyyaml tabulate pytest pytest-cov" in text


def test_ci_build_verify_creates_conf_before_bsample_help_check():
    text = _read_ci_workflow()

    conf_mkdir = 'mkdir -p "$HOME/.lsfMonitor/conf"'
    conf_write = "cat > \"$HOME/.lsfMonitor/conf/config.py\" <<'PYCONF'"
    bsample_help = "python monitor/bin/bsample.py -h"

    assert conf_mkdir in text
    assert conf_write in text
    assert bsample_help in text
    assert text.index(conf_write) < text.index(bsample_help)


def test_ci_keeps_manual_verifier_for_runtime_and_pytest_fallback():
    text = _read_ci_workflow()

    manual_verify_cmd = "python tests/manual_verify_recent_changes.py"

    assert "python -m compileall monitor memPrediction lsfmon.py tests/manual_verify_recent_changes.py" in text
    assert 'if python -c "import pytest"; then' in text
    assert "::warning::pytest environment unavailable, fallback to manual verification script." in text
    assert text.count(manual_verify_cmd) >= 2
