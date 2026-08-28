from pathlib import Path


WORKFLOW = Path(".github/workflows/ci.yml")


def workflow_source() -> str:
    return WORKFLOW.read_text()


def test_ci_workflow_exists():
    assert WORKFLOW.exists()


def test_ci_uses_supported_python():
    source = workflow_source()

    assert 'python-version: "3.11"' in source


def test_ci_installs_java_for_pyspark():
    source = workflow_source()

    assert "actions/setup-java@v4" in source
    assert 'java-version: "17"' in source


def test_ci_installs_project_and_dependencies():
    source = workflow_source()

    assert "pip install -r requirements.txt" in source
    assert "pip install -e ." in source


def test_ci_runs_full_test_suite():
    source = workflow_source()

    assert "pytest -q" in source


def test_ci_defaults_to_delta_backend():
    source = workflow_source()

    assert "ADSTREAM_STORAGE_BACKEND: delta" in source
