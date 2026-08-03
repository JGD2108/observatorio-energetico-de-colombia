"""Validaciones estaticas de Fase 1."""

import ast
from collections import deque
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]


def load_yaml(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def main():
    bundle = load_yaml(ROOT / "databricks.yml")
    assert bundle["include"] == ["Automation/Job.yaml"]
    job = load_yaml(ROOT / "Automation" / "Job.yaml")["resources"]["jobs"]["observatorio_daily_pipeline"]
    tasks = job["tasks"]
    assert len(tasks) == 23
    by_key = {task["task_key"]: task for task in tasks}
    assert len(by_key) == 23
    dependencies = {
        key: {item["task_key"] for item in task.get("depends_on", [])}
        for key, task in by_key.items()
    }
    assert dependencies["quality_check"] == {"gold_daily"}
    assert dependencies["gold_analytics"] == {"quality_check"}
    assert {"slv_agentes", "slv_plantas"} <= dependencies["slv_generacion"]
    assert {"slv_plantas", "slv_embalses"} <= dependencies["slv_plantas_reservorios"]
    children = {key: set() for key in by_key}
    degree = {key: len(value) for key, value in dependencies.items()}
    for child, parents in dependencies.items():
        for parent in parents:
            assert parent in by_key
            children[parent].add(child)
    queue = deque(key for key, value in degree.items() if value == 0)
    visited = 0
    while queue:
        current = queue.popleft(); visited += 1
        for child in children[current]:
            degree[child] -= 1
            if degree[child] == 0: queue.append(child)
    assert visited == 23
    for task in tasks:
        notebook = task.get("notebook_task", {}).get("notebook_path")
        assert notebook, (task["task_key"], "must run as notebook_task")
        prefix = "${workspace.file_path}/"
        assert notebook.startswith(prefix)
        assert not notebook.endswith(".py"), (
            task["task_key"],
            "Databricks SOURCE notebooks are deployed without the .py suffix",
        )
        notebook_file = ROOT / f"{notebook.removeprefix(prefix)}.py"
        assert notebook_file.exists()
        assert notebook_file.read_text(encoding="utf-8").startswith(
            "# Databricks notebook source"
        )

    imported_modules = [ROOT / "config" / "project_config.py"]
    for module in imported_modules:
        assert not module.read_text(encoding="utf-8").startswith(
            "# Databricks notebook source"
        ), f"Imported Python module cannot be a Databricks notebook: {module}"
    active = [
        ROOT / "setup" / "00_bootstrap.py",
        *(ROOT / "Ingestion").glob("*.py"),
        ROOT / "Bronze_Load" / "02_bronze_daily.py",
        *(ROOT / "Silver_Load").glob("*.py"),
        ROOT / "GOLD LOAD" / "GOLD_LOAD.py",
        ROOT / "Automation" / "gold_incremental_quality_checks.py",
        ROOT / "Gold_Analytics" / "01_vistas_dashboard.py",
    ]
    for path in active:
        text = path.read_text(encoding="utf-8")
        for token in ("/Workspace/Users/", "jgomezdelahoz2108@gmail.com", "!pip install"):
            assert token not in text, (path, token)
        ast.parse(text, filename=str(path))
    assert not list(ROOT.rglob("*.ipynb"))
    ddl = "\n".join(p.read_text(encoding="utf-8") for p in (ROOT / "DDL's").glob("*.py"))
    assert "DROP TABLE" not in ddl.upper()
    print("OK: contratos estaticos de Fase 1 validados")


if __name__ == "__main__":
    main()
