"""Validaciones estaticas de Fase 1."""

import ast
from collections import deque
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]


def literal_assignment(path, name):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
                return ast.literal_eval(node.value)
    raise AssertionError(f"Missing literal assignment {name} in {path}")


def contract_columns(contract):
    return {definition.strip().split()[0] for definition in contract.split(",")}


def load_yaml(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def main():
    bundle = load_yaml(ROOT / "databricks.yml")
    assert bundle["include"] == ["Automation/Job.yaml"]
    job = load_yaml(ROOT / "Automation" / "Job.yaml")["resources"]["jobs"]["observatorio_daily_pipeline"]
    tasks = job["tasks"]
    assert len(tasks) == 25
    by_key = {task["task_key"]: task for task in tasks}
    assert len(by_key) == 25
    dependencies = {
        key: {item["task_key"] for item in task.get("depends_on", [])}
        for key, task in by_key.items()
    }
    assert dependencies["quality_check"] == {"gold_daily"}
    assert dependencies["gold_analytics"] == {"quality_check"}
    assert dependencies["audit_start"] == {"setup_catalog"}
    assert dependencies["audit_finalize"] == {"gold_analytics"}
    assert by_key["audit_finalize"]["run_if"] == "ALL_DONE"
    for key in (
        "ing_demanda_real", "ing_disponibilidad", "ing_agentes",
        "ing_generacion_real", "ing_niveles_embalses", "ing_plantas",
        "ing_precio_bolsa", "ing_embalses", "ing_plantas_reservorios",
    ):
        assert dependencies[key] == {"audit_start"}
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
    assert visited == 25

    parameters = {parameter["name"]: parameter["default"] for parameter in job["parameters"]}
    assert parameters["run_id"] == "{{job.run_id}}"
    assert parameters["job_run_id"] == "{{job.run_id}}"
    assert parameters["job_start_time"] == "{{job.start_time.iso_datetime}}"

    business_tasks = set(by_key) - {"audit_start", "audit_finalize"}
    audit_task_specs = literal_assignment(
        ROOT / "Automation" / "99_audit_finalize.py", "TASK_SPECS"
    )
    assert set(audit_task_specs) == business_tasks
    finalizer_parameters = by_key["audit_finalize"]["notebook_task"]["base_parameters"]
    for task_key in business_tasks:
        assert finalizer_parameters[f"state_{task_key}"] == (
            f"{{{{tasks.{task_key}.result_state}}}}"
        )
        assert finalizer_parameters[f"task_run_{task_key}"] == (
            f"{{{{tasks.{task_key}.run_id}}}}"
        )
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

    imported_modules = [
        ROOT / "config" / "project_config.py",
        ROOT / "observability" / "audit.py",
    ]
    for module in imported_modules:
        assert not module.read_text(encoding="utf-8").startswith(
            "# Databricks notebook source"
        ), f"Imported Python module cannot be a Databricks notebook: {module}"
    active = [
        ROOT / "setup" / "00_bootstrap.py",
        ROOT / "Automation" / "00_audit_start.py",
        *(ROOT / "Ingestion").glob("*.py"),
        ROOT / "Bronze_Load" / "02_bronze_daily.py",
        *(ROOT / "Silver_Load").glob("*.py"),
        ROOT / "GOLD LOAD" / "GOLD_LOAD.py",
        ROOT / "Automation" / "gold_incremental_quality_checks.py",
        ROOT / "Automation" / "99_audit_finalize.py",
        ROOT / "Gold_Analytics" / "01_vistas_dashboard.py",
        ROOT / "observability" / "audit.py",
    ]
    for path in active:
        text = path.read_text(encoding="utf-8")
        for token in ("/Workspace/Users/", "jgomezdelahoz2108@gmail.com", "!pip install"):
            assert token not in text, (path, token)
        ast.parse(text, filename=str(path))

    bootstrap = ROOT / "setup" / "00_bootstrap.py"
    bronze_contracts = literal_assignment(bootstrap, "BRONZE_CONTRACTS")
    common_bronze = contract_columns(literal_assignment(bootstrap, "COMMON_BRONZE"))
    for silver_path in (ROOT / "Silver_Load").glob("silver_*.py"):
        source_name = literal_assignment(silver_path, "SOURCE_NAME")
        required_bronze = literal_assignment(silver_path, "required_bronze_columns")
        allowed_bronze = contract_columns(bronze_contracts[source_name]) | common_bronze
        assert required_bronze <= allowed_bronze, (
            silver_path,
            "Silver requires columns outside its Bronze contract",
            sorted(required_bronze - allowed_bronze),
        )
    assert not list(ROOT.rglob("*.ipynb"))
    ddl = "\n".join(p.read_text(encoding="utf-8") for p in (ROOT / "DDL's").glob("*.py"))
    assert "DROP TABLE" not in ddl.upper()
    bootstrap_text = (ROOT / "setup" / "00_bootstrap.py").read_text(encoding="utf-8")
    for table_name in ("pipeline_runs", "task_runs", "layer_metrics"):
        assert f"AUDIT_TABLES['{table_name}']" in bootstrap_text
    print("OK: contratos estaticos de Fase 1 validados")


if __name__ == "__main__":
    main()
