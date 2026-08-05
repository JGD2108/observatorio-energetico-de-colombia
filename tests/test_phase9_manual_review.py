from decimal import Decimal

from operations.manual_review import assess, optional_float
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_manual_review_is_apt_when_all_operational_controls_pass():
    review = assess(
        stale_sources=0,
        latest_pipeline_status="SUCCESS",
        open_blocking_quality_alerts=0,
        serving_views_ready=9,
        serving_views_expected=9,
    )
    assert review.estado == "APTO"
    assert review.detalle == "Sin hallazgos operativos."


def test_manual_review_requires_attention_for_any_operational_failure():
    review = assess(
        stale_sources=2,
        latest_pipeline_status="FAILED",
        open_blocking_quality_alerts=1,
        serving_views_ready=8,
        serving_views_expected=9,
    )
    assert review.estado == "REVISAR"
    assert "fuera del SLA" in review.detalle
    assert "FAILED" in review.detalle
    assert "HIGH/CRITICAL" in review.detalle
    assert "8/9" in review.detalle


def test_phase9_notebook_is_read_only_and_does_not_create_notifications():
    notebook = (ROOT / "Automation" / "96_manual_phase9_review.py").read_text(encoding="utf-8")
    assert "MERGE INTO" not in notebook
    assert ".write" not in notebook
    assert "CREATE TABLE" not in notebook
    assert "dbutils.notebook.exit" not in notebook


def test_spark_decimal_is_normalized_for_double_schema():
    assert optional_float(Decimal("75.000000")) == 75.0
    assert optional_float(None) is None
