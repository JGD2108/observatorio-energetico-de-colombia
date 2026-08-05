"""Pure, read-only decision rules for the Phase 9 operational review."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ReviewResult:
    estado: str
    fuentes_fuera_sla: int
    ultima_corrida_exitosa: bool
    alertas_calidad_abiertas: int
    contratos_serving_listos: int
    contratos_serving_esperados: int
    detalle: str


def optional_float(value: object) -> Optional[float]:
    """Normalize Spark Decimal/numeric scalars for an explicit DoubleType schema."""
    return None if value is None else float(value)


def assess(
    *,
    stale_sources: int,
    latest_pipeline_status: Optional[str],
    open_blocking_quality_alerts: int,
    serving_views_ready: int,
    serving_views_expected: int,
) -> ReviewResult:
    """Classify the dashboard's operational readiness without writing or alerting."""
    issues: list[str] = []
    if stale_sources > 0:
        issues.append(f"{stale_sources} fuente(s) fuera del SLA de frescura")
    if latest_pipeline_status != "SUCCESS":
        issues.append(f"ultima corrida con estado {latest_pipeline_status or 'SIN_EVIDENCIA'}")
    if open_blocking_quality_alerts > 0:
        issues.append(f"{open_blocking_quality_alerts} alerta(s) HIGH/CRITICAL abierta(s)")
    if serving_views_ready != serving_views_expected:
        issues.append(
            f"contratos Serving disponibles {serving_views_ready}/{serving_views_expected}"
        )

    return ReviewResult(
        estado="APTO" if not issues else "REVISAR",
        fuentes_fuera_sla=max(0, stale_sources),
        ultima_corrida_exitosa=latest_pipeline_status == "SUCCESS",
        alertas_calidad_abiertas=max(0, open_blocking_quality_alerts),
        contratos_serving_listos=max(0, serving_views_ready),
        contratos_serving_esperados=max(0, serving_views_expected),
        detalle="Sin hallazgos operativos." if not issues else "; ".join(issues),
    )
