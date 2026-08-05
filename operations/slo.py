"""Pure SLO evaluation primitives shared by Phase 6 notebooks and tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SLOResult:
    slo_id: str
    metric_name: str
    metric_value: Optional[float]
    operator: str
    threshold: float
    passed: bool
    blocking: bool
    detail: str


def evaluate(
    slo_id: str,
    metric_name: str,
    metric_value: Optional[float],
    operator: str,
    threshold: float,
    *,
    blocking: bool = True,
    detail: str = "",
) -> SLOResult:
    comparisons = {
        "<=": lambda value: value <= threshold,
        ">=": lambda value: value >= threshold,
        "==": lambda value: value == threshold,
    }
    if operator not in comparisons:
        raise ValueError(f"Operador SLO no soportado: {operator}")
    passed = metric_value is not None and comparisons[operator](metric_value)
    return SLOResult(
        slo_id=slo_id,
        metric_name=metric_name,
        metric_value=metric_value,
        operator=operator,
        threshold=float(threshold),
        passed=bool(passed),
        blocking=blocking,
        detail=detail,
    )
