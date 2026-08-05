import pytest

from operations.slo import evaluate


def test_upper_bound_slo_passes_at_threshold():
    result = evaluate("freshness", "lag_days", 45, "<=", 45)
    assert result.passed is True
    assert result.blocking is True


def test_lower_bound_slo_fails_below_threshold():
    result = evaluate("success", "success_rate", 94.9, ">=", 95, blocking=False)
    assert result.passed is False
    assert result.blocking is False


def test_equality_slo_requires_value():
    assert evaluate("views", "ready", None, "==", 8).passed is False


def test_unknown_operator_is_rejected():
    with pytest.raises(ValueError, match="Operador"):
        evaluate("invalid", "metric", 1, "!=", 2)
