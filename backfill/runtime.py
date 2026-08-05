"""Validated execution windows and chunked SIMEM reads for Phase 5."""

from __future__ import annotations

from datetime import date, datetime, timedelta
import random
import time
from typing import Any

import pandas as pd


def _parameter(dbutils: Any, name: str, default: str = "") -> str:
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        value = default
    return str(value or default).strip()


def resolve_window(dbutils: Any, default_start: date, default_end: date, default_mode: str):
    requested_mode = _parameter(dbutils, "execution_mode", "AUTO").upper()
    if requested_mode not in {"AUTO", "INCREMENTAL", "BACKFILL"}:
        raise ValueError("execution_mode debe ser AUTO, INCREMENTAL o BACKFILL")
    mode = default_mode if requested_mode == "AUTO" else requested_mode
    start_text = _parameter(dbutils, "backfill_start_date")
    end_text = _parameter(dbutils, "backfill_end_date")
    if mode != "BACKFILL":
        return default_start, default_end, mode
    if mode == "BACKFILL" and (not start_text or not end_text):
        if requested_mode == "BACKFILL":
            raise ValueError("BACKFILL requiere backfill_start_date y backfill_end_date")
        return default_start, default_end, mode
    start = datetime.strptime(start_text, "%Y-%m-%d").date() if start_text else default_start
    end = datetime.strptime(end_text, "%Y-%m-%d").date() if end_text else default_end
    if start > end:
        raise ValueError(f"Ventana invalida: {start} es posterior a {end}")
    if end > date.today():
        raise ValueError(f"La fecha final {end} no puede estar en el futuro")
    return start, end, mode


def chunk_days(dbutils: Any) -> int:
    value = int(_parameter(dbutils, "backfill_chunk_days", "31"))
    if value < 1 or value > 366:
        raise ValueError("backfill_chunk_days debe estar entre 1 y 366")
    return value


def retry_settings(dbutils: Any) -> tuple[int, float]:
    max_retries = int(_parameter(dbutils, "simem_max_retries", "5"))
    base_seconds = float(_parameter(dbutils, "simem_retry_base_seconds", "5"))
    if max_retries < 0 or max_retries > 10:
        raise ValueError("simem_max_retries debe estar entre 0 y 10")
    if base_seconds < 0 or base_seconds > 60:
        raise ValueError("simem_retry_base_seconds debe estar entre 0 y 60")
    return max_retries, base_seconds


def iter_chunks(start: date, end: date, days: int):
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=days - 1), end)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def is_window_covered(
    requested_start: date,
    requested_end: date,
    actual_start: date | None,
    actual_end: date | None,
    max_lag_days: int,
) -> bool:
    """Validate boundary coverage while honoring the publication-lag SLA."""
    if actual_start is None or actual_end is None:
        return False
    end_lag_days = max((requested_end - actual_end).days, 0)
    return actual_start <= requested_start and end_lag_days <= max_lag_days


def _is_transient(exc: Exception) -> bool:
    status_code = getattr(getattr(exc, "response", None), "status_code", None)
    if status_code is not None:
        return status_code in {429, 500, 502, 503, 504}
    return isinstance(exc, (ConnectionError, TimeoutError)) or exc.__class__.__name__ in {
        "ConnectionError",
        "ConnectTimeout",
        "ReadTimeout",
        "Timeout",
    }


def iter_simem_frames(
    reader,
    dataset_id: str,
    start: date,
    end: date,
    days: int,
    max_retries: int = 5,
    retry_base_seconds: float = 5,
):
    """Yield one SIMEM frame at a time so large sources can be streamed."""
    for chunk_start, chunk_end in iter_chunks(start, end, days):
        print(f"SIMEM chunk {dataset_id}: {chunk_start} a {chunk_end}")
        for attempt in range(max_retries + 1):
            try:
                frame = reader(
                    dataset_id, chunk_start.isoformat(), chunk_end.isoformat()
                ).main(filter=False)
                break
            except Exception as exc:
                if not _is_transient(exc):
                    raise
                if attempt >= max_retries:
                    raise RuntimeError(
                        f"SIMEM {dataset_id} no respondio tras {max_retries + 1} intentos "
                        f"para {chunk_start} a {chunk_end}"
                    ) from exc
                wait_seconds = min(retry_base_seconds * (2**attempt), 60)
                wait_seconds += random.uniform(0, min(wait_seconds * 0.2, 5))
                status = getattr(getattr(exc, "response", None), "status_code", "conexion")
                print(
                    f"SIMEM {dataset_id} error transitorio {status}; "
                    f"reintento {attempt + 1}/{max_retries} en {wait_seconds:.1f}s"
                )
                time.sleep(wait_seconds)
        if frame is not None and not frame.empty:
            yield frame


def read_simem_chunked(
    reader,
    dataset_id: str,
    start: date,
    end: date,
    days: int,
    max_retries: int = 5,
    retry_base_seconds: float = 5,
):
    frames = list(
        iter_simem_frames(
            reader,
            dataset_id,
            start,
            end,
            days,
            max_retries,
            retry_base_seconds,
        )
    )
    return pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()
