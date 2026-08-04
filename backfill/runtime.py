"""Validated execution windows and chunked SIMEM reads for Phase 5."""

from __future__ import annotations

from datetime import date, datetime, timedelta
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


def iter_chunks(start: date, end: date, days: int):
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=days - 1), end)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def read_simem_chunked(reader, dataset_id: str, start: date, end: date, days: int):
    frames = []
    for chunk_start, chunk_end in iter_chunks(start, end, days):
        print(f"SIMEM chunk {dataset_id}: {chunk_start} a {chunk_end}")
        frame = reader(dataset_id, chunk_start.isoformat(), chunk_end.isoformat()).main(filter=False)
        if frame is not None and not frame.empty:
            frames.append(frame)
    return pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()
