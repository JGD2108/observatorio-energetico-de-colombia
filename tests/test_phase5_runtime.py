from datetime import date

import pandas as pd
import pytest

from backfill.runtime import (
    is_window_covered,
    iter_chunks,
    read_simem_chunked,
    resolve_window,
    retry_settings,
)


class Widgets:
    def __init__(self, values):
        self.values = values

    def get(self, name):
        return self.values.get(name, "")


class Dbutils:
    def __init__(self, values):
        self.widgets = Widgets(values)


def test_explicit_backfill_window():
    dbutils = Dbutils({"execution_mode": "BACKFILL", "backfill_start_date": "2024-01-01", "backfill_end_date": "2024-12-31"})
    assert resolve_window(dbutils, date(2026, 1, 1), date(2026, 8, 4), "INCREMENTAL") == (date(2024, 1, 1), date(2024, 12, 31), "BACKFILL")


def test_backfill_requires_dates():
    with pytest.raises(ValueError, match="requiere"):
        resolve_window(Dbutils({"execution_mode": "BACKFILL"}), date.today(), date.today(), "INCREMENTAL")


def test_incremental_ignores_stale_backfill_dates():
    defaults = (date(2026, 6, 1), date(2026, 7, 15), "INCREMENTAL")
    dbutils = Dbutils({"execution_mode": "INCREMENTAL", "backfill_start_date": "2024-01-01", "backfill_end_date": "2024-12-31"})
    assert resolve_window(dbutils, defaults[0], defaults[1], defaults[2]) == defaults


def test_chunks_are_contiguous_and_bounded():
    assert list(iter_chunks(date(2024, 1, 1), date(2024, 3, 5), 31)) == [
        (date(2024, 1, 1), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 3, 2)),
        (date(2024, 3, 3), date(2024, 3, 5)),
    ]


def test_chunked_reader_deduplicates_chunk_boundaries():
    class Reader:
        def __init__(self, dataset, start, end):
            self.start = start

        def main(self, filter=False):
            return pd.DataFrame({"id": [1, 1], "chunk": [self.start, self.start]})

    result = read_simem_chunked(Reader, "dataset", date(2024, 1, 1), date(2024, 2, 1), 31)
    assert len(result) == 2


def test_simem_transient_error_is_retried(monkeypatch):
    attempts = []

    class Response:
        status_code = 502

    class TransientError(Exception):
        response = Response()

    class Reader:
        def __init__(self, dataset, start, end):
            attempts.append(start)

        def main(self, filter=False):
            if len(attempts) < 3:
                raise TransientError("Bad Gateway")
            return pd.DataFrame({"id": [1]})

    sleeps = []
    monkeypatch.setattr("backfill.runtime.random.uniform", lambda *_: 0)
    monkeypatch.setattr("backfill.runtime.time.sleep", sleeps.append)
    result = read_simem_chunked(
        Reader, "EC6945", date(2024, 1, 1), date(2024, 1, 1), 31,
        max_retries=5, retry_base_seconds=2,
    )
    assert len(result) == 1
    assert len(attempts) == 3
    assert sleeps == [2, 4]


def test_simem_permanent_error_is_not_retried(monkeypatch):
    class Response:
        status_code = 404

    class PermanentError(Exception):
        response = Response()

    class Reader:
        def __init__(self, dataset, start, end):
            pass

        def main(self, filter=False):
            raise PermanentError("Not Found")

    monkeypatch.setattr("backfill.runtime.time.sleep", lambda *_: pytest.fail("no debe reintentar"))
    with pytest.raises(PermanentError):
        read_simem_chunked(Reader, "dataset", date(2024, 1, 1), date(2024, 1, 1), 31)


def test_retry_settings_are_validated():
    assert retry_settings(Dbutils({"simem_max_retries": "4", "simem_retry_base_seconds": "3"})) == (4, 3.0)
    with pytest.raises(ValueError, match="simem_max_retries"):
        retry_settings(Dbutils({"simem_max_retries": "11"}))


def test_coverage_accepts_publication_lag_within_sla():
    assert is_window_covered(
        date(2026, 1, 1), date(2026, 8, 4),
        date(2026, 1, 1), date(2026, 7, 22), 45,
    )


def test_coverage_rejects_missing_start_or_excessive_lag():
    assert not is_window_covered(
        date(2026, 1, 1), date(2026, 8, 4),
        date(2026, 1, 2), date(2026, 8, 1), 45,
    )
    assert not is_window_covered(
        date(2026, 1, 1), date(2026, 8, 4),
        date(2026, 1, 1), date(2026, 6, 1), 45,
    )
