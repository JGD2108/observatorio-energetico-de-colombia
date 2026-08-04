from datetime import date

import pandas as pd
import pytest

from backfill.runtime import iter_chunks, read_simem_chunked, resolve_window


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
