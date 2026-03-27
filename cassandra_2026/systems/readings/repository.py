from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Generator
from uuid import UUID

from cassandra.cluster import Cluster, Session
from cassandra.query import BatchStatement, BatchType, PreparedStatement

from models import Reading


# ── Helpers ───────────────────────────────────────────────────────────────────

def _days_in_range(start: datetime, end: datetime) -> Generator[date, None, None]:
    day = start.date()
    while day <= end.date():
        yield day
        day += timedelta(days=1)


def _buckets_in_range(lo: float, hi: float) -> Generator[int, None, None]:
    yield from range(
        min(int(math.floor(lo * 10)), 9),
        min(int(math.floor(hi * 10)), 9) + 1,
    )


# ── Repository ────────────────────────────────────────────────────────────────

class ReadingRepository:

    def __init__(self, session: Session, keyspace: str = "sensor_ks") -> None:
        self.session = session
        self.ks = keyspace
        self._ps: dict[str, PreparedStatement] = {}
        self._prepare_statements()


    # ── Prepared statements ───────────────────────────────────────────────────

    def _prepare_statements(self) -> None:
        ks = self.ks

        self._ps["ins_main"] = self.session.prepare(
            f"INSERT INTO {ks}.readings (id, city, created, value) VALUES (?,?,?,?)"
        )
        self._ps["ins_time"] = self.session.prepare(
            f"INSERT INTO {ks}.readings_by_time (day, created, id, city, value) VALUES (?,?,?,?,?)"
        )
        self._ps["ins_city"] = self.session.prepare(
            f"INSERT INTO {ks}.readings_by_city (city, created, id, value) VALUES (?,?,?,?)"
        )
        self._ps["ins_value"] = self.session.prepare(
            f"INSERT INTO {ks}.readings_by_value (value_bucket, value, id, city, created) VALUES (?,?,?,?,?)"
        )
        self._ps["del_main"] = self.session.prepare(
            f"DELETE FROM {ks}.readings WHERE id=?"
        )
        self._ps["del_time"] = self.session.prepare(
            f"DELETE FROM {ks}.readings_by_time WHERE day=? AND created=? AND id=?"
        )
        self._ps["del_city"] = self.session.prepare(
            f"DELETE FROM {ks}.readings_by_city WHERE city=? AND created=? AND id=?"
        )
        self._ps["del_value"] = self.session.prepare(
            f"DELETE FROM {ks}.readings_by_value WHERE value_bucket=? AND value=? AND id=?"
        )
        self._ps["sel_id"] = self.session.prepare(
            f"SELECT id, city, created, value FROM {ks}.readings WHERE id=?"
        )
        self._ps["sel_time"] = self.session.prepare(
            f"SELECT id, city, created, value FROM {ks}.readings_by_time "
            f"WHERE day=? AND created>=? AND created<=?"
        )
        self._ps["sel_city"] = self.session.prepare(
            f"SELECT id, city, created, value FROM {ks}.readings_by_city WHERE city=?"
        )
        self._ps["sel_city_range"] = self.session.prepare(
            f"SELECT id, city, created, value FROM {ks}.readings_by_city "
            f"WHERE city=? AND created>=? AND created<=?"
        )

        self._ps["sel_value"] = self.session.prepare(
            f"SELECT id, city, created, value FROM {ks}.readings_by_value "
            f"WHERE value_bucket=? AND value>=? AND value<=?"
        )

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _to_reading(row) -> Reading:
        return Reading(id=row.id, city=row.city, created=row.created, value=row.value)

    def _insert_batch(self, r: Reading) -> None:
        # execute all 4 queries (below) or none of them (on error)
        # "all-or-none delivery"
        # but:
        # - no isolation (partial writes visible)
        #
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(self._ps["ins_main"],  (r.id, r.city, r.created, r.value))
        batch.add(self._ps["ins_time"],  (r.day, r.created, r.id, r.city, r.value))
        batch.add(self._ps["ins_city"],  (r.city, r.created, r.id, r.value))
        batch.add(self._ps["ins_value"], (r.value_bucket, r.value, r.id, r.city, r.created))
        self.session.execute(batch)

    def _delete_batch(self, r: Reading) -> None:
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(self._ps["del_main"],  (r.id,))
        batch.add(self._ps["del_time"],  (r.day, r.created, r.id))
        batch.add(self._ps["del_city"],  (r.city, r.created, r.id))
        batch.add(self._ps["del_value"], (r.value_bucket, r.value, r.id))
        self.session.execute(batch)

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def insert(self, reading: Reading) -> None:
        self._insert_batch(reading)

    def get(self, id: UUID) -> Reading | None:
        row = self.session.execute(self._ps["sel_id"], (id,)).one()
        return self._to_reading(row) if row else None

    def update(self, reading: Reading, **changes) -> Reading:
        """Delete old entry (all tables), insert updated one. Returns new Reading."""
        updated = reading.model_copy(update=changes)
        self._delete_batch(reading)
        self._insert_batch(updated)
        return updated

    def delete(self, reading: Reading) -> None:
        self._delete_batch(reading)

    def count(self) -> int:
        return self.session.execute("SELECT COUNT(*) FROM sensor_ks.readings").one()[0]

    # ── Named searches ────────────────────────────────────────────────────────

    def find_by_time_range(self, start: datetime, end: datetime) -> list[Reading]:
        """Iterates day partitions within [start, end]."""
        results: list[Reading] = []
        for day in _days_in_range(start, end):
            rows = self.session.execute(self._ps["sel_time"], (day, start, end))
            results.extend(self._to_reading(r) for r in rows)
        return results

    def find_by_city(
        self,
        city: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Reading]:
        """Exact city match; optionally narrow by created range."""
        if start and end:
            rows = self.session.execute(self._ps["sel_city_range"], (city, start, end))
        else:
            rows = self.session.execute(self._ps["sel_city"], (city,))
        return [self._to_reading(r) for r in rows]



    def find_by_value_range(self, lo: float, hi: float) -> list[Reading]:
        """Iterates relevant value buckets (0.1-wide bands) within [lo, hi]."""
        results: list[Reading] = []
        for bucket in _buckets_in_range(lo, hi):
            rows = self.session.execute(self._ps["sel_value"], (bucket, lo, hi))
            results.extend(self._to_reading(r) for r in rows)
        return results
