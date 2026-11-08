import os
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from ticketing.application.reservations import Reservations
from ticketing.infrastructure.postgres import Postgres
from ticketing.infrastructure.reservations import PostgresReservations


class NoShield:
    @contextmanager
    def shield(self, *_):
        yield  # Deliberately remove Redis protection to test the DB authority itself.


@pytest.fixture
def system():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL not configured")
    schema = "test_" + uuid4().hex
    with psycopg.connect(url) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    db = Postgres(make_conninfo(url, options=f"-c search_path={schema}"), maximum=40)
    db.pool.wait(timeout=10)
    try:
        with db.transaction() as conn:
            for migration in sorted(Path("migrations").glob("*.sql")):
                conn.execute(migration.read_text(encoding="utf-8-sig"))
            event_id = uuid4()
            conn.execute(
                """INSERT INTO events VALUES (%s,'Test','THB',clock_timestamp()-interval '1 day',
                clock_timestamp()+interval '1 day')""",
                (event_id,),
            )
            for seat in ["A", "B", "C"]:
                conn.execute(
                    "INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,%s,100)", (event_id, seat)
                )
        svc = Reservations(PostgresReservations(db, NoShield(), 120))
        yield svc, db, event_id
    finally:
        db.close()
        with psycopg.connect(url) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
