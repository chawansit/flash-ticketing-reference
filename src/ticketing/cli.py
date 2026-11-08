import argparse
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import jwt
import psycopg

from ticketing.config import Settings
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.postgres import Postgres
from ticketing.workers import consume_event, snapshot

DEMO_EVENT = UUID("00000000-0000-0000-0000-000000000001")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["migrate", "seed", "token", "replay"])
    parser.add_argument("--subject", default="demo-user")
    parser.add_argument("--id", help="Dead letter ID for replay")
    args = parser.parse_args()
    settings = Settings()
    settings.validate()
    if args.command == "token":
        if settings.environment != "development":
            raise RuntimeError("Local token issuer is development only")
        now = datetime.now(UTC)
        print(
            jwt.encode(
                {
                    "sub": args.subject,
                    "iss": "ticketing",
                    "aud": "ticketing",
                    "iat": now,
                    "exp": now + timedelta(hours=2),
                },
                settings.jwt_secret,
                algorithm="HS256",
            )
        )
        return
    if args.command == "migrate":
        with psycopg.connect(settings.database_url) as conn:
            conn.execute("SELECT pg_advisory_xact_lock(739112)")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations(name text PRIMARY KEY, checksum text NOT NULL)"
            )
            paths = sorted(Path("migrations").glob("*.sql"))
            if not paths:
                raise RuntimeError("Run migrations from the repository root")
            for path in paths:
                sql = path.read_text(encoding="utf-8")
                checksum = hashlib.sha256(sql.encode()).hexdigest()
                previous = conn.execute(
                    "SELECT checksum FROM schema_migrations WHERE name=%s", (path.name,)
                ).fetchone()
                if previous and previous[0] != checksum:
                    raise RuntimeError(f"Migration changed: {path.name}")
                if not previous:
                    conn.execute(sql)
                    conn.execute("INSERT INTO schema_migrations VALUES (%s,%s)", (path.name, checksum))
        return
    db, cache = Postgres(settings.database_url), RedisSeats(settings.redis_url)
    try:
        if args.command == "seed":
            if settings.environment != "development":
                raise RuntimeError("Demo seed is development only")
            with db.transaction() as conn:
                conn.execute(
                    """INSERT INTO events VALUES (%s,'Bangkok Demo Concert','THB',
                    clock_timestamp()-interval '1 day',clock_timestamp()+interval '365 days') ON CONFLICT DO NOTHING""",
                    (DEMO_EVENT,),
                )
                for i in range(1, 101):
                    conn.execute(
                        "INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,%s,250000) ON CONFLICT DO NOTHING",
                        (DEMO_EVENT, f"A{i:03}"),
                    )
            snapshot(db, cache, DEMO_EVENT)
            print(f"Seeded event {DEMO_EVENT}")
        else:
            if not args.id:
                parser.error("replay requires --id")
            with db.transaction() as conn:
                row = conn.execute(
                    "SELECT * FROM dead_letters WHERE event_id=%s", (UUID(args.id),)
                ).fetchone()
            if not row:
                raise RuntimeError("Dead letter not found")
            consume_event(db, cache, json.loads(row["payload"]["raw"]))
            with db.transaction() as conn:
                conn.execute("DELETE FROM dead_letters WHERE event_id=%s", (UUID(args.id),))
    finally:
        db.close()
        cache.redis.close()


if __name__ == "__main__":
    main()
