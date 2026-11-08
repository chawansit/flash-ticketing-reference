import json
from dataclasses import replace

import pytest

from ticketing.config import Settings
from ticketing.domain import Failure
from ticketing.infrastructure.redis_reservations import RedisReservationIntake


class FakePipeline:
    def __init__(self, owner):
        self.owner = owner

    def eval(self, *_args):
        return self

    def wait(self, replicas, timeout):
        assert replicas == 1
        assert timeout == 50
        return self

    def execute(self):
        return [[1, json.dumps(self.owner.response)], self.owner.acknowledgements]

class FakeRedis:
    def __init__(self, response, acknowledgements=0):
        self.response = response
        self.acknowledgements = acknowledgements
        self.registered = False

    def eval(self, *_args):
        return [1, json.dumps(self.response)]

    def pipeline(self, transaction=False):
        assert transaction is False
        return FakePipeline(self)

    def sadd(self, *_args):
        self.registered = True


class FakeCache:
    delta_history_entries = 512

    def __init__(self, redis):
        self.redis = redis

    @staticmethod
    def key(event):
        return f"seatmap:v2:{{{event}}}"

    @staticmethod
    def delta_key(event):
        return f"seatdelta:v1:{{{event}}}"


def test_production_redis_first_requires_replica_acknowledgement():
    base = replace(
        Settings(),
        environment="production",
        jwt_secret="configured",
        webhook_secret="configured",
        reservation_mode="redis-first",
    )
    with pytest.raises(RuntimeError, match="replica acknowledgement"):
        replace(base, redis_reservation_replica_acks=0).validate()
    replace(base, redis_reservation_replica_acks=1).validate()


def test_unknown_reservation_mode_is_rejected():
    with pytest.raises(RuntimeError, match="RESERVATION_MODE"):
        replace(Settings(), reservation_mode="unknown").validate()


def test_insufficient_wait_acknowledgement_returns_unknown_outcome():
    redis = FakeRedis(
        {
            "command_id": "command",
            "hold_id": "hold",
            "order_id": "order",
            "seats": ["A"],
            "persistence_status": "PENDING",
        }
    )
    intake = RedisReservationIntake(FakeCache(redis), replica_acks=1, wait_ms=50)
    with pytest.raises(Failure, match="RESERVATION_DURABILITY_UNKNOWN"):
        intake.enqueue("actor", "event", ["A"], "key")
    assert not redis.registered
