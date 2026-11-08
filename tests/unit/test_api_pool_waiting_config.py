"""The optional waiter cap preserves the old default and rejects unbounded values."""

from dataclasses import replace

import pytest

from ticketing.config import Settings


def test_default_waiter_setting_tracks_pool_size():
    settings = replace(Settings(), pool_max=3, simulator_concurrency=2, pool_max_waiting=None)
    settings.validate()
    assert settings.pool_max_waiting is None


@pytest.mark.parametrize("limit", [0, 65])
def test_waiter_setting_rejects_unbounded_values(limit):
    with pytest.raises(RuntimeError, match="DB_POOL_MAX_WAITING"):
        replace(Settings(), pool_max=3, simulator_concurrency=2, pool_max_waiting=limit).validate()


def test_waiter_setting_can_exceed_connection_limit_without_changing_it():
    settings = replace(Settings(), pool_max=3, simulator_concurrency=2, pool_max_waiting=12)
    settings.validate()
    assert settings.pool_max == 3
    assert settings.pool_max_waiting == 12
