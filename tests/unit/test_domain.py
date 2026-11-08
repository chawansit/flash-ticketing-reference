from unittest.mock import Mock

import pytest

from ticketing.application.reservations import Reservations
from ticketing.domain import Failure, payment_decision


@pytest.mark.parametrize(
    "status,valid,outcome,expected",
    [
        ("PENDING", True, "SUCCEEDED", "BOOK"),
        ("PENDING", False, "SUCCEEDED", "REFUND"),
        ("EXPIRED", True, "SUCCEEDED", "REFUND"),
        ("FAILED", True, "SUCCEEDED", "REFUND"),
        ("PAID", False, "SUCCEEDED", "UNCHANGED"),
        ("FULFILLED", False, "FAILED", "UNCHANGED"),
        ("PENDING", True, "FAILED", "FAILED"),
        ("REFUNDED", False, "FAILED", "UNCHANGED"),
    ],
)
def test_payment_state_machine(status, valid, outcome, expected):
    assert payment_decision(status, valid, outcome) == expected


@pytest.mark.parametrize("seats", [[], ["A", "A"], [""], ["x" * 65], list("ABCDEFGHI")])
def test_invalid_reservation_never_reaches_storage(seats):
    store = Mock()
    with pytest.raises(Failure):
        Reservations(store).reserve("actor", "event", seats, "key")
    store.reserve.assert_not_called()
