from ticketing.application.ports import PaymentReceiptStore


class PaymentConfirmation:
    """Ingress use case: receipt acceptance is distinct from financial confirmation."""

    def __init__(self, receipts: PaymentReceiptStore):
        self.receipts = receipts

    def receive(self, verified_payload: dict) -> dict:
        return self.receipts.receive(verified_payload)
