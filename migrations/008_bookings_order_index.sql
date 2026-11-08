-- Order reads/fulfillment must not scan all historical bookings.
-- Nonunique order_id permits multi-seat orders; retain unique(event_id,seat_id).
CREATE INDEX IF NOT EXISTS bookings_order_id ON bookings(order_id);
