CREATE INDEX IF NOT EXISTS event_seats_active_hold
 ON event_seats(hold_id, seat_id)
 WHERE hold_id IS NOT NULL;
