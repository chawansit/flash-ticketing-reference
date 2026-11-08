"""HTTP instrumentation without an intermediate task or body stream."""
import logging
import threading
import time
from collections import OrderedDict
from uuid import uuid4

from starlette.datastructures import MutableHeaders
from starlette.requests import Request
from starlette.responses import JSONResponse

from ticketing.infrastructure.slot_diagnostics import OPERATION, operation_category
from ticketing.observability import (
    HOLD_ADMISSION,
    HOLD_INFLIGHT,
    HOLD_LIMIT,
    HOLD_OCCUPANCY,
    HOLD_TRACE,
    HTTP_CONNECTION_AGE_SECONDS,
    HTTP_CONNECTION_CLOSE_TOTAL,
    LATENCY,
    OUTCOMES,
    REQUEST_ID,
    REQUESTS,
)

log = logging.getLogger('ticketing.api')

_CONNECTION_STATE_MAX = 16_384
_CONNECTION_STATE_IDLE_SECONDS = 120
_connection_state = OrderedDict()
_connection_state_lock = threading.Lock()


def _connection_key(scope):
    return (scope.get("client"), scope.get("server"))


def _now_ms():
    return round(time.perf_counter() * 1000, 3)


class RequestInstrumentation:
    def __init__(self, app, owner, config):
        self.app, self.owner, self.config = app, owner, config

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)

        request = Request(scope)
        request_started = time.monotonic()
        request.state.request_id = str(uuid4())
        request.state.error_code = None
        context = REQUEST_ID.set(request.state.request_id)
        operation_context = OPERATION.set(operation_category(request.method, request.url.path))
        reservation = request.method == 'POST' and request.url.path == '/v1/holds'
        trace = {} if reservation else None
        trace_context = HOLD_TRACE.set(trace)
        arrival_occupancy = self.owner.state.reserve_inflight
        hold_limit = getattr(self.config, "hold_admission_limit", self.config.reserve_concurrency)
        admitted = False
        response_headers_emitted = False

        connection_key = _connection_key(scope)
        key_string = str(connection_key)
        close_state = {"reason": None}
        now = time.monotonic()
        with _connection_state_lock:
            entry = _connection_state.pop(key_string, None)
            if entry is None:
                first_seen = now
                entry = {"first_seen": first_seen, "last_seen": now}
                connection_recreated = True
            else:
                first_seen = entry["first_seen"]
                entry["last_seen"] = now
                connection_recreated = False
            _connection_state[key_string] = entry
            idle_cutoff = now - _CONNECTION_STATE_IDLE_SECONDS
            while _connection_state:
                _, oldest = next(iter(_connection_state.items()))
                if len(_connection_state) <= _CONNECTION_STATE_MAX and oldest["last_seen"] >= idle_cutoff:
                    break
                _connection_state.popitem(last=False)

        connection_age_ms = (now - first_seen) * 1000

        def release():
            nonlocal admitted
            if admitted:
                admitted = False
                self.owner.state.reserve_inflight -= 1
                HOLD_INFLIGHT.set(self.owner.state.reserve_inflight)

        def mark_close(reason):
            if close_state["reason"] is None:
                close_state["reason"] = reason

        async def observed_receive():
            message = await receive()
            if message.get('type') == 'http.disconnect':
                mark_close('client_disconnect')
            return message

        async def observed_send(message):
            nonlocal response_headers_emitted
            if message['type'] == 'http.response.start':
                response_headers_emitted = True
                release()
                elapsed = time.monotonic() - request_started
                route = getattr(scope.get('route'), 'path', '/v1/holds' if reservation else 'unmatched')
                status = message['status']
                REQUESTS.labels(route, request.method, status).inc()
                LATENCY.labels(route).observe(elapsed)
                message = {**message, 'headers': list(message.get('headers', []))}
                headers = MutableHeaders(scope=message)
                headers['X-Request-ID'] = request.state.request_id
                headers['Server-Timing'] = f'app;dur={elapsed * 1000:.2f}'
                if reservation:
                    message.setdefault('extras', {})
                close_reason = close_state['reason']
                HTTP_CONNECTION_AGE_SECONDS.observe(connection_age_ms / 1000)
                if close_reason is not None:
                    HTTP_CONNECTION_CLOSE_TOTAL.labels(close_reason).inc()
                log.info('request', extra={
                    'fields': {
                        'request_id': request.state.request_id,
                        'route': route,
                        'status': status,
                        'duration_ms': round(elapsed * 1000, 2),
                        'error_code': request.state.error_code,
                        'db_failure_type': getattr(request.state, 'db_failure_type', None),
                        **({'db_acquisition_failure': request.state.db_acquisition_failure}
                           if hasattr(request.state, 'db_acquisition_failure') else {}),
                        'connection_key': key_string,
                        'connection_recreated': connection_recreated,
                        'connection_age_ms': round(connection_age_ms, 2),
                        'close_source': close_reason,
                        **({'hold_arrival_occupancy': arrival_occupancy,
                           'hold_limit': hold_limit,
                           'hold_phase_ms': {k: round(v, 3) for k, v in trace.items()}}
                          if reservation else {}),
                    }
                })
            try:
                await send(message)
            except BaseException:
                mark_close('server_send_error')
                raise

        try:
            if reservation:
                HOLD_OCCUPANCY.observe(arrival_occupancy)
                HOLD_LIMIT.set(hold_limit)
            # One process/event loop: check and increment contain no await.
            if reservation and self.owner.state.reserve_inflight >= hold_limit:
                HOLD_ADMISSION.labels('rejected').inc()
                OUTCOMES.labels('request', 'ADMISSION_FULL').inc()
                request.state.error_code = 'ADMISSION_FULL'
                response = JSONResponse(
                    status_code=503,
                    content={'code': 'ADMISSION_FULL', 'request_id': request.state.request_id},
                    headers={'Retry-After': '1'},
                )
                await response(scope, observed_receive, observed_send)
            else:
                if reservation:
                    self.owner.state.reserve_inflight += 1
                    admitted = True
                    request.state.hold_admitted_at = time.monotonic()
                    HOLD_INFLIGHT.set(self.owner.state.reserve_inflight)
                    HOLD_ADMISSION.labels('admitted').inc()
                await self.app(scope, observed_receive, observed_send)
        finally:
            release()
            if not response_headers_emitted:
                close_reason = close_state['reason']
                HTTP_CONNECTION_AGE_SECONDS.observe(connection_age_ms / 1000)
                if close_reason is not None:
                    HTTP_CONNECTION_CLOSE_TOTAL.labels(close_reason).inc()
                log.info('request', extra={
                    'fields': {
                        'request_id': request.state.request_id,
                        'route': getattr(scope.get('route'), 'path', request.url.path),
                        'status': None,
                        'duration_ms': round((time.monotonic() - request_started) * 1000, 2),
                        'error_code': request.state.error_code,
                        'db_failure_type': getattr(request.state, 'db_failure_type', None),
                        'connection_key': key_string,
                        'connection_recreated': connection_recreated,
                        'connection_age_ms': round(connection_age_ms, 2),
                        'close_source': close_reason,
                        **({'hold_arrival_occupancy': arrival_occupancy,
                           'hold_limit': hold_limit,
                           'hold_phase_ms': {k: round(v, 3) for k, v in trace.items()}}
                          if reservation else {}),
                    }
                })
            HOLD_TRACE.reset(trace_context)
            if close_state['reason'] in {'client_disconnect', 'server_send_error'}:
                with _connection_state_lock:
                    _connection_state.pop(key_string, None)
            else:
                with _connection_state_lock:
                    state = _connection_state.get(key_string)
                    if state is not None:
                        state['last_seen'] = time.monotonic()
            OPERATION.reset(operation_context)
            REQUEST_ID.reset(context)
