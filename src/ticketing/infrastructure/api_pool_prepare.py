"""One-use API preparation; no SQL, pool resize, business writes or authority."""
import copy
import hmac
import math
import os
import re
import threading
import time

from prometheus_client import REGISTRY
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse

PATH = '/internal/capacity/prepare-pools'
HEADER = 'X-Ticketing-Preparation'
DEADLINE_SECONDS = 30.0


def validate_configuration(token, run=None, pod_uid=None):
    run = os.getenv('CCE_RUN_ID', '') if run is None else run
    pod_uid = os.getenv('CCE_POD_UID', '') if pod_uid is None else pod_uid
    if (not isinstance(token, str) or not re.fullmatch(r'[0-9a-f]{64}', token)
            or not isinstance(run, str) or not re.fullmatch(r'adr0151-[0-9a-f]{12}', run)
            or not isinstance(pod_uid, str) or not re.fullmatch(r'[a-zA-Z0-9-]{1,128}', pod_uid)):
        raise RuntimeError('Explicit private API preparation capability and native identity required')


def prepare_existing_clients(general, payment, *, timeout=DEADLINE_SECONDS):
    """Hold every existing slot until growth completes, then return all clients."""
    if (type(timeout) not in (int, float) or not 0 < timeout <= DEADLINE_SECONDS
            or general.pool is payment.pool or general.pool.min_size != 1 or payment.pool.min_size != 1
            or general.pool.max_size != 2 or payment.pool.max_size != 4):
        raise ValueError('Original distinct API pools and bounded deadline required')
    deadline = time.monotonic() + timeout
    held = []
    try:
        for database in (general, payment):
            for _ in range(database.pool.max_size):
                remaining = deadline-time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('API preparation deadline exhausted')
                held.append((database.pool, database.pool.getconn(timeout=remaining)))
        return {'general': 2, 'payment': 4}
    finally:
        # Try every return even if a connection was concurrently closed. Preserve
        # the original preparation error and never close the running pools.
        import sys
        original = sys.exc_info()[1]
        failures = []
        for pool, connection in reversed(held):
            try:
                pool.putconn(connection)
            except Exception as error:  # noqa: BLE001 - Return every other borrowed client.
                failures.append(error)
        if failures and original is None:
            raise RuntimeError('API preparation client return failed') from failures[0]


class PoolPreparation:
    def __init__(self, general, payment, *, token, run=None, pod_uid=None):
        self.run = os.getenv('CCE_RUN_ID', '') if run is None else run
        self.pod_uid = os.getenv('CCE_POD_UID', '') if pod_uid is None else pod_uid
        validate_configuration(token, self.run, self.pod_uid)
        self._token = token
        self._general, self._payment = general, payment
        self._lock = threading.Lock()
        self._nonce = self._receipt = None
        self._state = 'UNUSED'
        self._active = threading.Event()
        self._active.set()

    def close(self):
        self._active.clear()
        self._token = ''

    def authorized(self, token):
        return (self._active.is_set() and isinstance(token, str) and re.fullmatch(r'[0-9a-f]{64}', token) is not None
                and hmac.compare_digest(token, self._token))

    def identity_matches(self, value):
        return (isinstance(value, dict) and set(value) == {'run', 'pod_uid', 'nonce'}
                and value['run'] == self.run and value['pod_uid'] == self.pod_uid
                and isinstance(value['nonce'], str) and re.fullmatch(r'[0-9a-f]{32}', value['nonce']) is not None)

    def prepare(self, nonce):
        if not isinstance(nonce, str) or not re.fullmatch(r'[0-9a-f]{32}', nonce):
            return 422, {'code': 'INVALID_PREPARATION'}
        if not self._lock.acquire(blocking=False):
            return 409, {'code': 'PREPARATION_IN_PROGRESS'}
        try:
            if not self._active.is_set():
                return 409, {'code': 'PREPARATION_CLOSED'}
            if self._state == 'PREPARED' and nonce == self._nonce:
                return 200, copy.deepcopy(self._receipt)
            if self._state != 'UNUSED':
                return 409, {'code': 'PREPARATION_CONSUMED'}
            self._nonce, self._state = nonce, 'PREPARING'
            try:
                process_start = REGISTRY.get_sample_value('process_start_time_seconds')
                if type(process_start) not in (int, float) or not math.isfinite(process_start) or process_start <= 0:
                    raise RuntimeError('Bounded process identity unavailable')
                clients = prepare_existing_clients(self._general, self._payment)
                if not self._active.is_set():
                    raise RuntimeError('API preparation closed during operation')
                self._receipt = {'decision': 'ADR0416', 'run': self.run, 'pod_uid': self.pod_uid,
                    'nonce': nonce, 'process_start_time_seconds': process_start,
                    'completed_epoch': time.time(), 'prepared_clients': clients}
                self._state = 'PREPARED'
                return 200, copy.deepcopy(self._receipt)
            except Exception:  # noqa: BLE001 - Do not expose connection/capability details.
                self._state = 'FAILED'
                return 503, {'code': 'PREPARATION_FAILED'}
        finally:
            self._lock.release()


def install_route(app):
    async def prepare(request: Request):
        manager = getattr(request.app.state, 'api_pool_preparation', None)
        if manager is None or not manager.authorized(request.headers.get(HEADER)):
            return JSONResponse({'code': 'NOT_FOUND'}, status_code=404)
        import json
        raw = bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw) > 1024:
                return JSONResponse({'code': 'INVALID_PREPARATION'}, status_code=422)
        try:
            value = json.loads(raw)
        except (TypeError, ValueError):
            return JSONResponse({'code': 'INVALID_PREPARATION'}, status_code=422)
        if not manager.identity_matches(value):
            return JSONResponse({'code': 'INVALID_PREPARATION'}, status_code=422)
        status, receipt = await run_in_threadpool(manager.prepare, value['nonce'])
        return JSONResponse(receipt, status_code=status)
    app.add_api_route(PATH, prepare, methods=['POST'], include_in_schema=False)
