"""Bounded callback HTTP ownership; transport failures never trigger an inline retry."""

from http.client import HTTPConnection, HTTPSConnection
from queue import Empty, LifoQueue
from threading import Lock
from urllib.error import HTTPError
from urllib.parse import urlsplit


class CallbackTransport:
    def __init__(self, endpoint, max_connections, timeout=5):
        url = urlsplit(endpoint)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.query
            or url.fragment
        ):
            raise ValueError("Callback endpoint must be a direct HTTP/HTTPS URL")
        if type(max_connections) is not int or max_connections <= 0 or timeout <= 0:
            raise ValueError("Callback connection and timeout bounds must be positive")
        self.endpoint = endpoint
        self.path = url.path or "/"
        self.timeout = timeout
        self._lock = Lock()
        self._closed = False
        self._idle = LifoQueue(maxsize=max_connections)
        connection = HTTPSConnection if url.scheme == "https" else HTTPConnection
        for _ in range(max_connections):
            self._idle.put(connection(url.hostname, port=url.port, timeout=timeout))

    def post(self, body, headers):
        with self._lock:
            if self._closed:
                raise RuntimeError("Callback transport is closed")
        try:
            connection = self._idle.get(timeout=self.timeout)
        except Empty as exc:
            raise TimeoutError("Callback connection acquisition timed out") from exc
        try:
            with self._lock:
                if self._closed:
                    raise RuntimeError("Callback transport is closed")
            connection.request("POST", self.path, body=body, headers=headers)
            response = connection.getresponse()
            try:
                response.read()
                if not 200 <= response.status < 300:
                    raise HTTPError(self.endpoint, response.status, response.reason, response.headers, None)
            finally:
                response.close()
        except BaseException:
            # The request may already have committed. Only the existing payment
            # lease may schedule a later delivery with the same idempotent ID.
            connection.close()
            raise
        finally:
            with self._lock:
                if self._closed:
                    connection.close()
                else:
                    self._idle.put_nowait(connection)

    def close(self):
        with self._lock:
            self._closed = True
            while True:
                try:
                    connection = self._idle.get_nowait()
                except Empty:
                    return
                connection.close()
