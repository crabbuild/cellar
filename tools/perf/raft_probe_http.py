"""Bounded, credential-free timing of the existing urllib HTTP probe path."""
import http.client
import contextvars
import time
import urllib.request


class ProbeTrace:
    CAPACITY = 16

    def __init__(self):
        self.events = []
        self.started = 0
        self.depth = 0

    def call(self, phase, operation, *args, **kwargs):
        row = {'sequence': self.started, 'phase': phase, 'depth': self.depth,
               'started_ns': time.monotonic_ns(), 'finished_ns': None,
               'outcome': 'pending', 'error_type': None}
        self.started += 1
        if len(self.events) < self.CAPACITY:
            self.events.append(row)
        self.depth += 1
        try:
            result = operation(*args, **kwargs)
            row['outcome'] = 'complete'
            return result
        except BaseException as error:
            row.update(outcome='failed', error_type=type(error).__name__)
            raise
        finally:
            row['finished_ns'] = time.monotonic_ns()
            self.depth -= 1

    def read(self, response):
        return self.call('response_body', response.read)

    def snapshot(self):
        return {'capacity': self.CAPACITY, 'started': self.started,
                'dropped': self.started - len(self.events),
                'events': [dict(row) for row in self.events]}


class TimedConnection(http.client.HTTPConnection):
    def __init__(self, *args, trace, **kwargs):
        super().__init__(*args, **kwargs)
        self.trace = trace

    def connect(self):
        return self.trace.call('connect', super().connect)

    def send(self, data):
        # stdlib send may call connect; these spans overlap intentionally.
        return self.trace.call('send', super().send, data)

    def getresponse(self):
        return self.trace.call('response_headers', super().getresponse)


class TimedHandler(urllib.request.HTTPHandler):
    def __init__(self):
        super().__init__()
        self.trace = contextvars.ContextVar('cellar_probe_http_trace')

    def http_open(self, request):
        trace = self.trace.get()
        def connection(*args, **kwargs):
            return TimedConnection(*args, trace=trace, **kwargs)
        return self.do_open(connection, request)


class ProbeHTTP:
    def __init__(self):
        # Build once before measurement, like urllib's cached default opener.
        # Preserve its proxy, redirect, HTTP-error and connection-close behavior.
        self.handler = TimedHandler()
        self.opener = urllib.request.build_opener(self.handler)

    def open(self, request, timeout, trace):
        if request.type != 'http' or request.get_method() != 'GET':
            raise ValueError('probe timing requires an HTTP read')
        binding = self.handler.trace.set(trace)
        try:
            return self.opener.open(request, timeout=timeout)
        finally:
            self.handler.trace.reset(binding)
