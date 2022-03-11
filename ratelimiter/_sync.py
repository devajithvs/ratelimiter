# -*- coding: utf-8 -*-

import time
import functools
import threading
import collections

class RateLimitExceeded(Exception):
    """Base class for other exceptions"""
    pass

class RateLimiter(object):

    """Provides rate limiting for an operation with a configurable number of
    requests for a time period.
    """

    def __init__(self, max_calls, period=1.0, callback=None):
        """Initialize a RateLimiter object which enforces as much as max_calls
        operations on period (eventually floating) number of seconds.
        """
        if period <= 0:
            raise ValueError("Rate limiting period should be > 0")
        if max_calls <= 0:
            raise ValueError("Rate limiting number of calls should be > 0")

        # We're using a deque to store the last execution timestamps, not for
        # its maxlen attribute, but to allow constant time front removal.
        self.calls = dict()

        self.period = period
        self.max_calls = max_calls
        self.callback = callback
        self._lock = threading.Lock()
        self._alock = None

        # Lock to protect creation of self._alock
        self._init_lock = threading.Lock()

    def __call__(self, f, identifier):
        """The __call__ function allows the RateLimiter object to be used as a
        regular function decorator.
        """
        if identifier not in self.calls:
            self.calls[identifier] = collections.deque()
            
        # Store the current operation timestamp.
        self.calls[identifier].append(time.time())

        # Pop the timestamp list front (ie: the older calls) until the sum goes
        # back below the period. This is our 'sliding period' window.
        while self._timespan[identifier] >= self.period:
            self.calls[identifier].popleft()
        
        if len(self.calls[identifier]) >= self.max_calls:
            raise RateLimitExceeded

        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            with self:
                return f(*args, **kwargs)

        return wrapped

    def __enter__(self, identifier):
        with self._lock:
            # We want to ensure that no more than max_calls were run in the allowed
            # period. For this, we store the last timestamps of each call and run
            # the rate verification upon each __enter__ call.
            
            # if len(self.calls[identifier]) >= self.max_calls:
            #     until = time.time() + self.period - self._timespan[identifier]
            #     if self.callback:
            #         t = threading.Thread(target=self.callback, args=(until,))
            #         t.daemon = True
            #         t.start()
            #     else:
            #         sleeptime = until - time.time()
            #         if sleeptime > 0:
            #             time.sleep(sleeptime)
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._lock:
            pass

    @property
    def _timespan(self, identifier):
        return self.calls[identifier][-1] - self.calls[identifier][0]
