import time

class CircuitBreaker:
    FAILURE_THRESHOLD = 3
    RECOVERY_TIMEOUT = 10
    CLOSE_STATE = 'CLOSED'
    OPEN_STATE = 'OPEN'
    HALF_OPEN_STATE = 'HALF-OPEN'

    def __init__(self, failure_threshold=3, recovery_timeout=10):
        self.failure_threshold = failure_threshold
        self.failure_count = 0
        self.state = CircuitBreaker.CLOSE_STATE
        self.recovery_timeout = recovery_timeout
        self.last_failure_time = None

    def call(self, service_function):
        if self.state == CircuitBreaker.OPEN_STATE:
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitBreaker.HALF_OPEN_STATE
            else:
                raise Exception("Circuit is OPEN, service is unavailable.")

        try:
            result = service_function()
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    def _on_success(self):
        self.failure_count = 0
        self.state = CircuitBreaker.CLOSE_STATE

    def _on_failure(self):
        self.failure_count += 1
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitBreaker.OPEN_STATE
            self.last_failure_time = time.time()