import json
import uuid
import random
from contextvars import ContextVar


TRACE_NS = uuid.UUID("bce1164c-4cf9-4d39-ada3-5d4b718f97d5")
RNG = random.Random(x=123456789)
COUNTER = 0
TIME = 1679545050.439509
CURRENT_SPAN = ContextVar("CURRENT_SPAN", default=None)


def new_span_id():
    global COUNTER
    COUNTER += 1
    return uuid.uuid5(TRACE_NS, str(COUNTER)).hex[:16]


def new_trace_id():
    global COUNTER
    COUNTER += 1
    return uuid.uuid5(TRACE_NS, str(COUNTER)).hex


def now():
    global TIME
    delta = RNG.random() * 0.01
    TIME += delta
    return TIME


def to_json(d):
    if isinstance(d, dict):
        return {k: to_json(v) for k, v in d.items() if k[:1] != "_"}
    elif isinstance(d, list):
        return [to_json(x) for x in d]
    elif hasattr(d, "to_json"):
        return d.to_json()
    return d


class Trace(object):
    def __init__(self, trace_id=None):
        if trace_id is None:
            trace_id = new_trace_id()
        self.trace_id = trace_id
        self.batches = {}

    def flush_span(self, span):
        if span.is_segment or span.segment_id is not None:
            if span.is_segment:
                batch_id = span.span_id
            else:
                batch_id = span.segment_id
        else:
            batch_id = ""
        self.batches.setdefault(batch_id, []).append(span)

    def to_json(self):
        return to_json(self.__dict__)


def get_current_span():
    return CURRENT_SPAN.get()


class Span(object):
    def __init__(
        self,
        trace=None,
        span_id=None,
        description=None,
        transaction=None,
        op=None,
        is_segment=None,
        start_time=None,
        end_time=None,
        parent_span=None,
        status=None,
    ):
        if parent_span is None:
            parent_span = get_current_span()
        if trace is None:
            trace = parent_span._trace
        self._trace = trace
        self.trace_id = trace.trace_id
        if span_id is None:
            span_id = new_span_id()
        self.span_id = span_id
        self.description = description
        if start_time is None:
            start_time = now()
        self.start_time = start_time
        self.end_time = end_time

        self._parent_span = parent_span
        self._segment = None
        if parent_span is not None:
            self.parent_span_id = parent_span.span_id
            if parent_span.is_segment:
                self._segment = parent_span
            else:
                self._segment = parent_span._segment
        else:
            self.parent_span_id = None
        self.segment_id = None

        if status is None:
            status = "ok"
        self.status = status
        self.tags = {}
        self.measurements = {}
        if is_segment is None:
            is_segment = transaction is not None
        self.is_segment = is_segment
        if transaction is not None:
            self.tags["transaction"] = transaction
        if op is not None:
            self.tags["op"] = op
        self.is_segment = is_segment or False
        self._previous_span = None

    def __enter__(self):
        self._previous_span = CURRENT_SPAN.set(self)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        CURRENT_SPAN.reset(self._previous_span)
        self.finish()
        self._previous_span = None

    def finish(self):
        # upon finish, we measure
        if self.end_time is None:
            self.end_time = now()

        # if we have a link to a segment and that segment is not yet
        # finished, we are part of the segment.
        if (
            self.segment_id is None
            and self._segment is not None
            and not self._segment.is_finished
        ):
            self.segment_id = self._segment.span_id

        self._trace.flush_span(self)

    @property
    def is_finished(self):
        return self.end_time is not None

    def to_json(self):
        return to_json(self.__dict__)


def generate_subsegment():
    with Span(
        description="generate-payment",
        transaction="thread task.generate-payment",
        op="task.background",
    ) as span:
        span.tags["thread.name"] = "generate-payment"

        db_query = Span(
            description="insert into payments",
            op="db.postgres.query",
        )
        db_query.tags["db.operation"] = "payments"
        db_query.tags[
            "db.statement"
        ] = "insert into payments (a, b, c) values (?, ?, ?)"
        db_query.finish()


def generate():
    trace = Trace()
    with Span(
        trace,
        description="/checkout/submit",
        transaction="/checkout/submit",
        op="http.server",
    ) as segment_span:
        segment_span.tags["release"] = "foobar@1.0.0"
        segment_span.tags["environment"] = "prod"

        db_query = Span(description="select * from auth_user", op="db.postgres.query")
        db_query.tags["db.operation"] = "auth_user"
        db_query.tags["db.statement"] = "select * from auth_user where user_id = ?"
        db_query.measurements["db.rows_returned"] = 1
        db_query.finish()

        db_query = Span(
            description="select * from user_session",
            op="db.postgres.query",
        )
        db_query.tags["db.operation"] = "user_session"
        db_query.tags[
            "db.statement"
        ] = "select * from user_session where session_id = ?"
        db_query.measurements["db.rows_returned"] = 1
        db_query.finish()

        generate_subsegment()

        thread_spawn = Span(description="thread spawn", op="thread.span")

        with Span(description="index.html", op="template.render") as span:
            span.tags["template.name"] = "index.html"
            db_query = Span(
                description="select * from site_config",
                op="db.postgres.query",
            )
            db_query.tags["db.operation"] = "site_config"
            db_query.tags["db.statement"] = "select * from site_config"
            db_query.measurements["db.rows_returned"] = 37
            db_query.finish()

            cache_op = Span(
                description="GET [redacted]",
                op="redis.get",
            )
            cache_op.tags["redis.operation"] = "GET"
            cache_op.tags["cache.hit"] = True
            cache_op.measurements["cache.result_size"] = 189
            cache_op.finish()

        segment_span.status = "ok"

    # outlasts the parent, detaches from segment
    thread_spawn.finish()

    return trace.to_json()


if __name__ == "__main__":
    print(json.dumps(generate(), indent=2))
