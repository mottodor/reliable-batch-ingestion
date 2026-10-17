"""Bounded raw HTTP page streaming contracts for BI-050 slice 2."""

import hashlib
import io
from collections.abc import Buffer, Callable, Iterator
from datetime import UTC, date, datetime

import httpx
import pytest

import batch_ingestion.api.fetcher as fetcher_module
from batch_ingestion.api.fetcher import (
    FetchedPage,
    HttpProtocolError,
    HttpRetryExhausted,
    HttpStatusError,
    ResponseSizeLimitError,
    RetryEvent,
    fetch_page,
)
from batch_ingestion.api.retry import RetryClassification
from batch_ingestion.api.sources import get_source_descriptor
from batch_ingestion.config import HttpConfig

SOURCE_DATE = date(2026, 9, 1)
BODY = b'{"items":[]}\n'


def _config(**overrides: object) -> HttpConfig:
    values: dict[str, object] = {
        "base_url": "https://mock.invalid",
        "connect_timeout_seconds": 2.0,
        "read_timeout_seconds": 5.0,
        "max_attempts": 3,
        "backoff_base_seconds": 0.25,
        "backoff_max_seconds": 1.0,
        "max_elapsed_seconds": 10.0,
        "page_size": 25,
        "max_pages": 100,
        "max_response_bytes": 1024,
        "max_backfill_days": 31,
    }
    values.update(overrides)
    return HttpConfig(**values)  # type: ignore[arg-type]


class ChunkStream(httpx.SyncByteStream):
    def __init__(
        self,
        chunks: list[bytes],
        *,
        on_chunk: Callable[[], None] | None = None,
    ) -> None:
        self._chunks = chunks
        self._on_chunk = on_chunk
        self.closed = False
        self.yielded_chunks = 0

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self._chunks:
            self.yielded_chunks += 1
            if self._on_chunk is not None:
                self._on_chunk()
            yield chunk

    def close(self) -> None:
        self.closed = True


class TrackingSpool(io.BytesIO):
    def __init__(self) -> None:
        super().__init__()
        self.written_chunks: list[bytes] = []

    def write(self, data: Buffer, /) -> int:
        self.written_chunks.append(bytes(data))
        return super().write(data)


def _fetch(
    handler: httpx.MockTransport,
    *,
    config: HttpConfig | None = None,
    monotonic: Callable[[], float] = lambda: 0.0,
) -> FetchedPage:
    with httpx.Client(base_url="https://mock.invalid", transport=handler) as client:
        return fetch_page(
            client=client,
            config=config or _config(),
            descriptor=get_source_descriptor("fx-rates"),
            source_date=SOURCE_DATE,
            page=1,
            monotonic=monotonic,
            sleep=lambda _: pytest.fail("happy-path fetch must not sleep"),
            utc_now=lambda: datetime(2026, 9, 1, 12, tzinfo=UTC),
        )


def test_fetch_page_streams_raw_chunks_and_calculates_exact_length_and_checksum() -> None:
    stream = ChunkStream([BODY[:4], BODY[4:9], BODY[9:]])
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            headers={"Content-Type": " application/json; charset=utf-8 "},
            stream=stream,
        )

    fetched = _fetch(httpx.MockTransport(handler))
    try:
        assert fetched.content_length == len(BODY)
        assert fetched.checksum_sha256 == hashlib.sha256(BODY).hexdigest()
        assert fetched.content_type == "application/json; charset=utf-8"
        assert fetched.attempt == 1
        assert fetched.received_at == datetime(2026, 9, 1, 12, tzinfo=UTC)
        assert fetched.stream.seekable()
        assert fetched.stream.tell() == 0
        assert fetched.stream.read() == BODY
    finally:
        fetched.close()

    assert fetched.stream.closed is True
    assert stream.yielded_chunks == 3
    assert stream.closed is True
    assert len(requests) == 1
    assert requests[0].url.path == "/v1/fx-rates"
    assert dict(requests[0].url.params) == {
        "source_date": "2026-09-01",
        "page": "1",
        "page_size": "25",
    }
    assert requests[0].extensions["timeout"] == {
        "connect": 2.0,
        "read": 5.0,
        "write": 5.0,
        "pool": 2.0,
    }


def test_response_size_limit_stops_on_the_first_exceeding_chunk() -> None:
    stream = ChunkStream([b"1234", b"5678", b"must-not-be-read"])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            stream=stream,
        )

    with pytest.raises(ResponseSizeLimitError, match="maximum of 6 bytes"):
        _fetch(httpx.MockTransport(handler), config=_config(max_response_bytes=6))
    assert stream.yielded_chunks == 2
    assert stream.closed is True


@pytest.mark.parametrize(
    "headers",
    [{}, {"Content-Type": "text/plain"}, {"Content-Type": "application/problem+json"}],
)
def test_fetch_page_requires_application_json_content_type(headers: dict[str, str]) -> None:
    stream = ChunkStream([BODY])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers=headers, stream=stream)

    with pytest.raises(HttpProtocolError, match="Content-Type"):
        _fetch(httpx.MockTransport(handler))
    assert stream.closed is True


def test_slow_transport_trickle_stops_at_deadline_and_closes_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    elapsed = 0.0
    spools: list[TrackingSpool] = []

    def monotonic() -> float:
        return elapsed

    def advance() -> None:
        nonlocal elapsed
        elapsed += 0.6

    def tracking_spool(*, max_size: int, mode: str) -> TrackingSpool:
        assert max_size == 1024
        assert mode == "w+b"
        spool = TrackingSpool()
        spools.append(spool)
        return spool

    monkeypatch.setattr(fetcher_module, "SpooledTemporaryFile", tracking_spool)
    stream = ChunkStream([b"x"] * 10, on_chunk=advance)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            stream=stream,
        )

    with pytest.raises(HttpRetryExhausted) as captured:
        _fetch(
            httpx.MockTransport(handler),
            config=_config(max_elapsed_seconds=1.0),
            monotonic=monotonic,
        )

    assert captured.value.attempt == 1
    assert captured.value.retry_count == 0
    assert captured.value.status_code is None
    assert captured.value.classification is RetryClassification.TRANSPORT_ERROR
    assert stream.yielded_chunks == 2
    assert stream.closed is True
    assert len(spools) == 1
    assert spools[0].written_chunks == [b"x"]
    assert spools[0].closed is True


class FakeClock:
    def __init__(self) -> None:
        self.elapsed = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.elapsed

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.elapsed += seconds


def test_rate_limit_retries_once_with_retry_after_then_returns_second_body() -> None:
    clock = FakeClock()
    requests = 0
    retries: list[RetryEvent] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        if requests == 1:
            return httpx.Response(429, headers={"Retry-After": "2"}, json={"error": "wait"})
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            stream=httpx.ByteStream(BODY),
        )

    with (
        httpx.Client(
            base_url="https://mock.invalid", transport=httpx.MockTransport(handler)
        ) as client,
        fetch_page(
            client=client,
            config=_config(max_elapsed_seconds=5.0),
            descriptor=get_source_descriptor("fx-rates"),
            source_date=SOURCE_DATE,
            page=1,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            utc_now=lambda: datetime(2026, 9, 1, 12, tzinfo=UTC),
            retry_observer=retries.append,
        ) as fetched,
    ):
        assert fetched.stream.read() == BODY
        assert fetched.attempt == 2

    assert requests == 2
    assert clock.sleeps == [2.0]
    assert retries == [
        RetryEvent(
            attempt=2,
            classification=RetryClassification.RATE_LIMIT,
            status_code=429,
            delay_seconds=2.0,
        )
    ]


def test_permanent_client_error_is_not_retried_and_body_is_not_exposed() -> None:
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(404, content=b"must-not-be-preserved")

    with (
        httpx.Client(
            base_url="https://mock.invalid", transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(HttpStatusError) as captured,
    ):
        fetch_page(
            client=client,
            config=_config(),
            descriptor=get_source_descriptor("fx-rates"),
            source_date=SOURCE_DATE,
            page=1,
            monotonic=lambda: 0.0,
            sleep=lambda _: pytest.fail("permanent 4xx must not sleep"),
            utc_now=lambda: datetime(2026, 9, 1, 12, tzinfo=UTC),
        )

    assert requests == 1
    assert captured.value.status_code == 404
    assert captured.value.classification is RetryClassification.PERMANENT_CLIENT_ERROR
    assert captured.value.attempt == 1


def test_permanent_server_error_exhausts_exact_attempt_budget() -> None:
    clock = FakeClock()
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(500, content=b"must-not-be-preserved")

    with (
        httpx.Client(
            base_url="https://mock.invalid", transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(HttpRetryExhausted) as captured,
    ):
        fetch_page(
            client=client,
            config=_config(max_attempts=3),
            descriptor=get_source_descriptor("fx-rates"),
            source_date=SOURCE_DATE,
            page=1,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            utc_now=lambda: datetime(2026, 9, 1, 12, tzinfo=UTC),
        )

    assert requests == 3
    assert clock.sleeps == [0.25, 0.5]
    assert captured.value.attempt == 3
    assert captured.value.retry_count == 2
    assert captured.value.status_code == 500
    assert captured.value.classification is RetryClassification.SERVER_ERROR


def test_transport_failures_are_bounded_by_elapsed_budget() -> None:
    clock = FakeClock()
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        raise httpx.ConnectError("injected transport failure", request=request)

    with (
        httpx.Client(
            base_url="https://mock.invalid", transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(HttpRetryExhausted) as captured,
    ):
        fetch_page(
            client=client,
            config=_config(max_attempts=5, max_elapsed_seconds=0.2),
            descriptor=get_source_descriptor("fx-rates"),
            source_date=SOURCE_DATE,
            page=1,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            utc_now=lambda: datetime(2026, 9, 1, 12, tzinfo=UTC),
        )

    assert requests == 1
    assert clock.sleeps == []
    assert captured.value.attempt == 1
    assert captured.value.retry_count == 0
    assert captured.value.status_code is None
    assert captured.value.classification is RetryClassification.TRANSPORT_ERROR


def test_fetched_page_context_closes_its_spooled_stream() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            stream=httpx.ByteStream(BODY),
        )

    with _fetch(httpx.MockTransport(handler)) as fetched:
        stream = fetched.stream
        assert isinstance(stream, io.BufferedIOBase) or stream.seekable()
        assert stream.closed is False

    assert stream.closed is True
