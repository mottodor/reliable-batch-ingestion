from io import BytesIO

import pytest

from batch_ingestion.checksums import sha256_stream


class TrackingStream(BytesIO):
    def __init__(self, value: bytes) -> None:
        super().__init__(value)
        self.requested_sizes: list[int | None] = []

    def read(self, size: int | None = -1, /) -> bytes:
        self.requested_sizes.append(size)
        return super().read(size)


def test_sha256_stream_reads_bounded_chunks() -> None:
    stream = TrackingStream(b"abcdefghij")

    digest = sha256_stream(stream, chunk_size=4)

    assert digest == "72399361da6a7754fec986dca5b7cbaf1c810a28ded4abaf56b2106d06cb78b0"
    assert stream.requested_sizes == [4, 4, 4, 4]


def test_sha256_stream_rejects_non_positive_chunk_size() -> None:
    with pytest.raises(ValueError, match="chunk_size must be positive"):
        sha256_stream(BytesIO(b"payload"), chunk_size=0)
