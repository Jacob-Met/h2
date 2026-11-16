"""Regression: concurrently active streams must not trigger quadratic state scans."""

from __future__ import annotations

import io

from h2.config import H2Configuration, OutputLogger
from h2.connection import H2Connection
from h2.stream import H2Stream, StreamClosedBy


def test_many_concurrent_streams_do_not_rescan_each_prior_stream(monkeypatch) -> None:
    """Adding streams should inspect stream state a bounded number of times."""
    calls = 0
    original_getter = H2Stream.open.fget
    assert original_getter is not None

    def counted_open(stream: H2Stream) -> bool:
        nonlocal calls
        calls += 1
        return original_getter(stream)

    monkeypatch.setattr(H2Stream, "open", property(counted_open))
    conn = H2Connection()
    conn.initiate_connection()
    headers = [(b":method", b"GET"), (b":scheme", b"https"), (b":authority", b"example.com"), (b":path", b"/")]
    for i in range(150):
        conn.send_headers(2 * i + 1, headers)
    assert conn.open_outbound_streams == 150
    assert calls <= 450, f"Repeatedly rescanned stream state {calls} times"


def test_stream_counts_drop_on_reset_and_closed_stream_is_retired() -> None:
    """Closed outbound streams cease counting before the next send and retain their close reason."""
    conn = H2Connection()
    conn.initiate_connection()
    headers = [(b":method", b"GET"), (b":scheme", b"https"), (b":authority", b"example.com"), (b":path", b"/")]
    conn.send_headers(1, headers)
    stream = conn.streams[1]
    assert not stream.closed
    assert conn.open_outbound_streams == 1
    conn.reset_stream(1)
    assert stream.closed
    assert conn.open_outbound_streams == 0
    assert 1 not in conn.streams
    assert conn._closed_streams[1] == StreamClosedBy.SEND_RST_STREAM
    conn.send_headers(3, headers)
    assert conn.open_outbound_streams == 1


def test_debug_output_does_not_reprint_all_open_stream_ids() -> None:
    """Debug tracing must not generate quadratic output as concurrent streams grow."""
    stream_count = 400
    log = io.StringIO()
    conn = H2Connection(H2Configuration(logger=OutputLogger(file=log)))
    conn.initiate_connection()
    headers = [(b":method", b"GET"), (b":scheme", b"https"), (b":authority", b"example.com"), (b":path", b"/")]
    for i in range(stream_count):
        conn.send_headers(2 * i + 1, headers)
    output = log.getvalue()
    assert "Current streams: dict_keys" not in output
    assert len(output) < 600 * stream_count
