"""Check content-length on HEADERS termination and content-free responses."""
from __future__ import annotations

import pytest

import h2.config
import h2.connection
import h2.events
import h2.exceptions

REQUEST_HEADERS = [
    (":method", "POST"), (":scheme", "https"),
    (":authority", "example.com"), (":path", "/"), ("content-length", "15"),
]


def client(method: str = "GET", *, trailers: bool = False) -> h2.connection.H2Connection:
    """Create a client stream ready to receive a response."""
    conn = h2.connection.H2Connection()
    conn.initiate_connection()
    conn.send_headers(1, [
        (":method", method), (":scheme", "https"),
        (":authority", "example.com"), (":path", "/"),
    ], end_stream=not trailers)
    if trailers:
        conn.send_headers(1, [("x-request-trailer", "v")], end_stream=True)
    conn.clear_outbound_data_buffer()
    return conn


@pytest.mark.parametrize("kind", ["request", "response"])
def test_nonzero_length_cannot_end_on_headers(frame_factory, kind: str) -> None:
    """END_STREAM on HEADERS must validate a declared nonzero length."""
    if kind == "request":
        conn = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=False),
        )
        conn.initiate_connection()
        conn.receive_data(frame_factory.preamble())
        headers = REQUEST_HEADERS
        expected = 15
    else:
        conn = client()
        headers = [(":status", "200"), ("content-length", "1234")]
        expected = 1234
    conn.clear_outbound_data_buffer()
    frame = frame_factory.build_headers_frame(headers, flags=["END_STREAM"])
    with pytest.raises(h2.exceptions.InvalidBodyLengthError) as exc:
        conn.receive_data(frame.serialize())
    assert exc.value.expected_length == expected
    assert exc.value.actual_length == 0


@pytest.mark.parametrize("status", ["204", "304"])
@pytest.mark.parametrize("end_in_headers", [True, False])
def test_no_content_responses_allow_representation_length(frame_factory, status: str, end_in_headers: bool) -> None:
    """204 and 304 have no content regardless of advertised entity size."""
    conn = client()
    headers = frame_factory.build_headers_frame(
        [(":status", status), ("content-length", "1234")],
        flags=["END_STREAM"] if end_in_headers else [],
    )
    payload = headers.serialize()
    if not end_in_headers:
        payload += frame_factory.build_data_frame(data=b"", flags=["END_STREAM"]).serialize()
    events = conn.receive_data(payload)
    assert isinstance(events[0], h2.events.ResponseReceived)
    assert isinstance(events[-1], h2.events.StreamEnded)


@pytest.mark.parametrize("status", ["204", "304"])
def test_no_content_responses_reject_actual_data(frame_factory, status: str) -> None:
    """Payload bytes remain invalid for messages that cannot carry content."""
    conn = client()
    headers = frame_factory.build_headers_frame([(":status", status)])
    data = frame_factory.build_data_frame(data=b"unauthorized", flags=["END_STREAM"])
    with pytest.raises(h2.exceptions.InvalidBodyLengthError) as exc:
        conn.receive_data(headers.serialize() + data.serialize())
    assert exc.value.expected_length == 0
    assert exc.value.actual_length == len(b"unauthorized")


@pytest.mark.parametrize("trailers", [True, False])
def test_head_response_still_allows_representation_length(frame_factory, trailers: bool) -> None:
    """Request trailers must not erase the original HEAD method."""
    conn = client("HEAD", trailers=trailers)
    headers = frame_factory.build_headers_frame([(":status", "200"), ("content-length", "1234")])
    data = frame_factory.build_data_frame(data=b"", flags=["END_STREAM"])
    events = conn.receive_data(headers.serialize() + data.serialize())
    assert isinstance(events[-1], h2.events.StreamEnded)


@pytest.mark.parametrize("header_length", ["0", None])
def test_empty_request_termination_control(frame_factory, header_length: str | None) -> None:
    """Empty requests with zero or absent lengths remain valid."""
    conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
    conn.initiate_connection()
    conn.receive_data(frame_factory.preamble())
    headers = [
        (":method", "POST"), (":scheme", "https"),
        (":authority", "example.com"), (":path", "/"),
    ]
    if header_length is not None:
        headers.append(("content-length", header_length))
    frame = frame_factory.build_headers_frame(headers, flags=["END_STREAM"])
    events = conn.receive_data(frame.serialize())
    assert isinstance(events[-1], h2.events.StreamEnded)

def test_push_promise_head_method_preserves_bodyless_response(frame_factory) -> None:
    """A HEAD push is still a HEAD even though its stream is remote-created."""
    conn = client()
    promised = frame_factory.build_push_promise_frame(
        stream_id=1, promised_stream_id=2,
        headers=[(":method", "HEAD"), (":scheme", "https"),
                 (":authority", "example.com"), (":path", "/cached")],
    )
    response = frame_factory.build_headers_frame(
        [(":status", "200"), ("content-length", "1234")], stream_id=2,
    )
    final = frame_factory.build_data_frame(data=b"", flags=["END_STREAM"], stream_id=2)
    events = conn.receive_data(promised.serialize() + response.serialize() + final.serialize())
    assert isinstance(events[0], h2.events.PushedStreamReceived)
    assert isinstance(events[-1], h2.events.StreamEnded)
    assert events[-1].stream_id == 2
