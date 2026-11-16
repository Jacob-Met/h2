"""Sending a graceful GOAWAY must preserve requests already in flight."""
from __future__ import annotations

import pytest
from hyperframe.frame import WindowUpdateFrame

from h2.config import H2Configuration
from h2.connection import ConnectionState, H2Connection
from h2.errors import ErrorCodes
from h2.events import ConnectionTerminated, DataReceived, ResponseReceived, StreamEnded, TrailersReceived
from h2.exceptions import ProtocolError

REQUEST = [
    (b":method", b"GET"),
    (b":scheme", b"https"),
    (b":authority", b"example.com"),
    (b":path", b"/"),
]
RESPONSE = [(b":status", b"200")]


def connected_pair(*, end_stream: bool = True) -> tuple[H2Connection, H2Connection]:
    """Create a connected client/server with an in-flight request."""
    client = H2Connection()
    server = H2Connection(H2Configuration(client_side=False))
    client.initiate_connection()
    server.initiate_connection()
    server.receive_data(client.data_to_send())
    client.receive_data(server.data_to_send())
    client.send_headers(1, REQUEST, end_stream=end_stream)
    server.receive_data(client.data_to_send())
    return client, server


def test_sent_graceful_goaway_preserves_response_and_wire_order() -> None:
    """A graceful close must preserve response bytes and GOAWAY order."""
    client, server = connected_pair()
    server.send_headers(1, RESPONSE)
    server.send_data(1, b"first")
    server.close_connection(graceful=True)
    server.send_data(1, b"last", end_stream=True)

    events = client.receive_data(server.data_to_send())
    assert [type(e) for e in events] == [
        ResponseReceived, DataReceived, ConnectionTerminated, DataReceived, StreamEnded,
    ]
    assert events[1].data + events[3].data == b"firstlast"
    assert server.state_machine.state is ConnectionState.CLOSED
    assert server.state_machine.draining


def test_sent_graceful_goaway_refuses_new_pushed_streams() -> None:
    """Draining cannot create new server-initiated pushed streams."""
    _, server = connected_pair()
    server.send_headers(1, RESPONSE)
    server.close_connection(graceful=True)
    with pytest.raises(ProtocolError, match="GOAWAY"):
        server.push_stream(1, 2, REQUEST)
    server.send_data(1, b"finished", end_stream=True)


def test_default_close_remains_terminal() -> None:
    """An ordinary close still refuses further frames."""
    _, server = connected_pair()
    server.send_headers(1, RESPONSE)
    server.close_connection()
    with pytest.raises(ProtocolError):
        server.send_data(1, b"unavailable")


def test_error_goaway_cannot_be_graceful() -> None:
    """A failed graceful close must not poison an open connection."""
    _, server = connected_pair()
    server.send_headers(1, RESPONSE)
    with pytest.raises(ValueError, match="requires NO_ERROR"):
        server.close_connection(error_code=ErrorCodes.INTERNAL_ERROR, graceful=True)
    server.send_data(1, b"still open", end_stream=True)


def test_goaway_does_not_admit_new_peer_streams_above_boundary() -> None:
    """Peer streams above the advertised boundary are ignored."""
    client, server = connected_pair()
    # Request 3 was already queued on the wire when the GOAWAY was sent.
    client.send_headers(3, REQUEST, end_stream=True)
    in_flight = client.data_to_send()
    server.send_headers(1, RESPONSE)
    server.close_connection(graceful=True, last_stream_id=1)

    assert server.receive_data(in_flight) == []
    assert 3 not in server.streams
    server.send_data(1, b"existing", end_stream=True)
    events = client.receive_data(server.data_to_send())
    assert any(isinstance(event, StreamEnded) and event.stream_id == 1 for event in events)



def test_graceful_goaway_counts_discarded_data_in_connection_window() -> None:
    """Ignored DATA still consumes and replenishes connection credit."""
    client, server = connected_pair()
    client.send_headers(3, REQUEST, end_stream=False)
    server.close_connection(graceful=True, last_stream_id=1)
    server.data_to_send()  # Deliver GOAWAY separately to isolate WINDOW_UPDATE.
    assert server.receive_data(client.data_to_send()) == []

    for _ in range(2):
        client.send_data(3, b"x" * 15000)
        assert server.receive_data(client.data_to_send()) == []
    assert server.inbound_flow_control_window == 65535 - 30000
    assert not server.data_to_send()

    client.send_data(3, b"x" * 15000)
    assert server.receive_data(client.data_to_send()) == []
    assert server.inbound_flow_control_window == 65535
    update = WindowUpdateFrame(0)
    update.window_increment = 45000
    assert server.data_to_send() == update.serialize()


def test_refused_headers_preserve_hpack_table_for_accepted_trailers() -> None:
    """A peer may send compressed refused headers before valid trailers."""
    client, server = connected_pair(end_stream=False)
    server.close_connection(graceful=True, last_stream_id=1)
    # The peer has not yet seen the GOAWAY and opens a stream that was refused.
    dynamic_header = (b"x-marker", b"dynamic header table value")
    client.send_headers(3, [*REQUEST, dynamic_header], end_stream=True)
    client.send_headers(1, [dynamic_header], end_stream=True)
    events = server.receive_data(client.data_to_send())
    assert [type(e) for e in events] == [TrailersReceived, StreamEnded]
    assert events[0].headers == [dynamic_header]
    assert 3 not in server.streams


def test_two_stage_graceful_shutdown_restricts_last_stream_id() -> None:
    """A second GOAWAY can narrow but never increase the accepted boundary."""
    client, server = connected_pair()
    server.send_headers(1, RESPONSE)
    server.close_connection(graceful=True, last_stream_id=2**31 - 1)
    server.close_connection(graceful=True, last_stream_id=1)
    with pytest.raises(ValueError, match="cannot increase"):
        server.close_connection(graceful=True, last_stream_id=3)
    server.send_data(1, b"complete", end_stream=True)
    events = client.receive_data(server.data_to_send())
    assert sum(isinstance(event, ConnectionTerminated) for event in events) == 2
    assert any(isinstance(event, StreamEnded) for event in events)


def test_repeated_graceful_goaway_in_idle_state() -> None:
    """Graceful GOAWAY supports a second phase before any request was sent."""
    server = H2Connection(H2Configuration(client_side=False))
    server.initiate_connection()
    server.close_connection(graceful=True, last_stream_id=0)
    server.close_connection(graceful=True, last_stream_id=0)
    assert server.state_machine.draining



def test_client_can_send_outstanding_request_after_local_goaway() -> None:
    """Both peers continue a request that predates the graceful GOAWAY."""
    client, server = connected_pair(end_stream=False)
    client.close_connection(graceful=True, last_stream_id=0)
    with pytest.raises(ProtocolError, match="GOAWAY"):
        client.send_headers(3, REQUEST, end_stream=True)
    client.send_data(1, b"request body", end_stream=True)

    events = server.receive_data(client.data_to_send())
    assert [type(event) for event in events] == [
        ConnectionTerminated, DataReceived, StreamEnded,
    ]
    assert events[1].data == b"request body"

    server.send_headers(1, RESPONSE, end_stream=True)
    response = client.receive_data(server.data_to_send())
    assert [type(event) for event in response] == [ResponseReceived, StreamEnded]



def test_error_goaway_cannot_expand_previous_graceful_boundary() -> None:
    """A terminal GOAWAY cannot claim streams already declined as unprocessed."""
    _, server = connected_pair()
    server.close_connection(graceful=True, last_stream_id=1)
    with pytest.raises(ValueError, match="cannot increase"):
        server.close_connection(
            error_code=ErrorCodes.INTERNAL_ERROR,
            last_stream_id=3,
        )
    assert server.state_machine.draining
    server.close_connection(
        error_code=ErrorCodes.INTERNAL_ERROR,
        last_stream_id=1,
    )
    assert not server.state_machine.draining
    server.close_connection(
        error_code=ErrorCodes.INTERNAL_ERROR,
        last_stream_id=0,
    )
    with pytest.raises(ValueError, match="cannot increase"):
        server.close_connection(
            error_code=ErrorCodes.INTERNAL_ERROR,
            last_stream_id=1,
        )


def test_graceful_goaway_after_terminal_close_is_not_reopened() -> None:
    """A terminally closed connection must not reopen when asked to drain."""
    _, server = connected_pair()
    server.close_connection()
    with pytest.raises(ProtocolError, match="terminated"):
        server.close_connection(graceful=True)


def test_refused_reset_after_local_goaway_is_ignored() -> None:
    """RST_STREAM for a declined peer stream is harmless after GOAWAY."""
    client, server = connected_pair()
    client.send_headers(3, REQUEST)
    client.reset_stream(3)
    server.close_connection(graceful=True, last_stream_id=1)
    assert server.receive_data(client.data_to_send()) == []
    assert 3 not in server.streams
