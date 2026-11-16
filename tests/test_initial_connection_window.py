"""Regression for configurable connection-level inbound flow control."""

from __future__ import annotations

import pytest
from hyperframe.frame import Frame, SettingsFrame, WindowUpdateFrame

from h2.config import H2Configuration
from h2.connection import H2Connection


def test_initial_connection_window_sends_one_update() -> None:
    """The connection advertises the configured larger window after SETTINGS."""
    conn = H2Connection(H2Configuration(initial_connection_window_size=1024 * 1024))
    conn.initiate_connection()
    assert conn.inbound_flow_control_window == 1024 * 1024
    outbound = conn.data_to_send()
    assert outbound.startswith(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
    _, length = Frame.parse_frame_header(memoryview(outbound[24:33]))
    first_end = 24 + 9 + length
    first, _ = Frame.parse_frame_header(memoryview(outbound[24:33]))
    second, _ = Frame.parse_frame_header(memoryview(outbound[first_end : first_end + 9]))
    assert isinstance(first, SettingsFrame)
    assert isinstance(second, WindowUpdateFrame)
    assert second.stream_id == 0
    second.parse_body(memoryview(outbound[first_end + 9 :]))
    assert second.window_increment == 1024 * 1024 - 65535


@pytest.mark.parametrize("value", [65534, 2**31, True, 1024.0, None])
def test_invalid_initial_connection_window_is_rejected(value) -> None:
    """A window outside the legal connection range must be rejected."""
    with pytest.raises(ValueError, match="initial_connection_window_size"):
        H2Configuration(initial_connection_window_size=value)


def test_large_connection_window_supports_two_unacknowledged_streams() -> None:
    """The advertised window allows aggregate receiving beyond the default 64 KiB."""
    larger_window = 131072
    server = H2Connection(H2Configuration(client_side=False, initial_connection_window_size=larger_window))
    server.initiate_connection()
    client = H2Connection()
    client.initiate_connection()
    client.receive_data(server.data_to_send())
    headers = [(b":method", b"POST"), (b":scheme", b"https"), (b":authority", b"example.com"), (b":path", b"/")]
    for stream_id in (1, 3):
        client.send_headers(stream_id, headers)
        for _ in range(4):
            client.send_data(stream_id, b"x" * 15000)
    server.receive_data(client.data_to_send())
    assert server.inbound_flow_control_window == larger_window - 120000

    server.acknowledge_received_data(60000, 1)
    server.acknowledge_received_data(60000, 3)
    assert server.inbound_flow_control_window == larger_window
    assert server.data_to_send()
