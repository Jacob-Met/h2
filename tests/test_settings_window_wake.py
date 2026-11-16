"""Peer SETTINGS changes should wake streams whose send credit increased."""
from __future__ import annotations

from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import RemoteSettingsChanged, WindowUpdated
from h2.settings import SettingCodes

REQUEST = [
    (b":method", b"POST"),
    (b":scheme", b"https"),
    (b":authority", b"example.com"),
    (b":path", b"/"),
]


def connected_peers() -> tuple[H2Connection, H2Connection]:
    """Exchange HTTP/2 prefaces and initial settings."""
    client = H2Connection()
    server = H2Connection(H2Configuration(client_side=False))
    client.initiate_connection()
    server.initiate_connection()
    server.receive_data(client.data_to_send())
    client.receive_data(server.data_to_send())
    return client, server


def test_initial_window_increase_wakes_all_open_streams() -> None:
    """SETTINGS growth delivers a per-stream wakeup, not a connection update."""
    client, server = connected_peers()
    client.send_headers(1, REQUEST)
    client.send_headers(3, REQUEST)
    server.receive_data(client.data_to_send())
    server.update_settings({SettingCodes.INITIAL_WINDOW_SIZE: 128000})
    events = client.receive_data(server.data_to_send())

    assert isinstance(events[0], RemoteSettingsChanged)
    assert [(event.stream_id, event.delta) for event in events[1:]] == [
        (1, 62465),
        (3, 62465),
    ]
    assert all(isinstance(event, WindowUpdated) for event in events[1:])
    assert client.outbound_flow_control_window == 65535


def test_window_increase_unblocks_stream_without_window_update_frame() -> None:
    """Applications following WindowUpdated can resume after SETTINGS alone."""
    client, server = connected_peers()
    client.send_headers(1, REQUEST)
    server.receive_data(client.data_to_send())
    client.send_data(1, b"x" * 15000)
    client.send_data(1, b"x" * 15000)
    server.receive_data(client.data_to_send())

    server.update_settings({SettingCodes.INITIAL_WINDOW_SIZE: 10000})
    decreased = client.receive_data(server.data_to_send())
    assert len(decreased) == 1
    assert isinstance(decreased[0], RemoteSettingsChanged)
    assert client.local_flow_control_window(1) == -20000

    server.update_settings({SettingCodes.INITIAL_WINDOW_SIZE: 40000})
    increased = client.receive_data(server.data_to_send())
    assert [(event.stream_id, event.delta) for event in increased[1:]] == [
        (1, 30000),
    ]
    assert client.local_flow_control_window(1) == 10000
    client.send_data(1, b"resumed", end_stream=True)
    assert server.receive_data(client.data_to_send())


def test_no_wakeup_for_unchanged_window_or_inactive_streams() -> None:
    """SETTINGS with no growth or no open streams does not invent wakeups."""
    client, server = connected_peers()
    server.update_settings({SettingCodes.INITIAL_WINDOW_SIZE: 65535})
    assert len(client.receive_data(server.data_to_send())) == 1

    client.send_headers(1, REQUEST)
    client.send_headers(3, REQUEST)
    client.reset_stream(3)
    server.receive_data(client.data_to_send())
    server.update_settings({SettingCodes.INITIAL_WINDOW_SIZE: 65536})
    events = client.receive_data(server.data_to_send())
    assert [(e.stream_id, e.delta) for e in events[1:]] == [(1, 1)]


def test_stream_window_wakeup_while_peer_drains_gracefully() -> None:
    """In-flight sends still receive SETTINGS wakeups during graceful GOAWAY."""
    client, server = connected_peers()
    client.send_headers(1, REQUEST)
    server.receive_data(client.data_to_send())
    server.close_connection(graceful=True, last_stream_id=1)
    client.receive_data(server.data_to_send())

    server.update_settings({SettingCodes.INITIAL_WINDOW_SIZE: 66000})
    events = client.receive_data(server.data_to_send())
    assert [(e.stream_id, e.delta) for e in events[1:]] == [(1, 465)]
    client.send_data(1, b"done", end_stream=True)
    assert server.receive_data(client.data_to_send())
