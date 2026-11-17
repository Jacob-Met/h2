"""Flow-control wakeups caused by remote SETTINGS frames."""

from __future__ import annotations

import pytest

import h2.connection
import h2.events
import h2.exceptions
import h2.settings


def test_settings_window_changes_emit_stream_updates(frame_factory) -> None:
    """Remote SETTINGS changes unblock existing streams, even without WINDOW_UPDATE."""
    c = h2.connection.H2Connection()
    headers = [(":authority", "example.com"), (":path", "/"), (":scheme", "https"), (":method", "GET")]
    c.send_headers(1, headers)
    c.send_headers(3, headers)

    for size, delta in [(0, -65535), (1000, 1000)]:
        f = frame_factory.build_settings_frame(
            settings={h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: size},
        )
        events = c.receive_data(f.serialize())
        updates = [event for event in events if isinstance(event, h2.events.WindowUpdated)]
        assert [(event.stream_id, event.delta) for event in updates] == [(1, delta), (3, delta)]
        assert c._get_stream_by_id(1).outbound_flow_control_window == size
        assert c._get_stream_by_id(3).outbound_flow_control_window == size
        assert isinstance(events[0], h2.events.RemoteSettingsChanged)
        if size == 0:
            with pytest.raises(h2.exceptions.FlowControlError):
                c.send_data(1, b"hello")
        else:
            c.send_data(1, b"hello")

    other = frame_factory.build_settings_frame(
        settings={h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 10},
    )
    assert not any(isinstance(event, h2.events.WindowUpdated) for event in c.receive_data(other.serialize()))


def test_closed_streams_do_not_receive_window_updated_events(frame_factory) -> None:
    """Never wake senders on closed streams when INITIAL_WINDOW_SIZE changes."""
    c = h2.connection.H2Connection()
    headers = [(":authority", "example.com"), (":path", "/"), (":scheme", "https"), (":method", "GET")]
    c.send_headers(1, headers)
    c.send_headers(3, headers)
    c.reset_stream(1)
    assert not c.streams[1].open

    frame = frame_factory.build_settings_frame(
        settings={h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 100000},
    )
    events = c.receive_data(frame.serialize())
    updates = [event for event in events if isinstance(event, h2.events.WindowUpdated)]
    assert [(event.stream_id, event.delta) for event in updates] == [(3, 34465)]
