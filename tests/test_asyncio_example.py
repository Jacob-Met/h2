"""Exercise the shipped asyncio example with actual HTTP/2 frames."""
from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path

import h2.connection
import h2.events


def _example_protocol():
    path = Path(__file__).parents[1] / "examples" / "asyncio" / "asyncio-server.py"
    spec = importlib.util.spec_from_file_location("h2_asyncio_example", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.H2Protocol


class MemoryTransport:
    def __init__(self):
        self.output = bytearray()
        self.closed = False

    def write(self, data):
        self.output += data

    def close(self):
        self.closed = True


def _request(method, path=b"/"):
    return [
        (b":method", method),
        (b":scheme", b"https"),
        (b":authority", b"example.org"),
        (b":path", path),
    ]


def test_asyncio_example_head_ends_on_headers_and_does_not_retain_body():
    protocol = _example_protocol()()
    transport = MemoryTransport()
    protocol.connection_made(transport)

    client = h2.connection.H2Connection()
    client.initiate_connection()
    client.send_headers(1, _request(b"HEAD"), end_stream=True)
    protocol.data_received(client.data_to_send())

    events = client.receive_data(bytes(transport.output))
    assert any(isinstance(event, h2.events.ResponseReceived) for event in events)
    assert any(isinstance(event, h2.events.StreamEnded) for event in events)
    assert not any(isinstance(event, h2.events.DataReceived) for event in events)
    assert not protocol.stream_data


def test_asyncio_example_many_streams_release_request_bodies():
    async def exercise():
        protocol = _example_protocol()()
        transport = MemoryTransport()
        protocol.connection_made(transport)

        client = h2.connection.H2Connection()
        client.initiate_connection()
        for stream_id in range(1, 161, 2):
            client.send_headers(stream_id, _request(b"GET"), end_stream=True)

        protocol.data_received(client.data_to_send())
        assert not protocol.stream_data
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        events = client.receive_data(bytes(transport.output))
        assert sum(isinstance(event, h2.events.ResponseReceived) for event in events) == 80
        assert sum(isinstance(event, h2.events.StreamEnded) for event in events) == 80
        assert not protocol.stream_data
        assert not protocol.flow_control_futures
        assert not protocol.response_tasks

        client.send_headers(161, _request(b"POST"), end_stream=False)
        protocol.data_received(client.data_to_send())
        assert 161 in protocol.stream_data
        client.reset_stream(161)
        protocol.data_received(client.data_to_send())
        assert 161 not in protocol.stream_data
        protocol.connection_lost(None)
        assert not protocol.stream_data

    asyncio.run(exercise())


def test_asyncio_example_reset_in_same_batch_as_end_does_not_crash():
    protocol = _example_protocol()()
    transport = MemoryTransport()
    protocol.connection_made(transport)

    client = h2.connection.H2Connection()
    client.initiate_connection()
    client.send_headers(1, _request(b"HEAD"), end_stream=True)
    client.reset_stream(1)
    protocol.data_received(client.data_to_send())
    assert not protocol.stream_data
    assert not transport.closed


def test_asyncio_example_reset_cancels_flow_control_blocked_response():
    async def exercise():
        from h2.settings import SettingCodes

        protocol = _example_protocol()()
        transport = MemoryTransport()
        protocol.connection_made(transport)

        client = h2.connection.H2Connection()
        client.initiate_connection()
        client.update_settings({SettingCodes.INITIAL_WINDOW_SIZE: 0})
        client.send_headers(1, _request(b"GET"), end_stream=True)
        protocol.data_received(client.data_to_send())

        await asyncio.sleep(0)
        assert 1 in protocol.flow_control_futures
        assert 1 in protocol.response_tasks

        client.reset_stream(1)
        protocol.data_received(client.data_to_send())
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert not protocol.stream_data
        assert not protocol.flow_control_futures
        assert not protocol.response_tasks

    asyncio.run(exercise())
