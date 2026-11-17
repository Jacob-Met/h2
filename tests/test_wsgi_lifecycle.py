"""HTTP/2 WSGI example stream lifetime regressions."""

from __future__ import annotations

import asyncio
import hashlib
import threading
from pathlib import Path
from types import SimpleNamespace

import h2.connection
import h2.events
from h2.settings import SettingCodes


def _module() -> SimpleNamespace:
    path = Path(__file__).parents[1] / "examples" / "asyncio" / "wsgi-server.py"
    source = path.read_text().split("\n# Set up the WSGI app.", 1)[0]
    namespace = {"__file__": str(path), "__name__": "_h2_wsgi_example"}
    exec(compile(source, str(path), "exec"), namespace)  # noqa: S102
    return SimpleNamespace(**namespace)


class MemoryTransport:
    def __init__(self) -> None:
        self.output = bytearray()

    def write(self, data: bytes) -> None:
        self.output.extend(data)


def test_reset_discards_queued_end_without_killing_every_stream() -> None:
    """A reset END sentinel cannot crash the shared writer loop."""

    async def exercise() -> None:
        module = _module()
        protocol = module.H2Protocol()
        protocol.connection_made(MemoryTransport())
        finished = threading.Event()
        protocol._reset_streams.add(1)
        try:
            protocol._stream_data.put_nowait((1, module.END_DATA_SENTINEL, finished))
            await asyncio.sleep(0)
            assert finished.is_set()
            assert not protocol._send_loop_task.done(), "reset request killed the connection's writer loop"
        finally:
            protocol.connection_lost(None)
            await asyncio.sleep(0)

    asyncio.run(exercise())


def test_reset_unblocks_flow_control_stalled_wsgi_writer() -> None:
    """Release the blocked writer when a peer resets the stream."""

    async def exercise() -> None:
        module = _module()
        protocol = module.H2Protocol()
        protocol.connection_made(MemoryTransport())
        client = h2.connection.H2Connection()
        client.initiate_connection()
        client.update_settings({SettingCodes.INITIAL_WINDOW_SIZE: 0})
        client.send_headers(
            1,
            [
                (b":method", b"POST"),
                (b":scheme", b"https"),
                (b":authority", b"example.org"),
                (b":path", b"/"),
            ],
            end_stream=True,
        )
        protocol.conn.receive_data(client.data_to_send())
        protocol.streams[1] = module.Stream(1, protocol)
        finished = protocol.data_for_stream(1, b"pending")
        for _ in range(20):
            if 1 in protocol._flow_controlled_data:
                break
            await asyncio.sleep(0)
        assert 1 in protocol._flow_controlled_data
        try:
            client.reset_stream(1)
            protocol.data_received(client.data_to_send())
            assert finished.is_set(), "WSGI worker still hangs after reset"
            assert 1 not in protocol._flow_controlled_data
        finally:
            protocol.connection_lost(None)
            await asyncio.sleep(0)

    asyncio.run(exercise())


def test_late_reset_after_retirement_does_not_kill_connection() -> None:
    """A late reset cannot access an already-retired request."""

    async def exercise() -> None:
        module = _module()
        protocol = module.H2Protocol()
        protocol.connection_made(MemoryTransport())
        try:
            protocol.reset_stream(SimpleNamespace(stream_id=1))
            assert not protocol._send_loop_task.done()
        finally:
            protocol.connection_lost(None)
            await asyncio.sleep(0)

    asyncio.run(exercise())


def test_wsgi_example_concurrent_binary_posts_deliver_exact_distinct_bodies() -> None:  # noqa: C901
    """Actual h2 DATA framing must not corrupt or confuse concurrent requests."""

    async def exercise() -> None:
        module = _module()
        inputs = {
            1: bytes(range(256)) * 19,
            3: b"\x00\xff" * 3007,
            5: b"alpha\n" * 1201,
            7: bytes(range(255, -1, -1)) * 25,
        }
        expected = {str(key): hashlib.sha256(body).hexdigest().encode("ascii") for key, body in inputs.items()}

        def application(environ, start_response) -> list[bytes]:
            body_size = int(environ["CONTENT_LENGTH"])
            data = environ["wsgi.input"].read(body_size)
            assert len(data) == body_size
            start_response("200 OK", [("content-type", "text/plain")])
            return [hashlib.sha256(data).hexdigest().encode("ascii")]

        module.H2Protocol.request_received.__globals__["APPLICATION"] = application
        protocol = module.H2Protocol()
        transport = MemoryTransport()
        protocol.connection_made(transport)
        client = h2.connection.H2Connection()
        client.initiate_connection()

        for stream_id, body in inputs.items():
            headers = [
                (b":method", b"POST"),
                (b":scheme", b"https"),
                (b":authority", b"example.org"),
                (b":path", f"/{stream_id}".encode("ascii")),
                (b"content-length", str(len(body)).encode("ascii")),
            ]
            client.send_headers(stream_id, headers)
            for offset in range(0, len(body), 1253):
                chunk = body[offset : offset + 1253]
                client.send_data(stream_id, chunk, end_stream=(offset + len(chunk) == len(body)))
        protocol.data_received(client.data_to_send())
        cursor = 0
        received = {str(key): bytearray() for key in inputs}
        completed = set()
        try:
            for _ in range(1000):
                await asyncio.sleep(0.001)
                output = bytes(transport.output[cursor:])
                cursor = len(transport.output)
                if output:
                    for event in client.receive_data(output):
                        if isinstance(event, h2.events.DataReceived):
                            received[str(event.stream_id)] += event.data
                        if isinstance(event, h2.events.StreamEnded):
                            completed.add(event.stream_id)
                if len(completed) == len(inputs):
                    break
            assert completed == set(inputs)
            assert {sid: bytes(data) for sid, data in received.items()} == expected
            assert not protocol.streams
            assert not protocol._pending_events
        finally:
            protocol.connection_lost(None)
            await asyncio.sleep(0)

    asyncio.run(exercise())


def test_connection_loss_releases_flow_control_blocked_writer() -> None:
    """Connection shutdown must not strand a WSGI writer thread."""

    async def exercise() -> None:
        module = _module()
        protocol = module.H2Protocol()
        protocol.connection_made(MemoryTransport())
        event = protocol._new_write_event(1)
        assert not event.is_set()
        protocol.connection_lost(None)
        assert event.is_set()
        assert not protocol._pending_events
        assert protocol._new_write_event(3).is_set()
        await asyncio.sleep(0)

    asyncio.run(exercise())


def test_reset_midflight_keeps_unrelated_response_alive() -> None:  # noqa: C901
    """A cancelled WSGI request must not terminate another stream's writer."""

    async def exercise() -> None:
        module = _module()

        def application(environ, start_response) -> list[bytes]:
            if environ["PATH_INFO"] == "/cancel":
                environ["wsgi.input"].read()
                payload = b"cancelled"
            else:
                payload = b"live stream"
            start_response("200 OK", [("content-type", "text/plain")])
            return [payload]

        module.H2Protocol.request_received.__globals__["APPLICATION"] = application
        protocol = module.H2Protocol()
        transport = MemoryTransport()
        protocol.connection_made(transport)
        client = h2.connection.H2Connection()
        client.initiate_connection()
        client.send_headers(
            1,
            [
                (b":method", b"POST"),
                (b":scheme", b"https"),
                (b":authority", b"example.org"),
                (b":path", b"/cancel"),
            ],
        )
        client.reset_stream(1)
        client.send_headers(
            3,
            [
                (b":method", b"GET"),
                (b":scheme", b"https"),
                (b":authority", b"example.org"),
                (b":path", b"/live"),
            ],
            end_stream=True,
        )
        protocol.data_received(client.data_to_send())
        cursor = 0
        body = bytearray()
        ended = False
        try:
            for _ in range(1000):
                await asyncio.sleep(0.001)
                output = bytes(transport.output[cursor:])
                cursor = len(transport.output)
                for event in client.receive_data(output) if output else ():
                    if isinstance(event, h2.events.DataReceived) and event.stream_id == 3:
                        body += event.data
                    if isinstance(event, h2.events.StreamEnded) and event.stream_id == 3:
                        ended = True
                if ended:
                    break
            assert ended
            assert bytes(body) == b"live stream"
            assert not protocol._send_loop_task.done()
            for _ in range(500):
                if not protocol.streams:
                    break
                await asyncio.sleep(0.001)
            assert not protocol.streams
            assert not protocol._pending_events
        finally:
            protocol.connection_lost(None)
            await asyncio.sleep(0)

    asyncio.run(exercise())
