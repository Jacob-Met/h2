"""The asyncio server example must handle non-UTF-8 request data."""
from __future__ import annotations

import asyncio
import base64
import importlib.util
import json
from pathlib import Path

from h2.connection import H2Connection
from h2.events import DataReceived, StreamEnded


class MemoryTransport:
    """Capture an asyncio.Protocol's HTTP/2 output."""

    def __init__(self) -> None:
        self.output = bytearray()

    def write(self, data: bytes) -> None:
        self.output.extend(data)

    def close(self) -> None:
        pass


def test_asyncio_example_interleaved_text_and_binary_requests() -> None:
    """Binary payloads cannot crash processing for other HTTP/2 streams."""
    async def exercise() -> None:
        source = Path(__file__).parents[1] / "examples" / "asyncio" / "asyncio-server.py"
        spec = importlib.util.spec_from_file_location("h2_asyncio_binary_example", source)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        protocol = module.H2Protocol()
        transport = MemoryTransport()
        protocol.connection_made(transport)

        client = H2Connection()
        client.initiate_connection()
        for stream_id, path in ((1, b"/text"), (3, b"/binary")):
            client.send_headers(stream_id, [
                (b":method", b"POST"),
                (b":scheme", b"https"),
                (b":authority", b"example.org"),
                (b":path", path),
            ])
        client.send_data(1, b"hello ")
        client.send_data(3, b"\x00\xff")
        client.send_data(1, b"world", end_stream=True)
        client.send_data(3, b"\xfe\x80", end_stream=True)
        protocol.data_received(client.data_to_send())
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        responses: dict[int, bytearray] = {}
        ended = set()
        for event in client.receive_data(bytes(transport.output)):
            if isinstance(event, DataReceived):
                responses.setdefault(event.stream_id, bytearray()).extend(event.data)
            elif isinstance(event, StreamEnded):
                ended.add(event.stream_id)
        assert ended == {1, 3}
        assert json.loads(responses[1])["body"] == "hello world"
        binary = json.loads(responses[3])
        assert binary["body_encoding"] == "base64"
        assert base64.b64decode(binary["body"]) == b"\x00\xff\xfe\x80"

    asyncio.run(exercise())
