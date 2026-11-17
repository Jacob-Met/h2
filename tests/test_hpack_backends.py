"""Alternative HPACK implementations must preserve connection-wide compression state."""

from __future__ import annotations

from typing import Any

import pytest
from hpack import Decoder, Encoder
from hyperframe.frame import Frame

from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.errors import ErrorCodes
from h2.events import RequestReceived, ResponseReceived
from h2.exceptions import DenialOfServiceError
from h2.settings import SettingCodes


class InstrumentedEncoder:
    """A duck-typed HPACK encoder that is intentionally not an Encoder subclass."""

    def __init__(self) -> None:
        self._backend = Encoder()
        self.calls = 0

    @property
    def header_table_size(self) -> int:
        return self._backend.header_table_size

    @header_table_size.setter
    def header_table_size(self, value: int) -> None:
        self._backend.header_table_size = value

    def encode(self, headers: Any, huffman: bool = True) -> bytes:
        self.calls += 1
        return self._backend.encode(headers, huffman=huffman)


class InstrumentedDecoder:
    """A duck-typed HPACK decoder preserving per-connection limits and state."""

    def __init__(self) -> None:
        self._backend = Decoder(max_header_list_size=2**24)
        self.calls = 0

    @property
    def max_header_list_size(self) -> int:
        return self._backend.max_header_list_size

    @max_header_list_size.setter
    def max_header_list_size(self, value: int) -> None:
        self._backend.max_header_list_size = value

    @property
    def max_allowed_table_size(self) -> int:
        return self._backend.max_allowed_table_size

    @max_allowed_table_size.setter
    def max_allowed_table_size(self, value: int) -> None:
        self._backend.max_allowed_table_size = value

    def decode(self, data: bytes, raw: bool = False) -> Any:
        self.calls += 1
        return self._backend.decode(data, raw=raw)


class UnsafeDecoder(InstrumentedDecoder):
    """Pretends to accept the compression bomb protection, but drops the limit."""

    @property
    def max_header_list_size(self) -> int:
        return 2**24

    @max_header_list_size.setter
    def max_header_list_size(self, value: int) -> None:
        pass


def make_pair() -> tuple[H2Connection, H2Connection]:
    """Create a client and server with independent custom HPACK codecs."""
    factories = {"encoder_factory": InstrumentedEncoder, "decoder_factory": InstrumentedDecoder}
    client = H2Connection(H2Configuration(client_side=True, **factories))
    server = H2Connection(H2Configuration(client_side=False, **factories))
    client.initiate_connection()
    server.receive_data(client.data_to_send())
    server.initiate_connection()
    client.receive_data(server.data_to_send())
    return client, server


def test_custom_factories_real_request_response_and_hpack_state() -> None:
    """Verify real peer traffic and dynamic tables with alternative codecs."""
    client, server = make_pair()
    assert isinstance(client.encoder, InstrumentedEncoder)
    assert isinstance(server.decoder, InstrumentedDecoder)
    assert client.encoder is not server.encoder
    assert client.decoder is not server.decoder
    assert client.decoder.max_header_list_size == 2**16

    headers = [(b":method", b"GET"), (b":scheme", b"https"), (b":authority", b"example.com"), (b":path", b"/"), (b"x-reused", b"abc")]
    for stream_id in (1, 3, 5, 7):
        client.send_headers(stream_id, headers, end_stream=True)
        events = server.receive_data(client.data_to_send())
        assert [event.stream_id for event in events if isinstance(event, RequestReceived)] == [stream_id]
        server.send_headers(stream_id, [(b":status", b"200"), (b"x-reused", b"abc")], end_stream=True)
        events = client.receive_data(server.data_to_send())
        assert [event.stream_id for event in events if isinstance(event, ResponseReceived)] == [stream_id]
    assert client.encoder.calls == 4
    assert server.decoder.calls == 4
    assert server.encoder.calls == 4
    assert client.decoder.calls == 4

    client.update_settings({SettingCodes.HEADER_TABLE_SIZE: 128})
    server.receive_data(client.data_to_send())
    assert server.encoder.header_table_size == 128
    assert server.decoder.max_header_list_size == 2**16

    client.close_connection(error_code=ErrorCodes.NO_ERROR)
    server.receive_data(client.data_to_send())


def test_custom_configuration_injects_fresh_instance_per_connection() -> None:
    """Never reuse HPACK state across HTTP/2 connections."""
    config = H2Configuration(encoder_factory=InstrumentedEncoder, decoder_factory=InstrumentedDecoder)
    one, two = H2Connection(config), H2Connection(config)
    assert type(one.encoder) is InstrumentedEncoder
    assert type(two.decoder) is InstrumentedDecoder
    assert one.encoder is not two.encoder
    assert one.decoder is not two.decoder
    assert isinstance(H2Connection().encoder, Encoder)
    assert isinstance(H2Connection().decoder, Decoder)


@pytest.mark.parametrize("field", ["encoder_factory", "decoder_factory"])
def test_invalid_factory_is_not_silently_ignored(field: str) -> None:
    """Reject invalid backend configuration at construction time."""
    with pytest.raises((TypeError, ValueError), match=field):
        H2Configuration(**{field: object()})


def test_injected_decoder_cannot_ignore_header_list_security_limit() -> None:
    """Refuse decoders that evade the decompression bomb limit."""
    config = H2Configuration(decoder_factory=UnsafeDecoder)
    with pytest.raises((TypeError, ValueError), match="max_header_list_size"):
        H2Connection(config)


class UnsafeTableDecoder(InstrumentedDecoder):
    """Pretends to support HPACK dynamic-table limits, but ignores them."""

    @property
    def max_allowed_table_size(self) -> int:
        return 2**24

    @max_allowed_table_size.setter
    def max_allowed_table_size(self, value: int) -> None:
        pass


def test_injected_decoder_must_enforce_dynamic_table_limit() -> None:
    """A custom codec cannot bypass SETTINGS_HEADER_TABLE_SIZE limits."""
    with pytest.raises((TypeError, ValueError), match="max_allowed_table_size"):
        H2Connection(H2Configuration(decoder_factory=UnsafeTableDecoder))


def test_custom_decoder_tracks_local_settings_ack() -> None:
    """New limits from SETTINGS must reach the replacement decoder on ACK."""
    client, server = make_pair()
    client.update_settings({SettingCodes.MAX_HEADER_LIST_SIZE: 512, SettingCodes.HEADER_TABLE_SIZE: 128})
    server.receive_data(client.data_to_send())
    client.receive_data(server.data_to_send())
    assert client.decoder.max_header_list_size == 512
    assert client.decoder.max_allowed_table_size == 128


def test_malformed_factory_result_is_rejected() -> None:
    """Missing required codec methods must fail before sending any bytes."""
    with pytest.raises(TypeError, match="encoder_factory"):
        H2Connection(H2Configuration(encoder_factory=object))
    with pytest.raises(TypeError, match="decoder_factory"):
        H2Connection(H2Configuration(decoder_factory=object))


def test_injected_decoder_rejects_oversized_header_block() -> None:
    """An alternative decoder still converts HPACK bomb failures into DoS errors."""
    client, server = make_pair()
    client.send_headers(1, [(b":method", b"GET"), (b":scheme", b"https"), (b":authority", b"example.com"), (b":path", b"/")], end_stream=True)
    server.receive_data(client.data_to_send())
    server.send_headers(1, [(b":status", b"200"), (b"x-large", b"x" * (2**16))], end_stream=True)
    with pytest.raises(DenialOfServiceError):
        client.receive_data(server.data_to_send())


def test_custom_decoder_supports_configured_unicode_headers() -> None:
    """Alternative decoders preserve hpack HeaderTuple for unicode responses."""
    client = H2Connection(
        H2Configuration(
            client_side=True,
            header_encoding="utf-8",
            encoder_factory=InstrumentedEncoder,
            decoder_factory=InstrumentedDecoder,
        ),
    )
    server = H2Connection(H2Configuration(client_side=False))
    client.initiate_connection()
    server.receive_data(client.data_to_send())
    server.initiate_connection()
    client.receive_data(server.data_to_send())
    client.send_headers(1, [(b":method", b"GET"), (b":scheme", b"https"), (b":authority", b"example.com"), (b":path", b"/")], end_stream=True)
    server.receive_data(client.data_to_send())
    server.send_headers(1, [(b":status", b"200"), (b"x-header", "héllo".encode())], end_stream=True)
    events = client.receive_data(server.data_to_send())
    responses = [event for event in events if isinstance(event, ResponseReceived)]
    assert len(responses) == 1
    assert responses[0].headers == [(":status", "200"), ("x-header", "héllo")]


def test_custom_encoder_respects_never_index_sensitive_headers() -> None:
    """A compatible codec keeps HTTP Authorization out of the HPACK table."""
    client = H2Connection(H2Configuration(encoder_factory=InstrumentedEncoder))
    client.initiate_connection()
    client.clear_outbound_data_buffer()
    client.send_headers(
        1,
        [
            (b":method", b"GET"),
            (b":scheme", b"https"),
            (b":authority", b"example.com"),
            (b":path", b"/"),
            (b"authorization", b"Bearer very-private-token"),
        ],
        end_stream=True,
    )
    payload = client.data_to_send()
    frame, length = Frame.parse_frame_header(memoryview(payload[:9]))
    frame.parse_body(memoryview(payload[9 : 9 + length]))
    decoder = Decoder()
    assert (b"authorization", b"Bearer very-private-token") in decoder.decode(frame.data, raw=True)
    assert all(name != b"authorization" for name, _ in decoder.header_table.dynamic_entries)


def needs_argument(required: object) -> None:
    """An intentionally invalid factory used for signature validation."""
    raise AssertionError(required)


@pytest.mark.parametrize("field", ["encoder_factory", "decoder_factory"])
def test_factory_constructor_errors_name_the_bad_setting(field: str) -> None:
    """Tell callers which configured factory violates the zero-argument contract."""
    with pytest.raises(TypeError, match=field):
        H2Connection(H2Configuration(**{field: needs_argument}))
