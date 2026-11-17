"""Regression cases for partially draining the outbound wire buffer."""

from __future__ import annotations

import random

from h2.connection import H2Connection


def test_partial_drain_interleaved_with_frames() -> None:
    """Queued frames remain in wire order across short reads."""
    conn = H2Connection()
    oracle = H2Connection()
    conn.initiate_connection()
    oracle.initiate_connection()
    initial = oracle.data_to_send()

    assert conn.data_to_send(7) == initial[:7]
    conn.ping(b"12345678")
    oracle.ping(b"12345678")
    expected = initial[7:] + oracle.data_to_send()
    chunks = [conn.data_to_send(3), conn.data_to_send(13), conn.data_to_send(4)]
    chunks.append(conn.data_to_send())
    assert b"".join(chunks) == expected
    assert conn.data_to_send() == b""


def test_many_partial_drains_with_append_and_clear() -> None:
    """Large partial writes, appends, and clears never lose wire bytes."""
    conn = H2Connection()
    conn._data_to_send.extend(bytes(range(256)) * 16384)
    expected = bytes(conn._data_to_send)

    pieces = [conn.data_to_send(4096) for _ in range(512)]
    conn._data_to_send.extend(b"tail")
    pieces.extend(conn.data_to_send(4096) for _ in range(512))
    assert b"".join(pieces) == expected
    assert conn.data_to_send() == b"tail"

    conn._data_to_send.extend(b"replace-old")
    assert conn.data_to_send(4) == b"repl"
    conn.clear_outbound_data_buffer()
    conn._data_to_send.extend(b"new")
    assert conn.data_to_send() == b"new"
    assert conn.data_to_send(1) == b""


def test_zero_amount_does_not_discard_pending_bytes() -> None:
    """A zero-byte request retains the unread contents."""
    conn = H2Connection()
    conn._data_to_send.extend(b"abcdef")
    assert conn.data_to_send(2) == b"ab"
    assert conn.data_to_send(0) == b""
    conn._data_to_send.extend(b"gh")
    assert conn.data_to_send(3) == b"cde"
    assert conn.data_to_send() == b"fgh"


def test_negative_amount_retains_slice_semantics() -> None:
    """Negative amounts retain the historical slicing semantics."""
    conn = H2Connection()
    conn._data_to_send.extend(b"abcdef")
    assert conn.data_to_send(-2) == b"abcd"
    assert conn.data_to_send(-3) == b""
    assert conn.data_to_send() == b"ef"


def test_randomized_partial_drains_match_bytearray_oracle() -> None:
    """A long mixed send/read trace must preserve all unread wire bytes."""
    rng = random.Random(2006)  # noqa: S311 - repeatable test input
    conn = H2Connection()
    oracle = bytearray()

    for _ in range(4000):
        choice = rng.randrange(10)
        if choice < 4:
            payload = rng.randbytes(rng.randrange(1, 600))
            conn._data_to_send.extend(payload)
            oracle.extend(payload)
        elif choice == 9:
            conn.clear_outbound_data_buffer()
            oracle.clear()
        else:
            amount = rng.choice((None, -2, 0, 1, 7, 300, 1000, 6000))
            if amount is None:
                expected = bytes(oracle)
                oracle.clear()
            else:
                expected = bytes(oracle[:amount])
                del oracle[:amount]
            assert conn.data_to_send(amount) == expected

    assert conn.data_to_send() == bytes(oracle)
