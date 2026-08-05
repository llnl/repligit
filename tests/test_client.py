import asyncio
import io
from typing import cast

import aiohttp
import pytest

from repligit.asyncio.parse import read_packfile as async_read_packfile
from repligit.exceptions import RemoteError, UnexpectedResponse
from repligit.parse import (
    decode_lines,
    encode_lines,
    generate_send_pack_header,
    read_packfile,
)


def _pkt(payload: bytes) -> bytes:
    """Encode a single git pkt-line."""
    return f"{len(payload) + 5:04x}".encode() + payload + b"\n"


# A minimal but valid packfile body: "PACK", version 2, zero objects, trailer.
FAKE_PACK = b"PACK\x00\x00\x00\x02\x00\x00\x00\x00" + b"\x00" * 20

SHA_A = "a" * 40
SHA_B = "b" * 40


class _AsyncStream:
    """Minimal async byte reader mimicking aiohttp's StreamReader."""

    def __init__(self, data: bytes):
        self._buf = data

    async def readexactly(self, n: int) -> bytes:
        if len(self._buf) < n:
            partial, self._buf = self._buf, b""
            raise asyncio.IncompleteReadError(partial, n)
        chunk, self._buf = self._buf[:n], self._buf[n:]
        return chunk

    async def read(self, n: int = -1) -> bytes:
        if n < 0:
            chunk, self._buf = self._buf, b""
            return chunk
        chunk, self._buf = self._buf[:n], self._buf[n:]
        return chunk


def _run_async(data: bytes) -> bytes | None:
    reader = cast(aiohttp.StreamReader, _AsyncStream(data))
    return asyncio.run(async_read_packfile(reader))


# Responses exercised by both the sync and async read_packfile helpers.
_PACKFILE_CASES = [
    # Single NAK line immediately followed by the pack.
    pytest.param(_pkt(b"NAK") + FAKE_PACK, FAKE_PACK, id="nak"),
    # Single terminal ACK line followed by the pack.
    pytest.param(_pkt(f"ACK {SHA_A}".encode()) + FAKE_PACK, FAKE_PACK, id="single_ack"),
    # Regression: multiple ACK lines precede the pack (multi-ack negotiation as
    # emitted by GitHub/GitLab). Previously only the first line was consumed,
    # leaving "ACK ..." bytes in front of the pack and corrupting its signature.
    pytest.param(
        _pkt(f"ACK {SHA_A} common".encode())
        + _pkt(f"ACK {SHA_B} common".encode())
        + _pkt(f"ACK {SHA_B}".encode())
        + FAKE_PACK,
        FAKE_PACK,
        id="multi_ack",
    ),
    # Shallow lines and a flush packet precede the pack.
    pytest.param(
        _pkt(f"shallow {SHA_A}".encode()) + b"0000" + _pkt(b"NAK") + FAKE_PACK,
        FAKE_PACK,
        id="shallow_and_flush",
    ),
    # Negotiation only, no packfile (stream ends): returns None.
    pytest.param(_pkt(b"NAK"), None, id="no_pack"),
]

_ERR_RESPONSE = _pkt(b"ERR upload-pack: not our ref " + SHA_A.encode())

# Malformed or truncated responses that must raise UnexpectedResponse rather
# than being silently misread as "no packfile".
_BAD_RESPONSES = [
    # Connection dropped mid-length-prefix.
    pytest.param(_pkt(b"NAK") + b"00", id="truncated_prefix"),
    # Connection dropped mid-pkt-line payload.
    pytest.param(_pkt(f"ACK {SHA_A}".encode())[:20], id="truncated_line"),
    # Length prefix is not hex (and not "PACK").
    pytest.param(b"zzzz" + FAKE_PACK, id="garbage_prefix"),
    # Lengths 1-3 can never denote a valid pkt-line in this response.
    pytest.param(b"0002" + _pkt(b"NAK") + FAKE_PACK, id="bogus_length"),
]


@pytest.mark.parametrize("raw,expected", _PACKFILE_CASES)
def test_read_packfile_sync(raw, expected):
    stream = read_packfile(io.BytesIO(raw))
    if expected is None:
        assert stream is None
    else:
        assert stream is not None
        assert stream.read() == expected


def test_read_packfile_sync_streams_incrementally():
    """The consumed PACK signature is replayed and reads pass through."""
    stream = read_packfile(io.BytesIO(_pkt(b"NAK") + FAKE_PACK))
    assert stream is not None
    assert stream.read(2) == b"PA"
    assert stream.read(6) == b"CK\x00\x00\x00\x02"
    assert stream.read() == FAKE_PACK[8:]
    assert stream.read() == b""


def test_read_packfile_sync_close_propagates():
    """Closing the packfile stream closes the underlying response stream."""
    underlying = io.BytesIO(_pkt(b"NAK") + FAKE_PACK)
    stream = read_packfile(underlying)
    assert stream is not None
    stream.close()
    assert underlying.closed


@pytest.mark.parametrize("raw,expected", _PACKFILE_CASES)
def test_read_packfile_async(raw, expected):
    assert _run_async(raw) == expected


def test_read_packfile_sync_raises_on_err():
    with pytest.raises(RemoteError):
        read_packfile(io.BytesIO(_ERR_RESPONSE))


def test_read_packfile_async_raises_on_err():
    with pytest.raises(RemoteError):
        _run_async(_ERR_RESPONSE)


@pytest.mark.parametrize("raw", _BAD_RESPONSES)
def test_read_packfile_sync_raises_on_malformed(raw):
    with pytest.raises(UnexpectedResponse):
        read_packfile(io.BytesIO(raw))


@pytest.mark.parametrize("raw", _BAD_RESPONSES)
def test_read_packfile_async_raises_on_malformed(raw):
    with pytest.raises(UnexpectedResponse):
        _run_async(raw)


def test_decode_lines():
    raw_lines = [
        "003fbef547a59eec448284136f03984dce0f2f8239a9 refs/pull/95/head",
        "003f358aa046cd57dbca306e80d4c3fbb86edc5b36af refs/pull/96/head",
        "0000",
    ]

    decoded_lines = [
        "bef547a59eec448284136f03984dce0f2f8239a9 refs/pull/95/head",
        "358aa046cd57dbca306e80d4c3fbb86edc5b36af refs/pull/96/head",
        "",
    ]

    lines = list(decode_lines(raw_lines))
    assert decoded_lines == lines


def test_encode_lines_from_bytes():
    input_lines = [
        b"bef547a59eec448284136f03984dce0f2f8239a9 refs/pull/95/head",
        b"358aa046cd57dbca306e80d4c3fbb86edc5b36af refs/pull/96/head",
    ]

    encoded_lines = (
        b"003fbef547a59eec448284136f03984dce0f2f8239a9 refs/pull/95/head\n"
        b"003f358aa046cd57dbca306e80d4c3fbb86edc5b36af refs/pull/96/head\n"
    )

    output_lines = encode_lines(input_lines)
    assert encoded_lines == output_lines


def test_encode_lines_from_str():
    input_lines = [
        "bef547a59eec448284136f03984dce0f2f8239a9 refs/pull/95/head",
        "358aa046cd57dbca306e80d4c3fbb86edc5b36af refs/pull/96/head",
    ]

    encoded_lines = (
        b"003fbef547a59eec448284136f03984dce0f2f8239a9 refs/pull/95/head\n"
        b"003f358aa046cd57dbca306e80d4c3fbb86edc5b36af refs/pull/96/head\n"
    )

    output_lines = encode_lines(input_lines)
    assert encoded_lines == output_lines


def test_generate_send_pack_header():
    expected_header = (
        b"0075aed5561af12f75f0b6b6ca34082610eaba109db7"
        b" b03ab96b18ed6633c877221318db41d36b15e3d7"
        b" refs/heads/main\x00 report-status\n0000"
    )

    from_sha = "aed5561af12f75f0b6b6ca34082610eaba109db7"
    to_sha = "b03ab96b18ed6633c877221318db41d36b15e3d7"
    ref = "refs/heads/main"

    output_header = generate_send_pack_header(ref, from_sha, to_sha)
    assert expected_header == output_header
