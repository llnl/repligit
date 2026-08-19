import asyncio
import io
from typing import cast

import aiohttp
import pytest

from repligit.asyncio.parse import read_pkt_lines as async_read_pkt_lines
from repligit.exceptions import RemoteError, UnexpectedResponse
from repligit.parse import (
    encode_lines,
    generate_send_pack_header,
    read_pkt_lines,
)


def _pkt(payload: bytes) -> bytes:
    """Encode a single git pkt-line."""
    return f"{len(payload) + 5:04x}".encode() + payload + b"\n"


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


def _async_stream(data: bytes) -> aiohttp.StreamReader:
    return cast(aiohttp.StreamReader, _AsyncStream(data))


def _collect_async_pkt_lines(raw: bytes) -> list[str]:
    async def _collect() -> list[str]:
        return [line async for line in async_read_pkt_lines(_async_stream(raw))]

    return asyncio.run(_collect())


_ERR_RESPONSE = _pkt(b"ERR upload-pack: not our ref " + SHA_A.encode())


# A realistic git-upload-pack ref advertisement. Note the flush packet after
# the service line carries no trailing newline, and the first ref line embeds
# the server capabilities after a NUL byte.
_ADVERTISEMENT = (
    _pkt(b"# service=git-upload-pack")
    + b"0000"
    + _pkt(
        f"{SHA_A} HEAD\x00multi_ack thin-pack side-band-64k "
        f"symref=HEAD:refs/heads/main".encode()
    )
    + _pkt(f"{SHA_A} refs/heads/main".encode())
    + _pkt(f"{SHA_B} refs/tags/v1".encode())
    + b"0000"
)

_ADVERTISEMENT_LINES = [
    "# service=git-upload-pack",
    f"{SHA_A} HEAD\x00multi_ack thin-pack side-band-64k symref=HEAD:refs/heads/main",
    f"{SHA_A} refs/heads/main",
    f"{SHA_B} refs/tags/v1",
]


def test_read_pkt_lines_frames_by_length_sync():
    # Flush packets are skipped and one trailing LF is stripped per payload.
    lines = list(read_pkt_lines(io.BytesIO(_ADVERTISEMENT)))
    assert lines == _ADVERTISEMENT_LINES


def test_read_pkt_lines_frames_by_length_async():
    assert _collect_async_pkt_lines(_ADVERTISEMENT) == _ADVERTISEMENT_LINES


# Malformed or truncated pkt-line streams that must raise UnexpectedResponse
# rather than yielding truncated or garbage lines.
_BAD_PKT_STREAMS = [
    # Connection dropped mid-length-prefix.
    pytest.param(_pkt(b"unpack ok") + b"00", id="truncated_prefix"),
    # Connection dropped mid-pkt-line payload.
    pytest.param(_pkt(b"unpack ok")[:8], id="truncated_payload"),
    # Length prefix is not hex.
    pytest.param(b"zzzz" + _pkt(b"unpack ok"), id="garbage_prefix"),
    # Lengths 1-3 can never fit the 4-byte prefix itself. Notably, "0002"
    # previously triggered read(-2), swallowing the rest of the stream.
    pytest.param(b"0002" + _pkt(b"unpack ok"), id="bogus_length"),
]


@pytest.mark.parametrize("raw", _BAD_PKT_STREAMS)
def test_read_pkt_lines_sync_raises_on_malformed(raw):
    with pytest.raises(UnexpectedResponse):
        list(read_pkt_lines(io.BytesIO(raw)))


@pytest.mark.parametrize("raw", _BAD_PKT_STREAMS)
def test_read_pkt_lines_async_raises_on_malformed(raw):
    with pytest.raises(UnexpectedResponse):
        _collect_async_pkt_lines(raw)


def test_read_pkt_lines_sync_raises_on_err():
    with pytest.raises(RemoteError):
        list(read_pkt_lines(io.BytesIO(_ERR_RESPONSE)))


def test_read_pkt_lines_async_raises_on_err():
    with pytest.raises(RemoteError):
        _collect_async_pkt_lines(_ERR_RESPONSE)


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
