import asyncio
import io
from typing import cast

import aiohttp
import pytest

import repligit.asyncio.client as aclient
from repligit import client
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

    async def read(self) -> bytes:
        chunk, self._buf = self._buf, b""
        return chunk

    async def readexactly(self, n: int) -> bytes:
        if len(self._buf) < n:
            partial, self._buf = self._buf, b""
            raise asyncio.IncompleteReadError(partial, n)
        chunk, self._buf = self._buf[:n], self._buf[n:]
        return chunk

    async def iter_any(self):
        if self._buf:
            chunk, self._buf = self._buf, b""
            yield chunk


def _collect_async_pkt_lines(raw: bytes) -> list[str]:
    stream = cast(aiohttp.StreamReader, _AsyncStream(raw))

    async def _collect() -> list[str]:
        return [line async for line in async_read_pkt_lines(stream)]

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


# A fake packfile body. Only the leading b"PACK" signature matters here:
# fetch_pack must return the stream positioned exactly at it.
_FAKE_PACK = b"PACK\x00\x00\x00\x02\x00\x00\x00\x01...packfile bytes..."

# Negotiation responses that fetch_pack must fully consume before the pack.
_NEGOTIATION_RESPONSES = [
    # Server has none of our haves.
    pytest.param(_pkt(b"NAK"), id="nak"),
    # multi-ack: intermediate status lines then a final bare ACK.
    pytest.param(
        _pkt(f"ACK {SHA_A} continue".encode())
        + _pkt(f"ACK {SHA_B} common".encode())
        + _pkt(f"ACK {SHA_B}".encode()),
        id="multi_ack",
    ),
    # Without multi-ack, some servers (e.g. GitHub) send one bare
    # "ACK <sha>" per common commit found. Treating the first bare ACK as
    # final made fetch_pack return the remaining ACK pkt-lines as if they
    # were the packfile, producing a corrupt pack whose upload failed with
    # "packfile signature mismatch" (regression test).
    pytest.param(
        _pkt(f"ACK {SHA_A}".encode()) + _pkt(f"ACK {SHA_B}".encode()),
        id="repeated_bare_acks",
    ),
    # A stray flush-pkt between negotiation and the pack must be skipped.
    pytest.param(_pkt(b"NAK") + b"0000", id="nak_then_flush"),
]


def _fetch_pack_sync(monkeypatch, raw: bytes) -> bytes | None:
    monkeypatch.setattr(client, "http_request", lambda *a, **kw: io.BytesIO(raw))
    resp = client.fetch_pack("http://x", SHA_A, [SHA_B])
    return None if resp is None else resp.read()


def _fetch_pack_async(monkeypatch, raw: bytes) -> bytes | None:
    class _Resp:
        def __init__(self):
            self.content = _AsyncStream(raw)

    class _Session:
        closed = False

        def __init__(self, *a, **kw):
            pass

        async def post(self, *a, **kw):
            return _Resp()

        async def close(self):
            _Session.closed = True

    monkeypatch.setattr(aclient.aiohttp, "ClientSession", _Session)

    async def _run() -> bytes | None:
        stream = await aclient.fetch_pack("http://x", SHA_A, [SHA_B])
        if stream is None:
            assert _Session.closed  # failed negotiation must close the session
            return None
        data = b"".join([chunk async for chunk in stream])
        assert _Session.closed  # stream exhaustion must close the session
        return data

    return asyncio.run(_run())


@pytest.mark.parametrize("negotiation", _NEGOTIATION_RESPONSES)
def test_fetch_pack_returns_exactly_the_packfile_sync(monkeypatch, negotiation):
    assert _fetch_pack_sync(monkeypatch, negotiation + _FAKE_PACK) == _FAKE_PACK


@pytest.mark.parametrize("negotiation", _NEGOTIATION_RESPONSES)
def test_fetch_pack_returns_exactly_the_packfile_async(monkeypatch, negotiation):
    assert _fetch_pack_async(monkeypatch, negotiation + _FAKE_PACK) == _FAKE_PACK


def test_fetch_pack_sync_stream_supports_short_reads(monkeypatch):
    # The returned stream replays the already-consumed b"PACK" signature;
    # make sure partial reads across that boundary are correct.
    monkeypatch.setattr(
        client, "http_request", lambda *a, **kw: io.BytesIO(_pkt(b"NAK") + _FAKE_PACK)
    )
    resp = client.fetch_pack("http://x", SHA_A, [SHA_B])
    assert resp is not None
    assert resp.read(2) == b"PA"
    assert resp.read(4) == b"CK\x00\x00"
    assert resp.read() == _FAKE_PACK[6:]


def test_send_pack_streams_sync_packfile(monkeypatch):
    sent = {}

    def _fake_request(*a, **kw):
        sent["data"] = kw["data"]
        return io.BytesIO(_pkt(b"unpack ok") + _pkt(b"ok refs/heads/main"))

    monkeypatch.setattr(client, "http_request", _fake_request)

    client.send_pack(
        "http://x", "refs/heads/main", SHA_A, SHA_B, io.BytesIO(b"PACK...bytes...")
    )

    # body must be an iterable of chunks (streamed), not pre-joined bytes
    assert not isinstance(sent["data"], bytes)
    header = generate_send_pack_header("refs/heads/main", SHA_A, SHA_B)
    assert b"".join(sent["data"]) == header + b"PACK...bytes..."


def test_send_pack_streams_async_packfile(monkeypatch):
    sent = {}

    class _Resp:
        content = _AsyncStream(_pkt(b"unpack ok") + _pkt(b"ok refs/heads/main"))

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class _Session:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        def post(self, *a, **kw):
            sent["data"] = kw["data"]
            return _Resp()

    monkeypatch.setattr(aclient.aiohttp, "ClientSession", _Session)

    async def _packfile():
        yield b"PACK"
        yield b"...bytes..."

    async def _run() -> bytes:
        await aclient.send_pack(
            "http://x", "refs/heads/main", SHA_A, SHA_B, _packfile()
        )
        return b"".join([chunk async for chunk in sent["data"]])

    body = asyncio.run(_run())
    header = generate_send_pack_header("refs/heads/main", SHA_A, SHA_B)
    assert body == header + b"PACK...bytes..."
