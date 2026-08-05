import asyncio
from collections.abc import AsyncIterator

import aiohttp

from repligit.exceptions import RemoteError, UnexpectedResponse
from repligit.parse import parse_pkt_length


async def _read_pkt_prefix(reader: aiohttp.StreamReader) -> bytes:
    """Read the next 4-byte pkt-line length prefix.

    Returns ``b""`` at a clean end of stream. Raises ``UnexpectedResponse``
    if the stream is truncated mid-prefix.
    """
    try:
        return await reader.readexactly(4)
    except asyncio.IncompleteReadError as e:
        if not e.partial:
            return b""
        raise UnexpectedResponse(
            f"response truncated mid-pkt-line: {e.partial!r}"
        ) from None


async def _read_pkt_payload(
    reader: aiohttp.StreamReader, line_length: int, encoding: str = "utf-8"
) -> bytes:
    """Read the payload of a pkt-line whose length prefix was already read.

    Raises ``UnexpectedResponse`` if the stream is truncated mid-payload and
    ``RemoteError`` if the payload is an ``ERR`` line (e.g. "ERR upload-pack:
    not our ref <sha>").
    """
    payload_length = line_length - 4
    try:
        payload = await reader.readexactly(payload_length)
    except asyncio.IncompleteReadError as e:
        raise UnexpectedResponse(
            f"response truncated mid-pkt-line: expected {payload_length} "
            f"bytes, got {len(e.partial)}"
        ) from None

    if payload[:3] == b"ERR":
        raise RemoteError(payload.decode(encoding).strip())

    return payload


async def read_packfile(reader: aiohttp.StreamReader) -> bytes | None:
    """Drain a git-upload-pack negotiation section and return the packfile.

    ``reader`` is an asynchronous byte stream exposing ``readexactly`` and
    ``read`` (e.g. ``aiohttp.ClientResponse.content``).

    The server response begins with a negotiation section made up of pkt-lines
    (``NAK``, ``ACK <sha>``, ``ACK <sha> <status>``, ``shallow <sha>``, ...).
    The packfile that follows is *not* a pkt-line: it starts with the literal
    4-byte ``PACK`` signature. Servers may send several pkt-lines (e.g. multiple
    ``ACK`` lines during negotiation), so we consume pkt-lines until the
    ``PACK`` signature is reached.

    Args:
        reader: The asynchronous response byte stream.

    Returns:
        bytes: The packfile, including its ``PACK`` signature.
        None: If the stream ends before a packfile is sent.

    Raises:
        RemoteError: If the server sends an ``ERR`` pkt-line.
        UnexpectedResponse: If the stream is truncated mid-pkt-line or a
            pkt-line is malformed.
    """
    while True:
        prefix = await _read_pkt_prefix(reader)

        # The packfile is not length-prefixed; it begins with "PACK". This can
        # never collide with a pkt-line length prefix, which is 4 hex digits.
        if prefix == b"PACK":
            return prefix + await reader.read()

        if not prefix:
            # Clean end of stream without a packfile (nothing to fetch).
            return None

        line_length = parse_pkt_length(prefix)
        if line_length == 0:
            # Flush packet ("0000"); keep draining.
            continue

        # NAK / ACK / shallow / unshallow -> keep draining the negotiation.
        await _read_pkt_payload(reader, line_length)


async def read_pkt_lines(
    reader: aiohttp.StreamReader, encoding: str = "utf-8"
) -> AsyncIterator[str]:
    """Yield the decoded payload of each pkt-line from a git response stream.

    This frames the stream by length exactly as the git pkt-line format
    dictates rather than splitting on newlines, which is fragile: pkt-line
    payloads may embed ``\\0`` (capabilities) and flush packets (``0000``)
    carry no trailing newline. Each pkt-line is read as a 4-hex-digit length
    prefix followed by ``length - 4`` payload bytes.

    Flush packets (``0000``) are skipped. A single optional trailing ``LF`` is
    stripped from each payload before it is decoded and yielded.

    ``reader`` is an asynchronous byte stream exposing ``readexactly``
    (e.g. ``aiohttp.ClientResponse.content``).

    Args:
        reader: The asynchronous response byte stream.
        encoding: Character encoding used to decode payloads. Defaults to
            "utf-8".

    Yields:
        str: The decoded payload of each data pkt-line.

    Raises:
        RemoteError: If the server sends an ``ERR`` pkt-line.
        UnexpectedResponse: If the stream is truncated mid-pkt-line or a
            pkt-line is malformed.
    """
    while True:
        prefix = await _read_pkt_prefix(reader)
        if not prefix:
            # Clean end of stream.
            return

        line_length = parse_pkt_length(prefix)
        if line_length == 0:
            # Flush packet ("0000"); carries no payload.
            continue

        payload = await _read_pkt_payload(reader, line_length, encoding)
        yield payload.removesuffix(b"\n").decode(encoding)
