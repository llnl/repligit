import asyncio
from collections.abc import AsyncIterable, AsyncIterator

import aiohttp

from repligit.exceptions import RemoteError, UnexpectedResponse


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
        try:
            prefix = await reader.readexactly(4)
        except asyncio.IncompleteReadError as e:
            if not e.partial:
                # Clean end of stream without a packfile (nothing to fetch).
                return None
            raise UnexpectedResponse(
                f"response truncated mid-pkt-line: {e.partial!r}"
            ) from None

        # The packfile is not length-prefixed; it begins with "PACK". This can
        # never collide with a pkt-line length prefix, which is 4 hex digits.
        if prefix == b"PACK":
            return prefix + await reader.read()

        try:
            line_length = int(prefix, 16)
        except ValueError:
            raise UnexpectedResponse(f"invalid pkt-line length: {prefix!r}") from None

        if line_length == 0:
            # Flush packet ("0000"); keep draining.
            continue

        if line_length < 4:
            # 1-3 never denote a valid payload length in this response.
            raise UnexpectedResponse(f"invalid pkt-line length: {prefix!r}")

        try:
            line = await reader.readexactly(line_length - 4)
        except asyncio.IncompleteReadError as e:
            raise UnexpectedResponse(
                f"response truncated mid-pkt-line: expected {line_length - 4} "
                f"bytes, got {len(e.partial)}"
            ) from None

        # e.g. "ERR upload-pack: not our ref <sha>"
        if line[:3] == b"ERR":
            raise RemoteError(line.decode("utf-8").strip())

        # NAK / ACK / shallow / unshallow -> continue draining negotiation.


async def decode_lines(line_stream: AsyncIterable) -> AsyncIterator:
    """Decode git server response iterator into individual data lines.

    This asynchronous function processes a stream of lines from a server response,
    where each line is prefixed with a 4-character hexadecimal length indicator.
    It extracts and yields the actual data portion of each line.

    Args:
        line_stream: An asynchronous iterable providing the raw server response lines.

    Yields:
        The decoded data portion of each line, with the length prefix removed.
    """
    async for line in line_stream:
        line_length = int(line[:4], 16)
        yield line[4:line_length]


async def iter_lines(
    resp: aiohttp.ClientResponse, encoding: str = "utf-8", chunk_size: int = 16 * 1024
) -> AsyncIterator[str]:
    """
    Asynchronously iterate over the lines of an HTTP response.

    Args:
        resp: The aiohttp ClientResponse object to read from.
        encoding: The character encoding to use for decoding bytes to strings.
            Defaults to "utf-8".
        chunk_size: The number of bytes to read in each chunk.
            Defaults to 16 KiB (16 * 1024 bytes).

    Yields:
        str: Each line from the response, with trailing carriage returns removed
            and decoded using the specified encoding.
    """
    incomplete_line = bytearray()

    async for chunk in resp.content.iter_chunked(chunk_size):
        lines = (incomplete_line + chunk).split(b"\n")
        incomplete_line = lines.pop()

        for line in lines:
            yield line.rstrip(b"\r").decode(encoding)

    if incomplete_line:
        yield incomplete_line.rstrip(b"\r").decode(encoding)
