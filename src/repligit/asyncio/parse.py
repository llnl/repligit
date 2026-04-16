from typing import AsyncIterable, AsyncIterator

import aiohttp


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
