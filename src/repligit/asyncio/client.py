from typing import Iterable

import aiohttp

from repligit.asyncio.parse import decode_lines, iter_lines
from repligit.parse import generate_fetch_pack_request, generate_send_pack_header


async def ls_remote(
    url: str, username: str | None = None, password: str | None = None
) -> dict[str, str]:
    """Get commit hash of remote master branch, return SHA-1 hex string or
    None if no remote commits.
    """

    url = f"{url}/info/refs?service=git-upload-pack"
    auth = (
        aiohttp.BasicAuth(username or "", password or "")
        if username or password
        else None
    )
    async with aiohttp.ClientSession(auth=auth) as session:
        async with session.get(url, raise_for_status=True) as resp:
            lines = decode_lines(iter_lines(resp, encoding="utf-8"))
            service_line = await anext(lines)
            assert service_line == "# service=git-upload-pack"

            # `async for` inside `dict()` not supported so no dict comprehension
            result = {}
            async for line in lines:
                if not line:
                    continue
                sha, ref = line.split()
                result[ref] = sha
            return result


async def fetch_pack(
    url: str,
    want_sha: str,
    have_shas: Iterable[str],
    username: str | None = None,
    password: str | None = None,
) -> bytes | None:
    """Download a packfile from a remote server."""
    # ensure have_shas is a set, else packfile errors will occur
    have_shas = set(have_shas)

    url = f"{url}/git-upload-pack"
    auth = (
        aiohttp.BasicAuth(username or "", password or "")
        if username or password
        else None
    )

    request = generate_fetch_pack_request(want_sha, have_shas)

    async with aiohttp.ClientSession(auth=auth) as session:
        async with session.post(
            url,
            headers={
                "Content-type": "application/x-git-upload-pack-request",
            },
            data=request,
            raise_for_status=True,
            timeout=None,
        ) as resp:
            length_bytes = await resp.content.readexactly(4)
            line_length = int(length_bytes, 16)

            line = await resp.content.readexactly(line_length - 4)
            if line[:3] == b"NAK" or line[:3] == b"ACK":
                # this is a difference in API between sync and async
                # has to be read within this context to be used in the caller
                return await resp.content.read()
            else:
                return None


async def send_pack(
    url: str,
    ref: str,
    from_sha: str,
    to_sha: str,
    packfile: bytes,
    username: str | None = None,
    password: str | None = None,
) -> None:
    """Send a packfile to a remote server."""
    url = f"{url}/git-receive-pack"
    auth = (
        aiohttp.BasicAuth(username or "", password or "")
        if username or password
        else None
    )

    header = generate_send_pack_header(ref, from_sha, to_sha)
    # unlike in the sync version the packfile is already read into memory
    receive_pack_request = header + packfile

    async with aiohttp.ClientSession(auth=auth) as session:
        async with session.post(
            url,
            headers={
                "Content-type": "application/x-git-receive-pack-request",
            },
            data=receive_pack_request,
            raise_for_status=True,
        ) as resp:
            lines = decode_lines(iter_lines(resp, encoding="utf-8"))
            assert await anext(lines) == "unpack ok"
            assert await anext(lines) == f"ok {ref}"
