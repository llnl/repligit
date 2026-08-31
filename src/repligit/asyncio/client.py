from typing import Iterable

import aiohttp

from repligit.asyncio.parse import read_pkt_lines
from repligit.exceptions import (
    RefUpdateRejected,
    RemoteError,
    UnexpectedResponse,
    UnpackFailed,
)
from repligit.parse import generate_fetch_pack_request, generate_send_pack_header


async def ls_remote(
    url: str, username: str | None = None, password: str | None = None
) -> dict[str, str]:
    """Get commit hash of remote master branch, return SHA-1 hex string or
    None if no remote commits.
    """

    url = f"{url}/info/refs?service=git-upload-pack"
    auth = aiohttp.BasicAuth(username or "", password) if password else None
    async with aiohttp.ClientSession(auth=auth) as session:
        async with session.get(url, raise_for_status=True) as resp:
            lines = read_pkt_lines(resp.content)
            service_line = await anext(lines)
            if service_line != "# service=git-upload-pack":
                raise UnexpectedResponse(f"invalid service line: {service_line!r}")

            refs: dict[str, str] = {}
            async for line in lines:
                # The first ref line carries the server capabilities after a NUL byte.
                sha, ref = line.split("\x00", 1)[0].split()
                refs[ref] = sha
            return refs


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
    auth = aiohttp.BasicAuth(username or "", password) if password else None

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
            # Consume negotiation pkt-lines until the packfile begins. The
            # server may send several ACK lines before the pack: with
            # multi-ack, "ACK <sha> continue|common|ready" lines, and even
            # without it some servers (e.g. GitHub) emit one bare
            # "ACK <sha>" per common commit found. The only reliable
            # delimiter is the b"PACK" signature itself, which can never be
            # a pkt-line length prefix ('P' and 'K' are not hex digits).
            while True:
                prefix_bytes = await resp.content.readexactly(4)

                if prefix_bytes == b"PACK":
                    # Negotiation done, packfile begins here. Unlike the
                    # sync API, the body must be read within this context
                    # to be usable by the caller.
                    return prefix_bytes + await resp.content.read()

                line_length = int(prefix_bytes, 16)
                if line_length == 0:
                    # flush-pkt ("0000"); carries no payload
                    continue

                line = await resp.content.readexactly(line_length - 4)

                prefix = line[:3]

                # e.g. "ERR upload-pack: not our ref <sha>"
                if prefix == b"ERR":
                    raise RemoteError(line.decode("utf-8").strip())

                if prefix not in (b"NAK", b"ACK"):
                    return None
                # negotiation line ("NAK", "ACK <sha>", or
                # "ACK <sha> continue|common|ready"): keep consuming until
                # the b"PACK" signature is reached


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
    auth = aiohttp.BasicAuth(username or "", password) if password else None

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
            lines = read_pkt_lines(resp.content)
            unpack_status = await anext(lines)
            if unpack_status != "unpack ok":
                raise UnpackFailed(unpack_status)

            # "ng <ref> <reason>" (ng = not good) means the remote rejected the
            # update. The reason may be non-fast-forward, hook declined, etc.
            ref_status = await anext(lines)
            prefix = f"ng {ref} "
            if ref_status.startswith(prefix):
                reason_str = ref_status[len(prefix) :]
                raise RefUpdateRejected(reason_str)

            if ref_status != f"ok {ref}":
                raise UnexpectedResponse(f"unexpected ref status line: {ref_status!r}")
