import urllib.request
from collections.abc import Iterable
from http.client import HTTPResponse
from typing import BinaryIO

from repligit.exceptions import (
    RefUpdateRejected,
    RemoteError,
    UnexpectedResponse,
    UnpackFailed,
)
from repligit.parse import (
    generate_fetch_pack_request,
    generate_send_pack_header,
    read_pkt_lines,
)


def http_request(
    url: str,
    headers: dict[str, str] | None = None,
    username: str | None = None,
    password: str | None = None,
    data: bytes | None = None,
) -> HTTPResponse:
    """
    Constructs and executes an HTTP request using urllib. (GET by default,
    POST if "data" is not None).

    Args:
        url (str): The URL to send the request to
        headers (dict, optional): HTTP headers to include in the request
        username (str, optional): Username for basic authentication
        password (str, optional): Password for basic authentication
        data (bytes, optional): Data to send in the request body

    Returns:
        file-like object: The response file handler from the request
    """
    password_manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()

    if password:
        password_manager.add_password(None, url, username or "", password)

    auth_handler = urllib.request.HTTPBasicAuthHandler(password_manager)
    opener = urllib.request.build_opener(auth_handler)

    request = urllib.request.Request(url, data=data)

    if headers:
        for header, value in headers.items():
            request.add_header(header, value)

    return opener.open(request)


def ls_remote(
    url: str, username: str | None = None, password: str | None = None
) -> dict[str, str]:
    """Get commit hash of remote master branch, return SHA-1 hex string or
    None if no remote commits.
    """
    url = f"{url}/info/refs?service=git-upload-pack"

    resp = http_request(url, username=username, password=password)

    lines = read_pkt_lines(resp)
    service_line = next(lines)
    if service_line != "# service=git-upload-pack":
        raise UnexpectedResponse(f"invalid service line: {service_line!r}")

    refs: dict[str, str] = {}
    for line in lines:
        # The first ref line carries the server capabilities after a NUL byte.
        sha, ref = line.split("\x00", 1)[0].split()
        refs[ref] = sha
    return refs


def fetch_pack(
    url: str,
    want_sha: str,
    have_shas: Iterable[str],
    username: str | None = None,
    password: str | None = None,
) -> HTTPResponse | None:
    """Download a packfile from a remote server."""
    # ensure have_shas is a set, else packfile errors will occur
    have_shas = set(have_shas)

    url = f"{url}/git-upload-pack"
    request = generate_fetch_pack_request(want_sha, have_shas)

    resp = http_request(
        url,
        headers={
            "Content-type": "application/x-git-upload-pack-request",
        },
        username=username,
        password=password,
        data=request,
    )

    line_length = int(resp.read(4), 16)
    line = resp.read(line_length - 4)

    # e.g. "ERR upload-pack: not our ref <sha>"
    if line[:3] == b"ERR":
        raise RemoteError(line.decode("utf-8").strip())

    if line[:3] in (b"NAK", b"ACK"):
        return resp
    return None


def send_pack(
    url: str,
    ref: str,
    from_sha: str,
    to_sha: str,
    packfile: BinaryIO,
    username: str | None = None,
    password: str | None = None,
) -> None:
    """Send a packfile to a remote server."""
    url = f"{url}/git-receive-pack"

    header = generate_send_pack_header(ref, from_sha, to_sha)
    receive_pack_request = header + packfile.read()

    resp = http_request(
        url,
        headers={
            "Content-type": "application/x-git-receive-pack-request",
        },
        username=username,
        password=password,
        data=receive_pack_request,
    )

    lines = read_pkt_lines(resp)
    unpack_status = next(lines)
    if unpack_status != "unpack ok":
        raise UnpackFailed(unpack_status)

    # "ng <ref> <reason>" (ng = not good) means the remote rejected the update.
    # The reason can be non-fast-forward, hook declined, missing objects, etc.
    ref_status = next(lines)
    prefix = f"ng {ref} "
    if ref_status.startswith(prefix):
        reason = ref_status[len(prefix) :]
        raise RefUpdateRejected(reason)

    if ref_status != f"ok {ref}":
        raise UnexpectedResponse(f"unexpected ref status line: {ref_status!r}")
