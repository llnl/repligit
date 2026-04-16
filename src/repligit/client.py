import urllib.request
from typing import Iterable

from repligit.parse import (
    decode_lines,
    generate_fetch_pack_request,
    generate_send_pack_header,
    iter_lines,
)


def http_request(url, headers=None, username=None, password=None, data=None):
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

    if username or password:
        password_manager.add_password(None, url, username or "", password or "")

    auth_handler = urllib.request.HTTPBasicAuthHandler(password_manager)
    opener = urllib.request.build_opener(auth_handler)

    request = urllib.request.Request(url, data=data)

    if headers:
        for header, value in headers.items():
            request.add_header(header, value)

    return opener.open(request)


def ls_remote(url: str, username: str | None = None, password: str | None = None):
    """Get commit hash of remote master branch, return SHA-1 hex string or
    None if no remote commits.
    """
    url = f"{url}/info/refs?service=git-upload-pack"

    resp = http_request(url, username=username, password=password)

    lines = decode_lines(iter_lines(resp))
    service_line = next(lines)
    assert service_line == "# service=git-upload-pack"

    return {ref: sha for sha, ref in (line.split() for line in lines if line)}


def fetch_pack(
    url: str, want_sha: str, have_shas: Iterable[str], username=None, password=None
):
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

    if line[:3] == b"NAK" or line[:3] == b"ACK":
        return resp
    else:
        return None


def send_pack(
    url: str,
    ref: str,
    from_sha: str,
    to_sha: str,
    packfile,
    username: str | None = None,
    password: str | None = None,
):
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

    lines = decode_lines(iter_lines(resp))
    assert next(lines) == "unpack ok"
    assert next(lines) == f"ok {ref}"
