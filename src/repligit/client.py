import io
import urllib.request
from http.client import HTTPResponse
from typing import BinaryIO, Iterable

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


class _PackfileStream(io.RawIOBase, BinaryIO):
    """Binary stream that replays already-consumed leading bytes (the
    b"PACK" signature read during negotiation) before the response body."""

    def __init__(self, head: bytes, resp: BinaryIO):
        super().__init__()
        self._head = head
        self._resp = resp

    def readable(self) -> bool:
        return True

    def read(self, size: int | None = -1) -> bytes:
        if size is None or size < 0:
            data, self._head = self._head, b""
            return data + self._resp.read()
        data, self._head = self._head[:size], self._head[size:]
        if len(data) < size:
            data += self._resp.read(size - len(data))
        return data


def fetch_pack(
    url: str,
    want_sha: str,
    have_shas: Iterable[str],
    username: str | None = None,
    password: str | None = None,
) -> BinaryIO | None:
    """Download a packfile from a remote server.

    Returns a binary stream positioned at the start of the packfile
    (reading it to EOF yields exactly the packfile), or None if the
    server response was unrecognized.
    """
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

    # Consume negotiation pkt-lines until the packfile begins. The server
    # may send several ACK lines before the pack: with multi-ack,
    # "ACK <sha> continue|common|ready" lines, and even without it some
    # servers (e.g. GitHub) emit one bare "ACK <sha>" per common commit
    # found. The only reliable delimiter is the b"PACK" signature itself,
    # which can never be a pkt-line length prefix ('P' and 'K' are not hex
    # digits).
    while True:
        prefix_bytes = resp.read(4)

        if prefix_bytes == b"PACK":
            # Negotiation done, packfile begins here. Wrap the remaining
            # stream so reading it to EOF yields exactly the packfile.
            return _PackfileStream(prefix_bytes, resp)

        line_length = int(prefix_bytes, 16)
        if line_length == 0:
            # flush-pkt ("0000"); carries no payload
            continue

        line = resp.read(line_length - 4)

        prefix = line[:3]

        # e.g. "ERR upload-pack: not our ref <sha>"
        if prefix == b"ERR":
            raise RemoteError(line.decode("utf-8").strip())

        if prefix not in (b"NAK", b"ACK"):
            return None
        # negotiation line ("NAK", "ACK <sha>", or
        # "ACK <sha> continue|common|ready"): keep consuming until the
        # b"PACK" signature is reached


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
        reason_str = ref_status[len(prefix) :]
        raise RefUpdateRejected(reason_str)

    if ref_status != f"ok {ref}":
        raise UnexpectedResponse(f"unexpected ref status line: {ref_status!r}")
