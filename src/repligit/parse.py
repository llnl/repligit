from typing import IO, Generator, Iterable, Set

from repligit.exceptions import RemoteError, UnexpectedResponse


def parse_pkt_length(prefix: bytes) -> int:
    """Parse and validate a 4-byte pkt-line length prefix.

    Args:
        prefix: The 4-byte hexadecimal length prefix of a pkt-line.

    Returns:
        int: The total pkt-line length. ``0`` denotes a flush packet.

    Raises:
        UnexpectedResponse: If the prefix is not hexadecimal or denotes an
            impossible length (1-3 can never fit the 4-byte prefix itself).
    """
    try:
        line_length = int(prefix, 16)
    except ValueError:
        raise UnexpectedResponse(f"invalid pkt-line length: {prefix!r}") from None

    if 0 < line_length < 4:
        raise UnexpectedResponse(f"invalid pkt-line length: {prefix!r}")

    return line_length


def _read_exact(stream: IO[bytes], n: int) -> bytes:
    """Read exactly ``n`` bytes from ``stream``, or fewer at end of stream.

    Loops over ``stream.read``, so it tolerates file-like objects whose
    ``read`` may return fewer bytes than requested before end of stream.
    """
    data = bytearray()
    while len(data) < n:
        chunk = stream.read(n - len(data))
        if not chunk:
            break
        data += chunk
    return bytes(data)


def _read_pkt_prefix(stream: IO[bytes]) -> bytes:
    """Read the next 4-byte pkt-line length prefix.

    Returns ``b""`` at a clean end of stream. Raises ``UnexpectedResponse``
    if the stream is truncated mid-prefix.
    """
    prefix = _read_exact(stream, 4)
    if prefix and len(prefix) < 4:
        raise UnexpectedResponse(f"response truncated mid-pkt-line: {prefix!r}")
    return prefix


def _read_pkt_payload(
    stream: IO[bytes], line_length: int, encoding: str = "utf-8"
) -> bytes:
    """Read the payload of a pkt-line whose length prefix was already read.

    Raises ``UnexpectedResponse`` if the stream is truncated mid-payload and
    ``RemoteError`` if the payload is an ``ERR`` line (e.g. "ERR upload-pack:
    not our ref <sha>").
    """
    payload_length = line_length - 4
    payload = _read_exact(stream, payload_length)
    if len(payload) < payload_length:
        raise UnexpectedResponse(
            f"response truncated mid-pkt-line: expected {payload_length} "
            f"bytes, got {len(payload)}"
        )

    if payload[:3] == b"ERR":
        raise RemoteError(payload.decode(encoding).strip())

    return payload


def read_pkt_lines(
    stream: IO[bytes], encoding: str = "utf-8"
) -> Generator[str, None, None]:
    """Yield the decoded payload of each pkt-line from a git response stream.

    This frames the stream by length exactly as the git pkt-line format
    dictates rather than splitting on newlines, which is fragile: pkt-line
    payloads may embed ``\\0`` (capabilities) and flush packets (``0000``)
    carry no trailing newline. Each pkt-line is read as a 4-hex-digit length
    prefix followed by ``length - 4`` payload bytes.

    Flush packets (``0000``) are skipped. A single optional trailing ``LF`` is
    stripped from each payload before it is decoded and yielded.

    ``stream`` is a synchronous, file-like byte stream (e.g.
    ``http.client.HTTPResponse``). Short reads before end of stream are
    tolerated.

    Args:
        stream: The response byte stream.
        encoding (str, optional): Character encoding used to decode payloads.
            Defaults to "utf-8".

    Yields:
        str: The decoded payload of each data pkt-line.

    Raises:
        RemoteError: If the server sends an ``ERR`` pkt-line.
        UnexpectedResponse: If the stream is truncated mid-pkt-line or a
            pkt-line is malformed.
    """
    # An empty prefix means a clean end of stream.
    while prefix := _read_pkt_prefix(stream):
        line_length = parse_pkt_length(prefix)
        if line_length == 0:
            # Flush packet ("0000"); carries no payload.
            continue

        payload = _read_pkt_payload(stream, line_length, encoding)
        yield payload.removesuffix(b"\n").decode(encoding)


def encode_lines(lines: Iterable[bytes | str]) -> bytes:
    """
    Encode a list of lines into a byte string format for git transmission.

    Args:
        lines: A list of strings or byte objects to be encoded.

    Returns:
        bytes: A single byte string containing all encoded lines.
    """
    result: list[bytes] = []
    for line in lines:
        data = line.encode("utf-8") if isinstance(line, str) else line

        result.append(f"{len(data) + 5:04x}".encode())
        result.append(data)
        result.append(b"\n")

    return b"".join(result)


def generate_send_pack_header(ref: str, from_sha: str, to_sha: str) -> bytes:
    """
    Generate a Git send-pack header for updating references.

    Args:
        ref (str): The full reference name (e.g., 'refs/heads/main')
        from_sha (str): The source SHA-1 object ID (40 hex characters)
        to_sha (str): The target SHA-1 object ID (40 hex characters)

    Returns:
        bytes: Encoded pack header with the format "<from_sha> <to_sha> <ref>\0 report-status"
               followed by the "0000" flush packet
    """
    return encode_lines([f"{from_sha} {to_sha} {ref}\x00 report-status"]) + b"0000"


def generate_fetch_pack_request(want: str, haves: Set[str]) -> bytes:
    """Generate a git-upload packfile request.

    Args:
        want (str): The SHA-1 hash of the commit that is wanted.
        haves (Set[str]): A set of SHA-1 hashes of commits that the client already has.

    Returns:
        bytes: The formatted git-upload-pack request as bytes.
    """

    want_cmds = encode_lines([f"want {want}".encode()])
    have_cmds = encode_lines([f"have {sha}".encode() for sha in haves])
    return want_cmds + b"0000" + have_cmds + encode_lines([b"done"])
