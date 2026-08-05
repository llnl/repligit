import io
from collections.abc import Generator, Iterable
from typing import IO, BinaryIO

from repligit.exceptions import RemoteError, UnexpectedResponse


def iter_lines(
    data: IO, encoding: str = "utf-8", chunk_size: int = 16 * 1024
) -> Generator[str, None, None]:
    """
    Iterate over the lines of a file-like object, yielding one line at a time.

    Args:
        data: A file-like object with a read() method that returns bytes
        encoding (str, optional): Character encoding to use for decoding bytes to strings.
            Defaults to "utf-8".
        chunk_size (int, optional): Number of bytes to read in each chunk.
            Defaults to 16 KiB.

    Yields:
        str: Each line from the input data, with line endings removed.
    """
    incomplete_line = bytearray()

    for chunk in iter(lambda: data.read(chunk_size), b""):
        lines = (incomplete_line + chunk).split(b"\n")
        incomplete_line = lines.pop()

        for line in lines:
            yield line.rstrip(b"\r").decode(encoding)

    if incomplete_line:
        yield incomplete_line.rstrip(b"\r").decode(encoding)


def decode_lines(lines: Iterable[str]) -> Generator[str, None, None]:
    """
    Decode lines from the git transfer protocol into usable lines.

    This asynchronous function processes a stream of lines from a server response,
    where each line is prefixed with a 4-character hexadecimal length indicator.
    It extracts and yields the actual data portion of each line.

    Args:
        lines: A generator yielding strings from a git server response.

    Yields:
        str: Decoded content from each line with the length prefix removed.
    """
    for line in lines:
        line_length = int(line[:4], 16)
        yield line[4:line_length]


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


class _PackfileStream(io.RawIOBase):
    """Read-only binary stream that replays the already-consumed ``PACK``
    signature before delegating reads to the underlying response stream.

    This lets ``read_packfile`` hand the packfile back to the caller as a
    stream without buffering it in memory. Closing this stream closes the
    underlying stream.
    """

    def __init__(self, stream: IO[bytes]):
        self._stream = stream
        self._prefix = b"PACK"

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        # Serve the replayed signature first, then pass reads through.
        if self._prefix:
            n = min(len(buffer), len(self._prefix))
            buffer[:n] = self._prefix[:n]
            self._prefix = self._prefix[n:]
            return n

        chunk = self._stream.read(len(buffer))
        buffer[: len(chunk)] = chunk
        return len(chunk)

    def close(self) -> None:
        try:
            self._stream.close()
        finally:
            super().close()


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


def read_packfile(stream: IO[bytes]) -> BinaryIO | None:
    """Drain a git-upload-pack negotiation section and return the packfile.

    ``stream`` is a synchronous, file-like byte stream (e.g.
    ``http.client.HTTPResponse``).

    The server response begins with a negotiation section made up of pkt-lines
    (``NAK``, ``ACK <sha>``, ``ACK <sha> <status>``, ``shallow <sha>``, ...).
    The packfile that follows is *not* a pkt-line: it starts with the literal
    4-byte ``PACK`` signature. Servers may send several pkt-lines (e.g. multiple
    ``ACK`` lines during negotiation), so we consume pkt-lines until the
    ``PACK`` signature is reached.

    Args:
        stream: The response byte stream.

    Returns:
        BinaryIO: A read-only stream over the packfile, starting at its
            ``PACK`` signature. The packfile is *not* buffered in memory;
            reads are forwarded to ``stream``, and closing the returned
            stream closes ``stream``.
        None: If the stream ends before a packfile is sent.

    Raises:
        RemoteError: If the server sends an ``ERR`` pkt-line.
        UnexpectedResponse: If the stream is truncated mid-pkt-line or a
            pkt-line is malformed.
    """
    while True:
        prefix = _read_exact(stream, 4)

        # The packfile is not length-prefixed; it begins with "PACK". This can
        # never collide with a pkt-line length prefix, which is 4 hex digits.
        if prefix == b"PACK":
            return io.BufferedReader(_PackfileStream(stream))

        if not prefix:
            # Clean end of stream without a packfile (nothing to fetch).
            return None

        if len(prefix) < 4:
            raise UnexpectedResponse(f"response truncated mid-pkt-line: {prefix!r}")

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

        line = _read_exact(stream, line_length - 4)
        if len(line) < line_length - 4:
            raise UnexpectedResponse(
                f"response truncated mid-pkt-line: expected {line_length - 4} "
                f"bytes, got {len(line)}"
            )

        # e.g. "ERR upload-pack: not our ref <sha>"
        if line[:3] == b"ERR":
            raise RemoteError(line.decode("utf-8").strip())

        # NAK / ACK / shallow / unshallow -> continue draining negotiation.


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


def generate_fetch_pack_request(want: str, haves: set[str]) -> bytes:
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
