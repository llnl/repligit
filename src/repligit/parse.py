from typing import IO, Generator, Iterable, Set, Union


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
    result = []
    for line in lines:
        if type(line) is str:
            line = line.encode("utf-8")

        result.append(f"{len(line) + 5:04x}".encode())
        result.append(line)
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
