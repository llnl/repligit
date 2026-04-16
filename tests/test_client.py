from repligit.parse import decode_lines, encode_lines, generate_send_pack_header


def test_decode_lines():
    raw_lines = [
        "003fbef547a59eec448284136f03984dce0f2f8239a9 refs/pull/95/head",
        "003f358aa046cd57dbca306e80d4c3fbb86edc5b36af refs/pull/96/head",
        "0000",
    ]

    decoded_lines = [
        "bef547a59eec448284136f03984dce0f2f8239a9 refs/pull/95/head",
        "358aa046cd57dbca306e80d4c3fbb86edc5b36af refs/pull/96/head",
        "",
    ]

    lines = list(decode_lines(raw_lines))
    assert decoded_lines == lines


def test_encode_lines_from_bytes():
    input_lines = [
        b"bef547a59eec448284136f03984dce0f2f8239a9 refs/pull/95/head",
        b"358aa046cd57dbca306e80d4c3fbb86edc5b36af refs/pull/96/head",
    ]

    encoded_lines = (
        b"003fbef547a59eec448284136f03984dce0f2f8239a9 refs/pull/95/head\n"
        b"003f358aa046cd57dbca306e80d4c3fbb86edc5b36af refs/pull/96/head\n"
    )

    output_lines = encode_lines(input_lines)
    assert encoded_lines == output_lines


def test_encode_lines_from_str():
    input_lines = [
        "bef547a59eec448284136f03984dce0f2f8239a9 refs/pull/95/head",
        "358aa046cd57dbca306e80d4c3fbb86edc5b36af refs/pull/96/head",
    ]

    encoded_lines = (
        b"003fbef547a59eec448284136f03984dce0f2f8239a9 refs/pull/95/head\n"
        b"003f358aa046cd57dbca306e80d4c3fbb86edc5b36af refs/pull/96/head\n"
    )

    output_lines = encode_lines(input_lines)
    assert encoded_lines == output_lines


def test_generate_send_pack_header():
    expected_header = (
        b"0075aed5561af12f75f0b6b6ca34082610eaba109db7"
        b" b03ab96b18ed6633c877221318db41d36b15e3d7"
        b" refs/heads/main\x00 report-status\n0000"
    )

    from_sha = "aed5561af12f75f0b6b6ca34082610eaba109db7"
    to_sha = "b03ab96b18ed6633c877221318db41d36b15e3d7"
    ref = "refs/heads/main"

    output_header = generate_send_pack_header(ref, from_sha, to_sha)
    assert expected_header == output_header
