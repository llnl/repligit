HOOK_DECLINED_MSG = "pre-receive hook declined"


class RepligitError(Exception):
    """Base class for all repligit errors."""


class RefUpdateRejected(RepligitError):
    """Raised when the remote rejects a ref update during send-pack."""


class RemoteError(RepligitError):
    """Raised when the remote sends an ERR packet (e.g. want SHA not found)."""


class UnpackFailed(RepligitError):
    """Raised when the remote fails to unpack the packfile during send-pack."""


class UnexpectedResponse(RepligitError):
    """Raised when the remote sends a response repligit does not recognize."""
