HOOK_DECLINED_MSG = "pre-receive hook declined"


class RefUpdateRejected(Exception):
    """Raised when the remote rejects a ref update during send-pack."""
