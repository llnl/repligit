import asyncio

from repligit.asyncio import fetch_pack, ls_remote, send_pack


async def main():
    src_remote_url = "https://github.com/spack/spack.git"
    dest_remote_url = "https://gitlab.com/test-org/test-repo.git"

    branch_name = "main"

    target_ref = f"refs/heads/{branch_name}"

    # Authentication credentials
    # Note: Only provide credentials when authentication is required
    # src_username = "<username>"  # Uncomment if source repo requires auth
    # src_password = "<token>"     # Uncomment if source repo requires auth
    dest_username = "<username>"  # For destination repo write access
    dest_password = "<token>"  # For destination repo write access

    # List references from source repository (without authentication)
    gh_refs = await ls_remote(src_remote_url)

    # List references from destination repository (with authentication)
    gl_refs = await ls_remote(
        dest_remote_url, username=dest_username, password=dest_password
    )

    want_sha = gh_refs[target_ref]
    have_shas = set(gl_refs.values())

    from_sha = gl_refs.get(target_ref) or ("0" * 40)

    if want_sha in have_shas:
        print("Everything is up to date")
        return

    # Fetch the packfile from source repository
    packfile = await fetch_pack(
        src_remote_url,
        want_sha,
        have_shas,
        # username=src_username,  # Uncomment if source repo requires auth
        # password=src_password,  # Uncomment if source repo requires auth
    )

    if packfile is None:
        print("Failed to fetch packfile")
        return

    # Upload packfile to destination repository
    await send_pack(
        dest_remote_url,
        target_ref,
        from_sha,
        want_sha,
        packfile,
        username=dest_username,
        password=dest_password,
    )


if __name__ == "__main__":
    asyncio.run(main())
