# repligit
`repligit` is a Python library that implements the Git transfer protocol. It enables users to query remote repositories, mirror repositories between two locations without storing state locally, and incrementally archive repositories to disk. `repligit` is used by [Hubcast](https://github.com/llnl/hubcast) to mirror repositories from GitHub to GitLab for secure CI/CD on local hardware.

## Installation
You can install repligit from PyPI using pip:

```bash
pip install repligit
```

## Features
- Query remote Git repositories.
- Mirror repositories between different Git hosting services.
- Incrementally archive repositories to disk.
- Implements Git transfer protocol in pure Python.

## Example Usage
```python
from repligit import fetch_pack, ls_remote, send_pack


def main():
    src_remote_url = "https://github.com/spack/spack.git"
    dest_remote_url = "https://gitlab.com/test-org/test-repo.git"

    branch_name = "main"

    target_ref = f"refs/heads/{branch_name}"

    # Authentication credentials
    # Note: Only provide credentials when authentication is required
    # src_username = "<username>"  # Uncomment if source repo requires auth
    # src_password = "<token>"     # Uncomment if source repo requires auth
    dest_username = "<username>"   # For destination repo write access
    dest_password = "<token>"      # For destination repo write access

    # List references from source repository (without authentication)
    gh_refs = ls_remote(src_remote_url)

    # List references from destination repository (with authentication)
    gl_refs = ls_remote(
        dest_remote_url,
        username=dest_username,
        password=dest_password,
    )

    want_sha = gh_refs[target_ref]
    have_shas = set(gl_refs.values())

    from_sha = gl_refs.get(target_ref) or ("0" * 40)

    if want_sha in have_shas:
        print("Everything is up to date")
        return

    # Fetch the packfile from source repository
    packfile = fetch_pack(
        src_remote_url,
        want_sha,
        have_shas,
        # username=src_username,  # Uncomment if source repo requires auth
        # password=src_password,  # Uncomment if source repo requires auth
    )

    # Upload packfile to destination repository
    send_pack(
        dest_remote_url,
        target_ref,
        from_sha,
        want_sha,
        packfile,
        username=dest_username,
        password=dest_password,
    )


if __name__ == "__main__":
    main()
```

## License

Licensed under the Apache License, Version 2.0 w/LLVM Exception
(the "License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

LLNL-CODE-2003682
