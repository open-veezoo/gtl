"""Git operations for gtl."""

import subprocess
import re
from pathlib import Path


def run_git(*args: str, check: bool = True) -> str:
    """Run a git command and return stdout."""
    result = subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        check=check,
    )
    return result.stdout.strip()


def get_repo_id() -> str | None:
    """Auto-detect repo ID from git remote origin URL.

    Converts URLs like:
    - https://github.com/org/repo.git -> github.com/org/repo
    - git@github.com:org/repo.git -> github.com/org/repo

    Returns None if no remote origin is configured.
    """
    url = run_git("remote", "get-url", "origin", check=False)
    if not url:
        return None

    # Handle SSH URLs (git@github.com:org/repo.git)
    ssh_match = re.match(r"git@([^:]+):(.+?)(?:\.git)?$", url)
    if ssh_match:
        host, path = ssh_match.groups()
        return f"{host}/{path}"

    # Handle HTTPS URLs (https://github.com/org/repo.git)
    https_match = re.match(r"https?://([^/]+)/(.+?)(?:\.git)?$", url)
    if https_match:
        host, path = https_match.groups()
        return f"{host}/{path}"

    # Fallback: return URL as-is
    return url


def get_repo_name(repo_id: str | None = None) -> str | None:
    """Get the repository name from the remote URL or repo_id."""
    if repo_id is None:
        repo_id = get_repo_id()
    if repo_id is None:
        return None
    return repo_id.split("/")[-1]


def get_repo_url() -> str | None:
    """Get the repository URL."""
    return run_git("remote", "get-url", "origin", check=False) or None


def get_new_commits(last_sha: str | None) -> list[dict]:
    """Get commits since last_sha with metadata.

    Returns commits in oldest-first order for proper insertion.
    """
    # Build the revision range
    if last_sha:
        # Get commits after last_sha up to HEAD
        rev_range = f"{last_sha}..HEAD"
    else:
        # Get all commits
        rev_range = "HEAD"

    # Format: sha|parent_sha|author_name|author_email|timestamp|message
    # Use %x00 as delimiter to handle special chars in messages
    format_str = "%H%x00%P%x00%an%x00%ae%x00%aI%x00%B%x00%x01"

    try:
        output = run_git(
            "log",
            "--reverse",  # Oldest first
            "--first-parent",  # Only follow first parent (master branch)
            f"--format={format_str}",
            rev_range,
        )
    except subprocess.CalledProcessError:
        return []

    if not output:
        return []

    commits = []
    for entry in output.split("\x01"):
        entry = entry.strip()
        if not entry:
            continue

        parts = entry.split("\x00")
        if len(parts) < 6:
            continue

        sha, parents, author_name, author_email, timestamp, message = parts[:6]

        # Get first parent (or None for initial commit)
        parent_sha = parents.split()[0] if parents else None

        commits.append({
            "sha": sha,
            "parent_sha": parent_sha,
            "author_name": author_name,
            "author_email": author_email,
            "committed_at": timestamp,
            "message": message.strip(),
        })

    return commits


def get_file_changes(sha: str, parent_sha: str | None) -> list[dict]:
    """Get per-file diffs for a commit.

    Returns list of dicts with:
    - file_path, change_type, old_path, diff, additions, deletions
    """
    changes = []

    # Get the list of changed files with stats
    if parent_sha:
        diff_args = ["diff", "--numstat", "-M", parent_sha, sha]
    else:
        # Initial commit: compare against empty tree
        diff_args = ["diff", "--numstat", "-M", "--root", sha]

    numstat_output = run_git(*diff_args, check=False)

    # Get name-status for change types
    if parent_sha:
        status_args = ["diff", "--name-status", "-M", parent_sha, sha]
    else:
        status_args = ["diff", "--name-status", "-M", "--root", sha]

    status_output = run_git(*status_args, check=False)

    # Parse name-status to get change types and paths
    file_info = {}
    for line in status_output.splitlines():
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue

        status = parts[0]
        change_type = status[0]  # A, M, D, or R (with percentage for renames)

        if change_type == "R" and len(parts) >= 3:
            old_path = parts[1]
            file_path = parts[2]
            file_info[file_path] = {"change_type": "R", "old_path": old_path}
        else:
            file_path = parts[1]
            file_info[file_path] = {"change_type": change_type, "old_path": None}

    # Parse numstat for additions/deletions
    for line in numstat_output.splitlines():
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 3:
            continue

        additions_str, deletions_str, path = parts[0], parts[1], parts[2]

        # Handle renamed files (path shows as old_path => new_path)
        if " => " in path:
            # Could be in format: dir/{old => new}/file or old => new
            old_path, new_path = None, path
            if "{" in path:
                # Complex rename pattern
                match = re.match(r"(.*)?\{(.*) => (.*)\}(.*)?", path)
                if match:
                    prefix, old_part, new_part, suffix = match.groups()
                    prefix = prefix or ""
                    suffix = suffix or ""
                    new_path = f"{prefix}{new_part}{suffix}"
                    old_path = f"{prefix}{old_part}{suffix}"
            else:
                parts = path.split(" => ")
                if len(parts) == 2:
                    old_path, new_path = parts
            path = new_path

        # Binary files show as "-" for additions/deletions
        if additions_str == "-" or deletions_str == "-":
            continue  # Skip binary files

        additions = int(additions_str)
        deletions = int(deletions_str)

        info = file_info.get(path, {"change_type": "M", "old_path": None})

        # Get the actual diff for this file
        diff = get_file_diff(sha, parent_sha, path, info.get("old_path"))

        changes.append({
            "file_path": path,
            "change_type": info["change_type"],
            "old_path": info["old_path"],
            "diff": diff,
            "additions": additions,
            "deletions": deletions,
        })

    return changes


def get_file_diff(sha: str, parent_sha: str | None, file_path: str, old_path: str | None) -> str:
    """Get the diff for a specific file in a commit."""
    if parent_sha:
        if old_path:
            diff_args = ["diff", parent_sha, sha, "--", old_path, file_path]
        else:
            diff_args = ["diff", parent_sha, sha, "--", file_path]
    else:
        diff_args = ["diff", "--root", sha, "--", file_path]

    return run_git(*diff_args, check=False)


def get_current_files(max_size: int) -> list[dict]:
    """Get all current text files in repo with contents.

    Args:
        max_size: Maximum file size in bytes to include.

    Returns:
        List of dicts with file_path, content, size_bytes, last_commit_sha.
    """
    files = []

    # Get list of all tracked files
    output = run_git("ls-files")
    if not output:
        return files

    for file_path in output.splitlines():
        if not file_path:
            continue

        path = Path(file_path)
        if not path.exists():
            continue

        # Check file size
        size_bytes = path.stat().st_size
        if size_bytes > max_size:
            continue

        # Read file content
        try:
            content = path.read_bytes()
        except (OSError, IOError):
            continue

        # Skip binary files
        if is_binary(content):
            continue

        # Decode content
        try:
            content_str = content.decode("utf-8")
        except UnicodeDecodeError:
            continue

        # Get last commit that touched this file
        last_commit_sha = run_git("log", "-1", "--format=%H", "--", file_path, check=False)

        files.append({
            "file_path": file_path,
            "content": content_str,
            "size_bytes": size_bytes,
            "last_commit_sha": last_commit_sha or None,
        })

    return files


def is_binary(data: bytes) -> bool:
    """Check if content is binary (has null bytes in first 8KB)."""
    # Check first 8KB for null bytes
    chunk = data[:8192]
    return b"\x00" in chunk
