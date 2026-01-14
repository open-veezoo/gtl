"""Core sync logic for gtl."""

from . import git
from . import bigquery as bq


def sync(
    project: str,
    dataset: str,
    repo_id: str | None = None,
    max_file_size: int = 102400,
    verbose: bool = False,
) -> dict:
    """Main sync orchestration.

    1. Get last processed commit
    2. Get new commits since then
    3. For each commit, get file changes and insert
    4. Update current_files with latest state

    Args:
        project: GCP project ID
        dataset: BigQuery dataset name
        repo_id: Repository identifier (auto-detected if not provided)
        max_file_size: Maximum file size in bytes (default: 100KB)
        verbose: Print progress information

    Returns:
        dict with sync statistics
    """
    # Auto-detect repo info if not provided
    if not repo_id:
        repo_id = git.get_repo_id()
        if not repo_id:
            raise ValueError(
                "Could not auto-detect repository ID (no git remote origin). "
                "Please specify --repo-id explicitly."
            )

    repo_name = git.get_repo_name(repo_id)
    repo_url = git.get_repo_url()

    if verbose:
        print(f"Syncing repository: {repo_id}")

    # Initialize BigQuery client
    client = bq.get_client(project)

    # Ensure schema exists
    bq.ensure_schema(client, dataset)

    # Ensure repository record exists
    bq.ensure_repo(client, dataset, repo_id, repo_name, repo_url)

    # Get last processed commit
    last_sha = bq.get_last_commit_sha(client, dataset, repo_id)

    if verbose:
        if last_sha:
            print(f"Last processed commit: {last_sha[:8]}")
        else:
            print("No previous commits found, processing full history")

    # Get new commits
    commits = git.get_new_commits(last_sha)

    if verbose:
        print(f"Found {len(commits)} new commits to process")

    # Process commits
    commits_processed = 0
    file_changes_processed = 0

    for commit in commits:
        # Add repo_id to commit
        commit["repo_id"] = repo_id

        # Insert commit
        bq.insert_commits(client, dataset, [commit])
        commits_processed += 1

        if verbose:
            print(f"  Processing commit {commit['sha'][:8]}: {commit['message'][:50]}...")

        # Get file changes for this commit
        changes = git.get_file_changes(commit["sha"], commit.get("parent_sha"))

        # Add repo_id and commit_sha to each change
        for change in changes:
            change["repo_id"] = repo_id
            change["commit_sha"] = commit["sha"]

        # Insert file changes
        if changes:
            bq.insert_file_changes(client, dataset, changes)
            file_changes_processed += len(changes)

    # Update current files
    if verbose:
        print("Updating current files...")

    current_files = git.get_current_files(max_file_size)
    bq.upsert_current_files(client, dataset, repo_id, current_files)

    if verbose:
        print(f"Updated {len(current_files)} current files")
        print("Sync complete!")

    return {
        "repo_id": repo_id,
        "commits_processed": commits_processed,
        "file_changes_processed": file_changes_processed,
        "current_files_updated": len(current_files),
    }


def init(project: str, dataset: str, verbose: bool = False) -> None:
    """Initialize BigQuery schema.

    Args:
        project: GCP project ID
        dataset: BigQuery dataset name
        verbose: Print progress information
    """
    if verbose:
        print(f"Initializing schema in {project}.{dataset}")

    client = bq.get_client(project)
    bq.ensure_schema(client, dataset)

    if verbose:
        print("Schema initialized successfully!")
        print("Tables created:")
        print("  - repositories")
        print("  - commits")
        print("  - file_changes")
        print("  - current_files")
