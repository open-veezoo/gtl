"""BigQuery operations for gtl."""

from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def get_client(project: str) -> bigquery.Client:
    """Create a BigQuery client."""
    return bigquery.Client(project=project)


def ensure_dataset(client: bigquery.Client, dataset: str) -> None:
    """Create dataset if it doesn't exist."""
    dataset_ref = client.dataset(dataset)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = "US"
        client.create_dataset(ds)


def ensure_schema(client: bigquery.Client, dataset: str) -> None:
    """Create tables if they don't exist."""
    project = client.project

    # Ensure dataset exists first
    ensure_dataset(client, dataset)

    # Define table schemas
    tables = {
        "repositories": [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ],
        "branches": [
            bigquery.SchemaField("repo_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("head_sha", "STRING"),
            bigquery.SchemaField("is_default", "BOOL"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ],
        "commits": [
            bigquery.SchemaField("repo_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sha", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("branch", "STRING"),
            bigquery.SchemaField("author_name", "STRING"),
            bigquery.SchemaField("author_email", "STRING"),
            bigquery.SchemaField("committed_at", "TIMESTAMP"),
            bigquery.SchemaField("message", "STRING"),
            bigquery.SchemaField("parent_sha", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ],
        "file_changes": [
            bigquery.SchemaField("repo_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("commit_sha", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("file_path", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("change_type", "STRING"),
            bigquery.SchemaField("old_path", "STRING"),
            bigquery.SchemaField("diff", "STRING"),
            bigquery.SchemaField("additions", "INT64"),
            bigquery.SchemaField("deletions", "INT64"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ],
        "current_files": [
            bigquery.SchemaField("repo_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("file_path", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("branch", "STRING"),
            bigquery.SchemaField("content", "STRING"),
            bigquery.SchemaField("size_bytes", "INT64"),
            bigquery.SchemaField("last_commit_sha", "STRING"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ],
    }

    for table_name, schema in tables.items():
        table_id = f"{project}.{dataset}.{table_name}"
        table = bigquery.Table(table_id, schema=schema)
        try:
            client.get_table(table_id)
        except NotFound:
            client.create_table(table)


def ensure_repo(
    client: bigquery.Client,
    dataset: str,
    repo_id: str,
    name: str,
    url: str,
) -> None:
    """Insert or update repository record."""
    project = client.project
    table_id = f"{project}.{dataset}.repositories"
    now = datetime.now(timezone.utc).isoformat()

    # Check if repo exists
    query = f"""
        SELECT id FROM `{table_id}`
        WHERE id = @repo_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("repo_id", "STRING", repo_id),
        ]
    )
    results = list(client.query(query, job_config=job_config).result())

    if not results:
        # Insert new repo
        rows = [{
            "id": repo_id,
            "name": name,
            "url": url,
            "created_at": now,
        }]
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            raise RuntimeError(f"Failed to insert repository: {errors}")


def ensure_branch(
    client: bigquery.Client,
    dataset: str,
    repo_id: str,
    branch_name: str,
    is_default: bool = False,
) -> None:
    """Insert or update branch record."""
    project = client.project
    table_id = f"{project}.{dataset}.branches"
    now = datetime.now(timezone.utc).isoformat()

    # Check if branch exists
    query = f"""
        SELECT name FROM `{table_id}`
        WHERE repo_id = @repo_id AND name = @branch_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("repo_id", "STRING", repo_id),
            bigquery.ScalarQueryParameter("branch_name", "STRING", branch_name),
        ]
    )
    results = list(client.query(query, job_config=job_config).result())

    if not results:
        # Insert new branch
        rows = [{
            "repo_id": repo_id,
            "name": branch_name,
            "head_sha": None,
            "is_default": is_default,
            "created_at": now,
            "updated_at": now,
        }]
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            raise RuntimeError(f"Failed to insert branch: {errors}")


def update_branch_head(
    client: bigquery.Client,
    dataset: str,
    repo_id: str,
    branch_name: str,
    head_sha: str,
) -> None:
    """Update the HEAD SHA for a branch."""
    project = client.project
    table_id = f"{project}.{dataset}.branches"
    now = datetime.now(timezone.utc).isoformat()

    query = f"""
        UPDATE `{table_id}`
        SET head_sha = @head_sha, updated_at = @updated_at
        WHERE repo_id = @repo_id AND name = @branch_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("repo_id", "STRING", repo_id),
            bigquery.ScalarQueryParameter("branch_name", "STRING", branch_name),
            bigquery.ScalarQueryParameter("head_sha", "STRING", head_sha),
            bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", now),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    query_job.result()  # Wait for completion


def get_last_commit_sha(
    client: bigquery.Client,
    dataset: str,
    repo_id: str,
    branch: str | None = None,
) -> str | None:
    """Get most recent processed commit SHA for a repo/branch.

    Args:
        client: BigQuery client
        dataset: BigQuery dataset name
        repo_id: Repository identifier
        branch: Optional branch name. If provided, returns the last commit
                for that specific branch. If None, returns the last commit
                across all branches.

    Returns:
        The SHA of the most recent commit, or None if no commits found.
    """
    project = client.project
    table_id = f"{project}.{dataset}.commits"

    if branch:
        query = f"""
            SELECT sha
            FROM `{table_id}`
            WHERE repo_id = @repo_id AND branch = @branch
            ORDER BY committed_at DESC
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("repo_id", "STRING", repo_id),
                bigquery.ScalarQueryParameter("branch", "STRING", branch),
            ]
        )
    else:
        query = f"""
            SELECT sha
            FROM `{table_id}`
            WHERE repo_id = @repo_id
            ORDER BY committed_at DESC
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("repo_id", "STRING", repo_id),
            ]
        )

    results = list(client.query(query, job_config=job_config).result())
    if results:
        return results[0].sha
    return None


def get_branch_head_sha(
    client: bigquery.Client,
    dataset: str,
    repo_id: str,
    branch: str,
) -> str | None:
    """Get the HEAD SHA for a branch from the branches table.

    Args:
        client: BigQuery client
        dataset: BigQuery dataset name
        repo_id: Repository identifier
        branch: Branch name

    Returns:
        The HEAD SHA of the branch, or None if not found.
    """
    project = client.project
    table_id = f"{project}.{dataset}.branches"

    query = f"""
        SELECT head_sha
        FROM `{table_id}`
        WHERE repo_id = @repo_id AND name = @branch
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("repo_id", "STRING", repo_id),
            bigquery.ScalarQueryParameter("branch", "STRING", branch),
        ]
    )

    results = list(client.query(query, job_config=job_config).result())
    if results and results[0].head_sha:
        return results[0].head_sha
    return None


def insert_commits(
    client: bigquery.Client,
    dataset: str,
    commits: list[dict],
) -> None:
    """Batch insert commits."""
    if not commits:
        return

    project = client.project
    table_id = f"{project}.{dataset}.commits"
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for commit in commits:
        rows.append({
            "repo_id": commit["repo_id"],
            "sha": commit["sha"],
            "branch": commit.get("branch"),
            "author_name": commit["author_name"],
            "author_email": commit["author_email"],
            "committed_at": commit["committed_at"],
            "message": commit["message"],
            "parent_sha": commit.get("parent_sha"),
            "ingested_at": now,
        })

    # Use streaming inserts with batching
    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        errors = client.insert_rows_json(table_id, batch)
        if errors:
            raise RuntimeError(f"Failed to insert commits: {errors}")


def insert_file_changes(
    client: bigquery.Client,
    dataset: str,
    changes: list[dict],
) -> None:
    """Batch insert file changes."""
    if not changes:
        return

    project = client.project
    table_id = f"{project}.{dataset}.file_changes"
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for change in changes:
        rows.append({
            "repo_id": change["repo_id"],
            "commit_sha": change["commit_sha"],
            "file_path": change["file_path"],
            "change_type": change["change_type"],
            "old_path": change.get("old_path"),
            "diff": change.get("diff"),
            "additions": change.get("additions"),
            "deletions": change.get("deletions"),
            "ingested_at": now,
        })

    # Use streaming inserts with batching
    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        errors = client.insert_rows_json(table_id, batch)
        if errors:
            raise RuntimeError(f"Failed to insert file changes: {errors}")


def upsert_current_files(
    client: bigquery.Client,
    dataset: str,
    repo_id: str,
    files: list[dict],
    branch: str | None = None,
) -> None:
    """Replace current files for a repo/branch using MERGE.

    This deletes files that no longer exist and upserts existing/new files.

    Args:
        client: BigQuery client
        dataset: BigQuery dataset name
        repo_id: Repository identifier
        files: List of file dicts with file_path, content, size_bytes, last_commit_sha
        branch: Optional branch name. If provided, only updates files for that branch.
    """
    project = client.project
    table_id = f"{project}.{dataset}.current_files"
    now = datetime.now(timezone.utc).isoformat()

    # Create a temporary table with the new files
    branch_suffix = f"_{branch.replace('/', '_').replace('-', '_')}" if branch else ""
    temp_table_id = f"{project}.{dataset}._gtl_temp_files_{repo_id.replace('/', '_').replace('.', '_')}{branch_suffix}"

    # Define schema for temp table
    schema = [
        bigquery.SchemaField("repo_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("file_path", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("branch", "STRING"),
        bigquery.SchemaField("content", "STRING"),
        bigquery.SchemaField("size_bytes", "INT64"),
        bigquery.SchemaField("last_commit_sha", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    # Create temp table
    temp_table = bigquery.Table(temp_table_id, schema=schema)
    temp_table.expires = datetime.now(timezone.utc) + timedelta(hours=1)

    try:
        client.delete_table(temp_table_id, not_found_ok=True)
        client.create_table(temp_table)

        # Insert files into temp table
        if files:
            rows = []
            for f in files:
                rows.append({
                    "repo_id": repo_id,
                    "file_path": f["file_path"],
                    "branch": branch,
                    "content": f["content"],
                    "size_bytes": f["size_bytes"],
                    "last_commit_sha": f.get("last_commit_sha"),
                    "updated_at": now,
                })

            # Use load job for better handling of large data
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )

            # Insert in batches using streaming
            batch_size = 500
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                errors = client.insert_rows_json(temp_table_id, batch)
                if errors:
                    raise RuntimeError(f"Failed to insert temp files: {errors}")

        # Build the MERGE query with branch-aware matching
        if branch:
            merge_query = f"""
                MERGE `{table_id}` AS target
                USING `{temp_table_id}` AS source
                ON target.repo_id = source.repo_id 
                   AND target.file_path = source.file_path 
                   AND target.branch = source.branch

                WHEN MATCHED THEN
                    UPDATE SET
                        content = source.content,
                        size_bytes = source.size_bytes,
                        last_commit_sha = source.last_commit_sha,
                        updated_at = source.updated_at

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (repo_id, file_path, branch, content, size_bytes, last_commit_sha, updated_at)
                    VALUES (source.repo_id, source.file_path, source.branch, source.content, source.size_bytes, source.last_commit_sha, source.updated_at)

                WHEN NOT MATCHED BY SOURCE AND target.repo_id = @repo_id AND target.branch = @branch THEN
                    DELETE
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("repo_id", "STRING", repo_id),
                    bigquery.ScalarQueryParameter("branch", "STRING", branch),
                ]
            )
        else:
            # Legacy behavior: match on repo_id and file_path only
            merge_query = f"""
                MERGE `{table_id}` AS target
                USING `{temp_table_id}` AS source
                ON target.repo_id = source.repo_id AND target.file_path = source.file_path

                WHEN MATCHED THEN
                    UPDATE SET
                        content = source.content,
                        size_bytes = source.size_bytes,
                        last_commit_sha = source.last_commit_sha,
                        updated_at = source.updated_at

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (repo_id, file_path, branch, content, size_bytes, last_commit_sha, updated_at)
                    VALUES (source.repo_id, source.file_path, source.branch, source.content, source.size_bytes, source.last_commit_sha, source.updated_at)

                WHEN NOT MATCHED BY SOURCE AND target.repo_id = @repo_id THEN
                    DELETE
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("repo_id", "STRING", repo_id),
                ]
            )

        query_job = client.query(merge_query, job_config=job_config)
        query_job.result()  # Wait for completion

    finally:
        # Clean up temp table
        client.delete_table(temp_table_id, not_found_ok=True)
