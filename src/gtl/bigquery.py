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
        "commits": [
            bigquery.SchemaField("repo_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sha", "STRING", mode="REQUIRED"),
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


def get_last_commit_sha(
    client: bigquery.Client,
    dataset: str,
    repo_id: str,
) -> str | None:
    """Get most recent processed commit SHA for a repo."""
    project = client.project
    table_id = f"{project}.{dataset}.commits"

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
) -> None:
    """Replace current files for a repo using MERGE.

    This deletes files that no longer exist and upserts existing/new files.
    """
    project = client.project
    table_id = f"{project}.{dataset}.current_files"
    now = datetime.now(timezone.utc).isoformat()

    # Create a temporary table with the new files
    temp_table_id = f"{project}.{dataset}._gtl_temp_files_{repo_id.replace('/', '_').replace('.', '_')}"

    # Define schema for temp table
    schema = [
        bigquery.SchemaField("repo_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("file_path", "STRING", mode="REQUIRED"),
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

        # Use MERGE to update the target table
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
                INSERT (repo_id, file_path, content, size_bytes, last_commit_sha, updated_at)
                VALUES (source.repo_id, source.file_path, source.content, source.size_bytes, source.last_commit_sha, source.updated_at)

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
