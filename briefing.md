# Technical Briefing: gtl (Git-Transform-Load)

## Project Overview

Build a pip-installable Python package called `gtl` that syncs Git repository history to BigQuery. The package will be maintained in its own repository and can be installed into any project that needs to sync its git history. The schema supports multiple repositories in a single BigQuery dataset.

## Requirements

- Standalone pip-installable package (hosted in its own repo)
- Support multiple git repositories in a single BigQuery dataset
- Track all commits on master branch
- Store per-file diffs for each commit
- Store current file contents only (not historical versions)
- Skip binary files entirely
- Import full repository history on first run
- Incremental updates on subsequent runs

## Architecture

```
gtl (pip package)
    ↓ install
any-repo/.github/workflows/sync.yml
    ↓ push to master
GitHub Actions
    ↓ gtl sync
BigQuery (shared dataset, multiple repos)
```

## BigQuery Schema

### Table: repositories

Tracks each repository being synced.

| Column | Type | Description |
|--------|------|-------------|
| id | STRING | Unique identifier (e.g., "github.com/org/repo") |
| name | STRING | Repository name |
| url | STRING | Repository URL |
| created_at | TIMESTAMP | When the repo was first synced |

### Table: commits

Stores commit metadata.

| Column | Type | Description |
|--------|------|-------------|
| repo_id | STRING | References repositories.id |
| sha | STRING | Commit SHA |
| author_name | STRING | Name of the commit author |
| author_email | STRING | Email of the commit author |
| committed_at | TIMESTAMP | When the commit was made |
| message | STRING | Commit message |
| parent_sha | STRING | SHA of the parent commit (null for initial) |
| ingested_at | TIMESTAMP | When the record was inserted |

### Table: file_changes

Stores per-file diffs for each commit.

| Column | Type | Description |
|--------|------|-------------|
| repo_id | STRING | References repositories.id |
| commit_sha | STRING | References commits.sha |
| file_path | STRING | Full path of the file |
| change_type | STRING | Type of change: "A" (added), "M" (modified), "D" (deleted), "R" (renamed) |
| old_path | STRING | Previous path if renamed, null otherwise |
| diff | STRING | Diff for this file in this commit |
| additions | INT64 | Lines added |
| deletions | INT64 | Lines deleted |
| ingested_at | TIMESTAMP | When the record was inserted |

### Table: current_files

Stores current state of files (overwritten on each sync).

| Column | Type | Description |
|--------|------|-------------|
| repo_id | STRING | References repositories.id |
| file_path | STRING | Full path of the file |
| content | STRING | Current file contents |
| size_bytes | INT64 | Size of the file in bytes |
| last_commit_sha | STRING | SHA of last commit that touched this file |
| updated_at | TIMESTAMP | When this record was last updated |

## Package Structure

```
gtl/
├── pyproject.toml
├── README.md
├── src/
│   └── gtl/
│       ├── __init__.py
│       ├── cli.py          # CLI entry point
│       ├── sync.py         # Core sync logic
│       ├── git.py          # Git operations
│       ├── bigquery.py     # BigQuery operations
│       └── schema.sql      # Schema definitions
└── tests/
    └── ...
```

## CLI Interface

```bash
# Initialize schema (run once per dataset)
gtl init --project=my-project --dataset=git_repo

# Sync current repository
gtl sync --project=my-project --dataset=git_repo

# Sync with custom repo ID
gtl sync --project=my-project --dataset=git_repo --repo-id=github.com/org/repo

# Sync with custom file size limit (default 100KB)
gtl sync --project=my-project --dataset=git_repo --max-file-size=50000
```

## Configuration

The CLI should support configuration via:

1. Command-line arguments (highest priority)
2. Environment variables
3. `.gtl.yaml` config file in repo root (lowest priority)

| CLI Arg | Env Var | Config Key | Description |
|---------|---------|------------|-------------|
| `--project` | `GTL_PROJECT` | `project` | GCP project ID |
| `--dataset` | `GTL_DATASET` | `dataset` | BigQuery dataset name |
| `--repo-id` | `GTL_REPO_ID` | `repo_id` | Repository identifier (auto-detected from git remote if not set) |
| `--max-file-size` | `GTL_MAX_FILE_SIZE` | `max_file_size` | Max file size in bytes (default: 102400) |

## Core Functions

### git.py

```python
def get_repo_id() -> str:
    """Auto-detect repo ID from git remote origin URL."""

def get_new_commits(last_sha: str | None) -> list[dict]:
    """Get commits since last_sha with metadata."""

def get_file_changes(sha: str, parent_sha: str | None) -> list[dict]:
    """Get per-file diffs for a commit. Returns list with:
    - file_path, change_type, old_path, diff, additions, deletions
    """

def get_current_files(max_size: int) -> list[dict]:
    """Get all current text files in repo with contents."""

def is_binary(data: bytes) -> bool:
    """Check if content is binary (has null bytes)."""
```

### bigquery.py

```python
def ensure_schema(client, dataset: str):
    """Create tables if they don't exist."""

def ensure_repo(client, dataset: str, repo_id: str, name: str, url: str):
    """Insert or update repository record."""

def get_last_commit_sha(client, dataset: str, repo_id: str) -> str | None:
    """Get most recent processed commit SHA for a repo."""

def insert_commits(client, dataset: str, commits: list[dict]):
    """Batch insert commits."""

def insert_file_changes(client, dataset: str, changes: list[dict]):
    """Batch insert file changes."""

def upsert_current_files(client, dataset: str, repo_id: str, files: list[dict]):
    """Replace current files for a repo using MERGE."""
```

### sync.py

```python
def sync(project: str, dataset: str, repo_id: str, max_file_size: int):
    """Main sync orchestration:
    1. Get last processed commit
    2. Get new commits since then
    3. For each commit, get file changes and insert
    4. Update current_files with latest state
    """
```

## Sync Logic

```
1. Get last_sha from BigQuery for this repo
2. Get new commits since last_sha (or all if first run)
3. For each commit (oldest first):
   a. Insert commit record
   b. Get per-file diffs
   c. Insert file_changes records
4. After all commits processed:
   a. Get current files from git working tree
   b. MERGE into current_files table (delete removed, upsert existing)
```

## GitHub Actions Usage

In any repo that wants to sync:

### .github/workflows/sync-to-bigquery.yml

```yaml
name: Sync to BigQuery

on:
  push:
    branches: [master]

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY }}

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - run: |
          pip install gtl
          gtl sync --project=my-project --dataset=git_repo
```

Or with config file:

### .gtl.yaml

```yaml
project: my-project
dataset: git_repo
max_file_size: 102400
```

```yaml
# Simplified workflow
- run: |
    pip install gtl
    gtl sync
```

## Setup Steps

### 1. Create the gtl Package Repository

1. Create new repo `gtl`
2. Implement package structure as described above
3. Publish to PyPI or private package index

### 2. GCP Setup (One-time)

```bash
# Create service account
gcloud iam service-accounts create gtl-sync \
  --display-name="GTL BigQuery Sync"

# Grant BigQuery access
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:gtl-sync@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

# Generate key
gcloud iam service-accounts keys create key.json \
  --iam-account=gtl-sync@${PROJECT_ID}.iam.gserviceaccount.com
```

### 3. Initialize BigQuery Schema

```bash
pip install gtl
gtl init --project=my-project --dataset=git_repo
```

### 4. Configure Each Repository

1. Add `GCP_SA_KEY` secret to repo (Settings → Secrets → Actions)
2. Add workflow file `.github/workflows/sync-to-bigquery.yml`
3. Optionally add `.gtl.yaml` for config

## Example Queries

**Get all commits for a repo:**
```sql
SELECT sha, author_name, committed_at, message
FROM `project.git_repo.commits`
WHERE repo_id = 'github.com/org/repo'
ORDER BY committed_at DESC;
```

**See what files changed in a commit:**
```sql
SELECT file_path, change_type, additions, deletions
FROM `project.git_repo.file_changes`
WHERE repo_id = 'github.com/org/repo'
  AND commit_sha = 'abc123...'
ORDER BY file_path;
```

**View diff for a specific file in a commit:**
```sql
SELECT diff
FROM `project.git_repo.file_changes`
WHERE repo_id = 'github.com/org/repo'
  AND commit_sha = 'abc123...'
  AND file_path = 'src/main.py';
```

**Get history of changes to a file:**
```sql
SELECT c.sha, c.author_name, c.committed_at, c.message, 
       f.change_type, f.additions, f.deletions
FROM `project.git_repo.commits` c
JOIN `project.git_repo.file_changes` f 
  ON c.repo_id = f.repo_id AND c.sha = f.commit_sha
WHERE c.repo_id = 'github.com/org/repo'
  AND f.file_path = 'src/main.py'
ORDER BY c.committed_at DESC;
```

**Current contents of a file:**
```sql
SELECT content
FROM `project.git_repo.current_files`
WHERE repo_id = 'github.com/org/repo'
  AND file_path = 'README.md';
```

**Find largest current files across all repos:**
```sql
SELECT repo_id, file_path, size_bytes
FROM `project.git_repo.current_files`
ORDER BY size_bytes DESC
LIMIT 20;
```

**Most frequently changed files:**
```sql
SELECT file_path, COUNT(*) as change_count, 
       SUM(additions) as total_additions,
       SUM(deletions) as total_deletions
FROM `project.git_repo.file_changes`
WHERE repo_id = 'github.com/org/repo'
GROUP BY file_path
ORDER BY change_count DESC
LIMIT 20;
```

**Activity across all repos:**
```sql
SELECT 
  repo_id,
  DATE(committed_at) as date,
  COUNT(*) as commits
FROM `project.git_repo.commits`
GROUP BY 1, 2
ORDER BY 2 DESC, 3 DESC;
```

## Notes

- Repo ID is auto-detected from `git remote get-url origin` if not specified
- First sync processes entire history; subsequent syncs are incremental
- Binary detection uses null-byte check in first 8KB
- `current_files` is fully replaced on each sync (MERGE with delete for removed files)
- Per-file diffs may be large; consider adding `--max-diff-size` option to truncate
- Renames are tracked with change_type "R" and old_path populated
