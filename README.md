# GTL - Git-Transform-Load

Sync Git repository history to BigQuery.

## Installation

```bash
pip install gtl
```

## Usage

### Initialize Schema

Run once per BigQuery dataset to create the required tables:

```bash
gtl init --project=my-project --dataset=git_repo
```

### Sync Repository

Sync the current repository to BigQuery:

```bash
gtl sync --project=my-project --dataset=git_repo
```

Options:
- `--repo-id`: Override auto-detected repository identifier
- `--branch`: Specific branch to sync (defaults to current branch)
- `--all-branches`: Sync all branches in the repository
- `--max-file-size`: Maximum file size in bytes (default: 102400)
- `-v, --verbose`: Print verbose output

### Branch Syncing

By default, GTL syncs the currently checked out branch. You can specify a different branch or sync all branches:

```bash
# Sync a specific branch
gtl sync --project=my-project --dataset=git_repo --branch=develop

# Sync all branches
gtl sync --project=my-project --dataset=git_repo --all-branches
```

## Configuration

GTL supports configuration via (in priority order):

1. Command-line arguments
2. Environment variables
3. `.gtl.yaml` config file

| CLI Arg | Env Var | Config Key | Description |
|---------|---------|------------|-------------|
| `--project` | `GTL_PROJECT` | `project` | GCP project ID |
| `--dataset` | `GTL_DATASET` | `dataset` | BigQuery dataset name |
| `--repo-id` | `GTL_REPO_ID` | `repo_id` | Repository identifier |
| `--branch` | `GTL_BRANCH` | `branch` | Branch to sync |
| `--max-file-size` | `GTL_MAX_FILE_SIZE` | `max_file_size` | Max file size (default: 102400) |

Example `.gtl.yaml`:

```yaml
project: my-project
dataset: git_repo
branch: main
max_file_size: 102400
```

## GitHub Actions

```yaml
name: Sync to BigQuery

on:
  push:
    branches: [main, develop]

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
          gtl sync --project=my-project --dataset=git_repo --branch=${{ github.ref_name }}
```

To sync all branches on a schedule:

```yaml
name: Sync All Branches to BigQuery

on:
  schedule:
    - cron: '0 0 * * *'  # Daily at midnight

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
          gtl sync --project=my-project --dataset=git_repo --all-branches
```

## BigQuery Schema

### repositories
Tracks synced repositories.

### branches
Tracks branches for each repository with their HEAD SHA.

### commits
Stores commit metadata (sha, branch, author, timestamp, message).

### file_changes
Stores per-file diffs for each commit.

### current_files
Stores current file contents per branch (updated on each sync).

## License

MIT
