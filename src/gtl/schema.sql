-- GTL BigQuery Schema
-- Tables for storing Git repository history

-- Table: repositories
-- Tracks each repository being synced
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.repositories` (
    id STRING NOT NULL,
    name STRING,
    url STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table: commits
-- Stores commit metadata
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.commits` (
    repo_id STRING NOT NULL,
    sha STRING NOT NULL,
    author_name STRING,
    author_email STRING,
    committed_at TIMESTAMP,
    message STRING,
    parent_sha STRING,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table: file_changes
-- Stores per-file diffs for each commit
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.file_changes` (
    repo_id STRING NOT NULL,
    commit_sha STRING NOT NULL,
    file_path STRING NOT NULL,
    change_type STRING,
    old_path STRING,
    diff STRING,
    additions INT64,
    deletions INT64,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table: current_files
-- Stores current state of files (overwritten on each sync)
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.current_files` (
    repo_id STRING NOT NULL,
    file_path STRING NOT NULL,
    content STRING,
    size_bytes INT64,
    last_commit_sha STRING,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
