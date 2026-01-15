"""CLI entry point for gtl."""

import os
from pathlib import Path

import click
import yaml

from .sync import sync as do_sync, init as do_init


def load_config() -> dict:
    """Load configuration from .gtl.yaml if it exists."""
    config_path = Path(".gtl.yaml")
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f) or {}
    return {}


def get_config_value(
    cli_value: str | int | None,
    env_var: str,
    config_key: str,
    config: dict,
    default: str | int | None = None,
) -> str | int | None:
    """Get configuration value with priority: CLI > env > config file > default."""
    if cli_value is not None:
        return cli_value

    env_value = os.environ.get(env_var)
    if env_value is not None:
        return env_value

    if config_key in config:
        return config[config_key]

    return default


@click.group()
@click.version_option()
def main():
    """GTL - Git-Transform-Load: Sync Git repository history to BigQuery."""
    pass


@main.command()
@click.option(
    "--project",
    help="GCP project ID",
)
@click.option(
    "--dataset",
    help="BigQuery dataset name",
)
@click.option(
    "-v", "--verbose",
    is_flag=True,
    help="Print verbose output",
)
def init(project: str | None, dataset: str | None, verbose: bool):
    """Initialize BigQuery schema.

    Creates the dataset and tables if they don't exist.
    Run this once per dataset before syncing repositories.
    """
    config = load_config()

    project = get_config_value(project, "GTL_PROJECT", "project", config)
    dataset = get_config_value(dataset, "GTL_DATASET", "dataset", config)

    if not project:
        raise click.ClickException("--project is required (or set GTL_PROJECT env var)")
    if not dataset:
        raise click.ClickException("--dataset is required (or set GTL_DATASET env var)")

    try:
        do_init(
            project=project,
            dataset=dataset,
            verbose=verbose,
        )
    except Exception as e:
        raise click.ClickException(str(e))


@main.command()
@click.option(
    "--project",
    help="GCP project ID",
)
@click.option(
    "--dataset",
    help="BigQuery dataset name",
)
@click.option(
    "--repo-id",
    help="Repository identifier (auto-detected from git remote if not set)",
)
@click.option(
    "--branch",
    help="Branch to sync (defaults to current branch)",
)
@click.option(
    "--all-branches",
    is_flag=True,
    help="Sync all branches",
)
@click.option(
    "--max-file-size",
    type=int,
    help="Maximum file size in bytes (default: 102400)",
)
@click.option(
    "-v", "--verbose",
    is_flag=True,
    help="Print verbose output",
)
def sync(
    project: str | None,
    dataset: str | None,
    repo_id: str | None,
    branch: str | None,
    all_branches: bool,
    max_file_size: int | None,
    verbose: bool,
):
    """Sync current repository to BigQuery.

    Processes all new commits since the last sync and updates
    the current file contents in BigQuery.

    By default, syncs the current branch. Use --branch to specify
    a different branch, or --all-branches to sync all branches.
    """
    config = load_config()

    project = get_config_value(project, "GTL_PROJECT", "project", config)
    dataset = get_config_value(dataset, "GTL_DATASET", "dataset", config)
    repo_id = get_config_value(repo_id, "GTL_REPO_ID", "repo_id", config)
    branch = get_config_value(branch, "GTL_BRANCH", "branch", config)
    max_file_size = get_config_value(
        max_file_size,
        "GTL_MAX_FILE_SIZE",
        "max_file_size",
        config,
        default=102400,
    )

    # Convert max_file_size to int if it's a string from env
    if isinstance(max_file_size, str):
        max_file_size = int(max_file_size)

    if not project:
        raise click.ClickException("--project is required (or set GTL_PROJECT env var)")
    if not dataset:
        raise click.ClickException("--dataset is required (or set GTL_DATASET env var)")

    try:
        result = do_sync(
            project=project,
            dataset=dataset,
            repo_id=repo_id,
            branch=branch,
            all_branches=all_branches,
            max_file_size=max_file_size,
            verbose=verbose,
        )

        if verbose:
            click.echo("")
            click.echo("Summary:")
            click.echo(f"  Repository: {result['repo_id']}")
            if result.get('branches_synced'):
                click.echo(f"  Branches synced: {', '.join(result['branches_synced'])}")
            click.echo(f"  Commits processed: {result['commits_processed']}")
            click.echo(f"  File changes processed: {result['file_changes_processed']}")
            click.echo(f"  Current files updated: {result['current_files_updated']}")
        else:
            branches_info = ""
            if result.get('branches_synced'):
                branches_info = f" on {', '.join(result['branches_synced'])}"
            click.echo(
                f"Synced {result['commits_processed']} commits, "
                f"{result['file_changes_processed']} file changes{branches_info}"
            )

    except Exception as e:
        raise click.ClickException(str(e))


if __name__ == "__main__":
    main()
