"""GTL - Git-Transform-Load: Sync Git repository history to BigQuery."""

from .sync import sync, init

__version__ = "0.1.0"
__all__ = ["sync", "init", "__version__"]
