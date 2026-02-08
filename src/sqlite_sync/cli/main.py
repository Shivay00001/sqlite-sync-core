"""
cli/main.py - Entry point for sqlite-sync CLI.

Re-exports the CLI from ext/cli/main.py for pyproject.toml compatibility.
"""

from sqlite_sync.ext.cli.main import main, CLIManager

__all__ = ["main", "CLIManager"]

if __name__ == "__main__":
    main()
