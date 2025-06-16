#!/usr/bin/env python3
"""
Version bumping script for pyspark-analyzer.

Usage:
    python scripts/bump_version.py patch  # 0.1.1 -> 0.1.2
    python scripts/bump_version.py minor  # 0.1.1 -> 0.2.0
    python scripts/bump_version.py major  # 0.1.1 -> 1.0.0
"""

import subprocess
import sys
from pathlib import Path


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ["patch", "minor", "major"]:
        print(__doc__)
        sys.exit(1)

    bump_type = sys.argv[1]
    project_root = Path(__file__).parent.parent

    # Ensure we're in the right directory
    if not (project_root / ".bumpversion.cfg").exists():
        print("Error: .bumpversion.cfg not found. Are you in the project root?")
        sys.exit(1)

    # Run bump2version
    try:
        subprocess.run(
            ["bump2version", bump_type],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
        )
        print(f"Successfully bumped {bump_type} version!")

        # Show the new version
        result = subprocess.run(
            ["grep", "current_version", ".bumpversion.cfg"],
            cwd=project_root,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            version_line = result.stdout.strip()
            new_version = version_line.split("=")[1].strip()
            print(f"New version: {new_version}")
            print("\nNext steps:")
            print("1. Push the changes: git push && git push --tags")
            print("2. The release workflow will automatically trigger")
            print(
                "3. Monitor the release at: https://github.com/bjornvandijkman1993/pyspark-analyzer/actions"
            )

    except subprocess.CalledProcessError as e:
        print("Error: Failed to bump version")
        if e.stderr:
            print(f"Error details: {e.stderr}")
        sys.exit(1)


if __name__ == "__main__":
    main()
