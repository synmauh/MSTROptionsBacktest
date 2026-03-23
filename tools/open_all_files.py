"""Open all Python project files so that ruff and sonarlint can analyze them.

COPYRIGHT BY SYNERGETIK GMBH
The copyright of this source code(s) herein is the property of
Synergetik GmbH, Schiffweiler, Germany. (www.synergetik.de)
The program(s) may be used only with the written permission of
Synergetik GmbH or in accordance with the terms and conditions stipulated
in an agreement/contract under which the program(s) have been supplied.
Examples (not exclusive) of restrictions:
    - all sources are confidential and under NDA
    - giving these sources to other people/companies is not allowed
    - Using these sources in other projects is not allowed
    - copying parts of these sources is not allowed
    - changing these sources is not allowed
"""

__author__    = "Markus Uhle"
__copyright__ = "Synergetik GmbH"


# -----------------------------------------------------------------------------
# -- linter control
# -----------------------------------------------------------------------------

# ruff : noqa: T201   - print found
# ruff : noqa: ERA001 - documented-out code found
# ruff : noqa: PTH118 - use Path from pathlib instead of os.path.join
# ruff : noqa: PTH120 - use Path.parent from pathlib
# ruff : noqa: PTH207 - Replace `glob` with `Path.glob` or `Path.rglob`
# ruff : noqa: S605   - Starting a process with a shell, possible injection detected


# -----------------------------------------------------------------------------
# -- module import
# -----------------------------------------------------------------------------

import glob
import os

# -----------------------------------------------------------------------------
# Script
# -----------------------------------------------------------------------------


# Define the source directory relative to this script's location
workspace_dir = os.path.join(os.path.dirname(__file__), "..")

# List of file extensions to open
extensions = (".py", ".toml")

# Define source directories
src_dirs = [
    os.path.join(workspace_dir, "src"),
]

# Define directories to exclude
exclude_dirs = []

# Normalize paths to absolute paths and ensure trailing slashes for clarity
exclude_dirs = [os.path.normpath(ex_dir) + os.sep for ex_dir in exclude_dirs]
src_dirs     = [os.path.normpath(src_dir) + os.sep for src_dir in src_dirs]


# List of file extensions to open
extensions = (".py", ".toml")

count = 0

# Iterate over each directory in src_dirs
for src_dir in src_dirs:
    # Construct the glob pattern for file search in each directory
    pattern = os.path.join(src_dir, "**/*")

    # Find and print each file with the specified extensions
    for file_path in glob.glob(pattern, recursive=True):
        # Check if the file is in an excluded directory
        if any(file_path.startswith(exclude_dir) for exclude_dir in exclude_dirs):
            # print(f"{file_path}  -- Skipping")
            continue

        if file_path.endswith(extensions):
            print(file_path)
            os.system(f'code "{file_path}"')
            count = count + 1

    print(f"Total files: {count}")
