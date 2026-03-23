"""Auto alignment for source codes.

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

__author__ = "Markus Uhle"
__copyright__ = "Synergetik GmbH"

# -----------------------------------------------------------------------------
# -- linter control
# -----------------------------------------------------------------------------

# ruff: noqa: T201    - print found
# ruff: noqa: ERA001  - documented-out code found
# ruff: noqa: PTH118  - use Path from pathlib instead of os.path.join
# ruff: noqa: PTH120  - use Path.parent from pathlib
# ruff: noqa: PTH207  - Replace `glob` with `Path.glob` or `Path.rglob`
# ruff: noqa: S605    - Starting a process with a shell, possible injection detected
# ruff: noqa: PLR0911 - Too many return statements
# ruff: noqa: SIM102  - Use a single `if` statement instead of nested `if`
# ruff: noqa: PLR2004 - Magic value used in comparison, consider replacing with a constant variable


# -----------------------------------------------------------------------------
# -- module import
# -----------------------------------------------------------------------------

import sys
from pathlib import Path


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def find_delimiter_outside_string(line: str, delimiter: str) -> int:
    """Find delimiter.

    Only outside string and not in inline comment.
    Only outside ()
    Only outside []
    Returns -1 if not found.
    """
    in_string = False
    round_brackets = 0
    square_brackets = 0
    string_char = ""

    # Go through line from left to right
    idx = 0
    while idx < len(line):
        # Get next char
        char = line[idx]

        # Only process char if not inside a string
        if not in_string:
            # If a comment starts outside a string, stop searching.
            if char == "#":
                break

            # Check for the start of a string.
            if char in {'"', "'"}:
                in_string = True
                string_char = char

            # Check for brackets
            elif char == "(":
                round_brackets = round_brackets + 1

            elif char == "[":
                square_brackets = square_brackets + 1

            elif char == ")":
                round_brackets = round_brackets - 1

            elif char == "]":
                square_brackets = square_brackets - 1

            # Check for delimiter
            elif char == delimiter and round_brackets == 0 and square_brackets == 0:
                return idx

        # Inside string - Skip escaped characters.
        elif char == "\\":
            idx += 1

        # Inside string - Check for string end
        elif char == string_char:
            in_string = False
            string_char = ""

        # Next char
        idx += 1

    # Delimiter not found
    return -1


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def qualifies(line: str, delimiter: str) -> bool:
    """Line qualifier."""
    # -------------------------------------------
    # Easy de-qualifiers
    # -------------------------------------------

    # Empty line
    if line.strip() == "":
        return False

    # A line starting with a comment
    if line.lstrip().startswith("#"):
        return False

    # Delimiter is not present
    if delimiter not in line:
        return False

    # More complex test if delimiter is not present
    index = find_delimiter_outside_string(line, delimiter)
    if index < 0:
        return False

    # -------------------------------------------
    # Special de-qualifiers
    # -------------------------------------------

    # Only use code part, not use trailing inline comment
    code_part = line.split("#", 1)[0]

    # For '=' alignment, exclude lines that have "==" or "!=" in the code portion.
    if delimiter == "=":
        if any(operator in code_part for operator in ("==", "!=", "+=", "-=", "*=", "/=", ">=", "<=")):
            return False

    # For '=' or ':' alignment, check that there's content after the delimiter.
    trailing_text = code_part[index + 1 :]
    if trailing_text.strip() == "":
        return False

    # If both '=' and ':' exist, only qualify if the chosen delimiter appears first.
    other = None
    if delimiter == "=":
        other = ":"
    elif delimiter == ":":
        other = "="

    if other:
        index_other = find_delimiter_outside_string(line, other)
        if index_other >= 0:
            if index_other < index:
                return False

    # Now, the delimiter is contained in the line and the line was not sorted out
    return True


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def align_block(lines: list[str], delimiter: str) -> list[str]:
    """Align block."""
    # Build array with the column position of the delimiter in each line
    # positions = [line.find(delimiter) for line in lines]
    positions = [find_delimiter_outside_string(line, delimiter) for line in lines]
    # Should never happen
    if -1 in positions:
        print("Fatal error")
        sys.exit(0)

    # Check if any of the line has the separator without space on the left side
    add_space = any(line[pos - 1] != " " for line, pos in zip(lines, positions, strict=True))

    # Get the rightmost delimiter position
    target = max(positions)

    aligned_lines = []

    # Iterate through each line with the corresponding position
    for line, pos in zip(lines, positions, strict=True):
        #
        # Calculate number of spaces to insert
        diff = target - pos

        # Split line into 'before delimiter' and 'rest including delimiter'
        before, after = line.split(delimiter, 1)

        # Add calculated number of spaces to the first part
        if diff > 0:
            before = before + (" " * diff)

        # Add space if left part was directly ending at separator  (e.g. var=)
        if add_space:
            before = before + " "

        # Build aligned line and append to resulting lines
        new_line = before + delimiter + " " + after.lstrip()
        aligned_lines.append(new_line)

    return aligned_lines


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def process_lines(lines: list[str], delimiter: str) -> list[str]:
    """Process lines."""
    new_lines = []
    block = []
    in_docstring = False

    # -------------------------------------------------
    # Process all source code lines
    # -------------------------------------------------
    line_cnt = 0
    for line in lines:
        # Update line counter
        line_cnt = line_cnt + 1

        # Debug breakpoint possibility
        if line_cnt == 152:
            print(f"Breakpoint at line {line_cnt}")

        # -------------------------------------------------
        # Update docstring state
        # -------------------------------------------------
        docstring_border = False
        if '"""' in line or "'''" in line:
            triple_count = line.count('"""') + line.count("'''")

            # Toggle in_docstring if an odd number of triple quotes appear.
            if triple_count % 2 == 1:
                in_docstring = not in_docstring

            # Mark line containing a triple-quote
            docstring_border = True

        # -------------------------------------------------
        # Docstrings will not be processed
        # This is a multi-line test which cannot be handled by qualifies function
        # -------------------------------------------------
        if docstring_border or in_docstring:
            #
            # If there are any lines in the actual block, then process block
            if block:
                if len(block) > 1:
                    block = align_block(block, delimiter)

                new_lines.extend(block)
                block = []

            # Flush actual line which causes the block to end
            new_lines.append(line)
            continue

        # -------------------------------------------------
        # Normal processing: add lines that qualify to the block.
        # -------------------------------------------------
        if qualifies(line, delimiter):
            block.append(line)
            continue

        # -------------------------------------------------
        # Normal processing: Flush if not qualified
        # -------------------------------------------------
        if block:
            if len(block) > 1:
                block = align_block(block, delimiter)

            new_lines.extend(block)
            block = []

        new_lines.append(line)

    # -------------------------------------------------
    # Finalize after last line
    # -------------------------------------------------
    if block:
        if len(block) > 1:
            block = align_block(block, delimiter)

        new_lines.extend(block)

    return new_lines


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
def main() -> None:
    """Align sourcecode."""
    # First argument: path to this script
    # Second argument: file to be processed
    if len(sys.argv) < 2:
        print("Debug mode active")
        default_file = Path(sys.argv[0]).resolve().parent / "align_tests" / "align_test.py"
        sys.argv.append(str(default_file))
        print(f"{sys.argv[0]}   {sys.argv[1]}")

    # Check arguments
    input_path = Path(sys.argv[1])

    # Exclude own sourcecode (otherwise debugging could destroy ourself) and testing stuff
    if input_path.name in ("align.py", "align_result_expected.py", "align_result.py"):
        print(f"{input_path} excluded from processing")
        return

    # Open source code file
    with input_path.open() as f:
        lines = f.readlines()

    # First, align consecutive lines containing '='
    aligned_lines = process_lines(lines, "=")

    # Next, align consecutive lines containing ':'
    aligned_lines2 = process_lines(aligned_lines, ":")

    # Next, align consecutive lines containing '#'
    # aligned_lines3 = process_lines(aligned_lines2, "#")

    # Write back file
    output_path = input_path

    # For testing, generate an extra file instead of overwriting
    if input_path.name == "align_test.py":
        output_path = input_path.with_name("align_result.py")

    # Write back
    with output_path.open("w") as f:
        f.writelines(aligned_lines2)


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    """Align python sourcecode.

    Let

    The following setting must be done in ./vscode/settings.json to automatically call
    this script after a file was saved (and processed by other tools which are enabled
    with editor.defaultFormatter)

        "emeraldwalk.runonsave": {
            "commands": [
                {
                "match": "\\.py$",
                "cmd": "${workspaceFolder}/.venv/Scripts/python ${workspaceFolder}/tools/align.py \"${file}\""
                }
            ]
        },
    """
    main()
