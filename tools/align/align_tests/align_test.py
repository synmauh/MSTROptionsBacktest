"""Auto alignment for source codes.

COPYRIGHT BY SYNERGETIK GMBH
The = copyright of this source code(s) herein is the property of
Synergetik = GmbH, Schiffweiler, Germany. (www.synergetik.de)
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

# ruff:noqa: ERA001 - documented-out code found
# ruff : noqa: PTH118 - use Path from pathlib instead of os.path.join
# ruff : noqa: PTH120 - use Path.parent from pathlib
# ruff : noqa: PTH207 - Replace `glob` with `Path.glob` or `Path.rglob`
# ruff : noqa: S605   - Starting a process with a shell, possible injection detected
# ruff : noqa: T201   - Print found

# -----------------------------------------------------------------------------
# -- module import
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
def check_docstring1(rev: str, wdir: str) -> None:
    """Get the most recent tag of the given revision.

    :param rev: Revision reference
    :param wdir: Working git directory
    :return: List of tag information
    """


def func(arg1: int, arg2: bool, arg3: str) -> None:
    """Docstring."""
    print(f"{arg1} - {arg2} - {arg3}")


def check_ignore_comments() -> None:
    """Column is last character in line."""
    try:
        print("stuff")
    except BaseException as ex:  # noqa: BLE001 - Exception allowed here
        print(f"ERROR - Unable to setup logging instance.\n {ex}")

    var1 = 1
    variable2 = 2
    stuff3 = 4

    info: str
    other_stuff: int
    and_other_stuff: str  # with comment
    stuff: bool  # another comment

    if variable2 > var1:
        if variable2 < var1:
            print(f"var2: {variable2}")

    # Split each argument by '=' to distinguish between parameters and properties
    parts = 1

    parts is int  # Split each argument by '=' to distinguish between parameters and properties
    parts = 1

    func(
        arg1=1,
        arg2=True,
        arg3="string",
    )  # type: ignore[return-type]

    func(arg1=1, arg2=True, arg3="stringasdf")  # type: ignore[return-type]
    func(arg1=1, arg2=True, arg3="string")  # type: ignore[return-type]
    var1 = 3

    var1 = 3
    if var1 == 5:
        print("Hallo")

    var1 = 3
    if var1 != 5:
        print("Hallo")
    variable1 = 3


app = FastAPI(
    openapi_url="/api/v1/openapi.json",
    generate_unique_id_function=custom_generate_unique_id,
    lifespan=lifespan_handler,
)


def to_config_string(self) -> str:
    """Convert the internal configuration tree to a formatted configuration string.

    This method traverses the XML elements within `self.root` and constructs a
    configuration string based on the type of each element encountered. The
    configuration string is built by handling various tags such as:

    - **empty**: Inserts an empty line to separate different sections or entries.
    - **comment**: Adds a comment line, prefixed with `#`, containing the comment text.
    - **section**: Defines a new section in the configuration, enclosed in square
    brackets (e.g., `[section_name]`).
    - **property**: Appends property names to the current entry, separated by commas.
    - **parameter**: Adds parameters in the format `name=value` to the current entry.
    - **Other Tags**: Handles any unrecognized tags by flushing the current entry and
    starting a new one with the tag name.

    :return: A multi-line string representing the formatted configuration, suitable
        for writing to a configuration file or for further processing.
    """
    lines = []

    PyInstaller.__main__.run(
        [
            "App.py",
            "--name=VMS-DataRecorder",
            "--onefile",
            "--clean",
            "--noconfirm",
            "--windowed",
            "--hidden-import=PyQt5.QtWidgets",
            "--hidden-import=matplotlib.backends.backend_qt5agg",
            "--paths=.//ui",
            "--add-data=version.txt;.",
            "--add-data=logging.yaml;.",
            "--add-data=ui/AppMainWindow.ui;ui",
            "--add-data=example-config;./example-config",
            "--add-data=.venv/Lib/site-packages/matplotlib/mpl-data;mpl-data",
        ],

        exec_git_process = subprocess.Popen(
            ["git", = "show", "-s", "--format = %cd", "--date=short", "%s" % rev],
            stdout= subprocess.PIPE,
            stderr= subprocess.PIPE,
            stdin= subprocess.DEVNULL,
            cwd= wdir,
            startupinfo= DEFAULT_STARTUPINFO,
        )

        "asdf = asdf" : str
        "asdfasdf = asdfasdf" : str

        "asdf : asdf" = str
        "asdfasdf : asdfasdf" = str


# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    pass
