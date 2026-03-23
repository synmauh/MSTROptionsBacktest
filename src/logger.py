"""Application wide logging module that provides a `ColoredFormatter` for enhanced log readability.

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

__author__ = "Vitalij Mast"
__copyright__ = "Synergetik GmbH"

# -----------------------------------------------------------------------------
# -- module import
# -----------------------------------------------------------------------------

import logging
import logging.config
from pathlib import Path
from types import MappingProxyType

import yaml

# -----------------------------------------------------------------------------
# -- custom module import
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# -- ColoredFormatter class
# -----------------------------------------------------------------------------
class ColoredFormatter(logging.Formatter):
    """A logging formatter that applies color formatting based on log level to improve readability."""

    # -----------------------------------------------------------------------------
    # Constants and definitions
    # -----------------------------------------------------------------------------

    # fmt: off
    FS_BLUE     = "\x1b[34m"
    FS_MAGENTA  = "\x1b[35;22m"
    FS_YELLOW   = "\x1b[33;22m"
    FS_RED      = "\x1b[31;22m"
    FS_RED_BOLD = "\x1b[31;1m"
    FS_RESET    = "\x1b[0m"

    FORMATS = MappingProxyType(
        {
            logging.DEBUG   : f"{FS_MAGENTA}%s{FS_RESET}",
            logging.INFO    : f"{FS_BLUE}%s{FS_RESET}",
            logging.WARNING : f"{FS_YELLOW}%s{FS_RESET}",
            logging.ERROR   : f"{FS_RED}%s{FS_RESET}",
            logging.CRITICAL: f"{FS_RED_BOLD}%s{FS_RESET}",
        },
    )
    # fmt: on

    # -----------------------------------------------------------------------------
    # Static class members
    # -----------------------------------------------------------------------------

    SupportsColorOutput = True

    # -----------------------------------------------------------------------------
    # Public methods
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    def format(self, record: "logging.LogRecord") -> str:
        """Format the specified log record, applying color formatting based on the log level if color output is enabled.

        :param record: The log record to format
        :return: The formatted log message with appropriate color, if color
            output is enabled; otherwise, a standard log message format.
        """
        if ColoredFormatter.SupportsColorOutput:
            log_fmt = self.FORMATS.get(record.levelno, "")
            formatter = logging.Formatter(log_fmt % self._fmt)
        else:
            formatter = logging.Formatter(self._fmt)
        return formatter.format(record)


# -----------------------------------------------------------------------------
# -- logging setup
# -----------------------------------------------------------------------------
def configure_logger(log_config_file: str = "logging.yaml") -> None:
    """Configure the logging system with a custom `ColoredFormatter` and load logging configuration from a YAML file.

    :param log_config_file: The path to the logging configuration file in YAML format, defaults to "logging.yaml"
    """
    if not hasattr(logging, "ColoredFormatter"):
        setattr(logging, "ColoredFormatter", ColoredFormatter)  # noqa: B010 - Monkey patch: property does not exists

        # load logging configuration file, if any
        try:
            path = Path(log_config_file)
            if path.is_file():
                with path.open("rt") as file:
                    config = yaml.safe_load(file.read())
                    logging.config.dictConfig(config)

                logging.info("logging.yaml loaded successfully.")
        except Exception as ex:  # noqa: BLE001 - Exception allowed here
            print(f"ERROR: Unable to setup logging instance.\n {ex}")  # noqa: T201 - print found
