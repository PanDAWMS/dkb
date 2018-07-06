"""
Common library for Data Knowledge Base development.
"""

import sys

__version__ = "0.3-SNAPSHOT"

__all__ = ["dataflow"]

try:
    import common
    logMsgFormat = '%(asctime)s: (%(levelname)s) %(message)s' \
                   ' (%(name)s)'
# Or, in case of multithreading:
#                   ' (%(name)s) (%(threadName)s)'
    common.logging.configureRootLogger(msg_format=logMsgFormat,
                                       level=common.logging.DEBUG)
    logger = common.logging.getLogger(__name__)
except (SyntaxError, ImportError), err:
    raise ImportError("%s (in submodule 'common')" % err)

try:
    import dataflow
except (SyntaxError, ImportError), err:
    logger.error("Failed to import submodule 'dataflow'")
    logger.traceback()
    raise ImportError("%s (in submodule 'dataflow')" % err)
