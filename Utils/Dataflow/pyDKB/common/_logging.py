"""
pyDKB.common._logging

A wrapper for standard 'logging' module.
"""

import logging
from logging import CRITICAL, FATAL, ERROR, WARNING, WARN, INFO, DEBUG, NOTSET
import sys

__logging_version = int(logging.__version__.split('.')[1])

# ---------------------------------------------------
# Module variables
# ---------------------------------------------------
#
# Some variables are initialized in 'init()' function.
#
# Logger instance to be used in the module
logger = None
# Root logger for the whole library
_rootLogger = None
# Additional log level
TRACE = DEBUG / 2
logging.addLevelName(TRACE, 'TRACE')


# -------------------------------------------
# Custom implementation for 'logging' classes
# -------------------------------------------


class Logger(logging.Logger, object):
    """ Logger implementation, aware of 'TRACE' log level.

    New methods:
        * trace()     -- log with TRACE level;
        * traceback() -- log traceback with DEBUG level.
    """

    def trace(self, msg, *args, **kwargs):
        """ Log 'msg % args' with severity 'TRACE'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.trace("Houston, we have a %s", "interesting problem",
                     exc_info=1)
        """
        if self.isEnabledFor(TRACE):
            self._log(TRACE, msg, args, **kwargs)

    def traceback(self, **kwargs):
        """ Log traceback without additionat messages with severity 'DEBUG'.

        logger.traceback()
        """
        if self.isEnabledFor(DEBUG):
            if not (kwargs.get('exc_info')):
                kwargs['exc_info'] = 1
            self.debug('Traceback info:', **kwargs)


class RootLogger(Logger):
    """ Same as Logger, but must must have `Level` and be the only one. """
    def __init__(self, level, name='root'):
        """ Initialize new root logger. """
        Logger.__init__(self, name, level)


class MultilineFormatter(logging.Formatter, object):
    """ Formatter for multiline messages.

    Every extra line (directly in the message body, or the traceback)
    is:
      * prefixed with '  (==)';
      * suffixed with the part of format string which goes after
                 '%(message)s' part.
    """

    _suffix = None

    def __init__(self, *args):
        """ Split format string into message format and suffix. """
        new_args = list(args)
        if len(args):
            fmt = args[0]
            new_args[0], self._suffix = self.splitFormat(fmt)
        super(MultilineFormatter, self).__init__(*new_args)

    def splitFormat(self, fmt):
        """ Split format string into msg format and suffix.

        Suffix is everything that goes after the message body.
        """
        format, suffix = ('', '')
        splitted = fmt.split("%(message)s")
        if len(splitted) > 1:
            format = "%(message)s".join(splitted[:-1]) + "%(message)s"
            suffix = splitted[-1]
        else:
            format = fmt
        return format, suffix

    def formatExtra(self, lines, suffix=None, prefix="  (==) ", align=False):
        """ Format extra lines of the log message (traceback, ...).

        Parameter 'align' shows whether the suffix should be aligned
        to the right (by the longest line), or to the left (as for normal
        log messages).
        """
        if suffix is None:
            suffix = self._suffix
        if isinstance(lines, list) and len(lines):
            max_len = len(max(lines, key=len))
            if suffix and align:
                line_fmt = "%%(line)-%ds" % max_len
            else:
                line_fmt = "%(line)s"
            extra = prefix + line_fmt % {'line': lines[0]} + suffix
            for line in lines[1:]:
                extra += "\n" + prefix + line_fmt % {'line': line} + suffix
        else:
            extra = ""
        return extra

    def formatException(self, ei):
        """ Format traceback as extra lines. """
        s = super(MultilineFormatter, self).formatException(ei)
        lines = s.splitlines()
        exc_text = self.formatExtra(lines, align=True)
        return exc_text

    def format(self, record):
        """ Format multiline message.

        Second and further lines from initial message are formatted
        'extra' lines.
        """
        lines = record.msg.splitlines()
        msg = lines[0] if lines else ''
        extra = self.formatExtra(lines[1:])
        if extra and extra[:1] != '\n':
            extra = '\n' + extra
        record.msg = msg
        formatted = super(MultilineFormatter, self).format(record)
        lines = formatted.splitlines()
        msg = (lines[0] + self._suffix) if lines else ''
        if len(lines) > 1:
            extra += '\n'
        # Need to expand suffixes (as they are added after parent`s
        # 'format()' operation).
        result = (msg + extra + '\n'.join(lines[1:])) % record.__dict__
        return result


# --------------------------
# Module top-level functions
# --------------------------


def isInitialized():
    """ Checks if the module (namely, root logger) is initialized. """
    return isinstance(_rootLogger, Logger) and _rootLogger.handlers


def getRootLogger(**kwargs):
    """ Reconfigure and return library root logger.  """
    if kwargs or not isInitialized():
        configureRootLogger(**kwargs)
    return _rootLogger


def init(**kwargs):
    """ Initialize module.

    If already initialized, do nothing.
    """
    global logger
    if not isInitialized():
        initRootLogger(**kwargs)
        logger = getLogger(__name__)


def initRootLogger(**kwargs):
    """ Initialize root logger.

    If already initialized, do nothing.
    """
    global _rootLogger

    if isInitialized():
        return _rootLogger

    name = kwargs.get('name')
    if not name:
        name = __name__.split('.')[0]
    # Create new root logger
    root = RootLogger(WARNING, name)
    # Set Logger class 'root' to the new root
    Logger.root = root
    # Create new manager (object, responsible for new loggers creation)
    manager = logging.Manager(root)
    if __logging_version < 5:
        logging.setLoggerClass(Logger)
    else:
        manager.setLoggerClass(Logger)
    Logger.manager = manager
    root.manager = Logger.manager
    # 3) reset root logger for our class.
    Logger.root = root

    filename = kwargs.get('filename')
    if filename:
        mode = kwargs.get('mode', 'a')
        hdlr_name = "file_%s_%s" % (filename, mode)
        # Open file without delay, as presence of 'filename'
        # keyword supposes that function was called during
        # intended module initialization, not as a side effect
        # of calling 'getLogger()' before initializtion.
        hdlr = FileHandler(filename, mode)
    else:
        stream = kwargs.get('stream', sys.stderr)
        hdlr_name = "stream_%s" % stream.fileno()
        hdlr = logging.StreamHandler(stream)

    if __logging_version < 5:
        hdlr._name = hdlr_name
    else:
        hdlr.set_name(hdlr_name)

    fs = kwargs.get('msg_format', logging.BASIC_FORMAT)
    dfs = kwargs.get('datefmt', None)
    fmt = MultilineFormatter(fs, dfs)
    hdlr.setFormatter(fmt)
    root.addHandler(hdlr)

    level = kwargs.get('level')
    if level is not None:
        root.setLevel(level)
    _rootLogger = root

    return _rootLogger


def configureRootLogger(**kwargs):
    """ (Re)configure root logger. """
    if not isInitialized():
        return init(**kwargs)

    root = _rootLogger
    name = kwargs.get('name')
    if name and name != root.name:
        # Renaming is not allowed,
        # as logger name is the hierarchy-forming parameter
        logger.warning("Root logger ('%s') can not be renamed to '%s'.",
                       root.name, name)

    # Root logger is supposed to have exactly one handler,
    # so we count on this fact here
    old_hdlr = root.handlers[0]
    old_fmt = old_hdlr.formatter

    filename = kwargs.get('filename')
    stream = kwargs.get('stream', sys.stderr)
    if filename:
        mode = kwargs.get('mode', 'a')
        hdlr_name = "file_%s_%s" % (filename, mode)
        # Delay allows us not to open file right now,
        # but only when it is actually required
        hdlr = FileHandler(filename, mode, delay=True)
    elif stream:
        hdlr_name = "stream_%s" % stream.fileno()
        hdlr = logging.StreamHandler(stream)
    else:
        hdlr = old_hdlr

    if __logging_version < 5:
        if not getattr(hdlr, '_name', None):
            hdlr._name = hdlr_name
    else:
        if not hdlr.get_name():
            hdlr.set_name(hdlr_name)

    # Remove old handler or go on with it instead of the newly created one
    # (if it is configured just the same way).
    if old_hdlr != hdlr:
        if __logging_version < 5 \
                and (getattr(old_hdlr, '_name', None)
                     != getattr(hdlr, '_name', '')) \
                or __logging_version >= 5 \
                and old_hdlr.get_name() != hdlr.get_name():
            old_hdlr.close()
            root.removeHandler(old_hdlr)
            root.addHandler(hdlr)
        else:
            hdlr.close()
            hdlr = old_hdlr

    fs = kwargs.get("msg_format")
    dfs = kwargs.get("datefmt")

    # Check if handler formatter was configured earlier
    # and use old values if no new specified
    if isinstance(old_fmt, logging.Formatter):
        if not fs:
            fs = old_fmt._fmt
        if not dfs:
            dfs = old_fmt.datefmt
    elif not fs:
        fs = logging.BASIC_FORMAT

    fmt = MultilineFormatter(fs, dfs)
    hdlr.setFormatter(fmt)

    level = kwargs.get("level")
    if level is not None:
        root.setLevel(level)

    return root


def getLogger(name):
    """ Return logger with given name. """
    if not isInitialized():
        init()
    root = _rootLogger
    if name == root.name:
        return root
    return root.manager.getLogger(name)
