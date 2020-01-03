import functools
import logging
import signal
import sys

from collections import OrderedDict

log = logging.getLogger(__name__)


def merge_dicts_ordered(*dict_args, **extra_entries):
    """
    Given an arbitrary number of dictionaries, merge them into a
    single new dictionary. Later dictionaries take precedence if
    a key is shared by multiple dictionaries.

    Extra entries can also be provided via kwargs. These entries
    have the highest precedence.
    """
    res = OrderedDict()

    for d in dict_args + (extra_entries,):
        res.update(d)

    return res


def log_exceptions(exit_on_exception=False):
    """
    Logs any exceptions raised.

    By default, exceptions are then re-raised. If set to exit on exception,
    sys.exit(1) is called instead.
    """

    def decorator(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                if exit_on_exception:
                    log.exception('Unrecoverable exception encountered. Exiting.')
                    sys.exit(1)
                else:
                    log.exception('Exception encountered.')
                    raise

        return wrapper

    return decorator


def nice_shutdown(shutdown_signals=(signal.SIGINT, signal.SIGTERM)):
    """
    Logs shutdown signals nicely.

    Installs handlers for the shutdown signals (SIGINT and SIGTERM by default)
    that log the signal that has been received, and then raise SystemExit.
    The original handlers are restored before returning.
    """

    def sig_handler(signum, _):
        log.info('Received signal %(signal)s.',
                 {'signal': signal.Signals(signum).name})
        # Raise SystemExit to bypass (most) try/except blocks.
        sys.exit()

    def decorator(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Setup new shutdown handlers, storing the old ones for later.
            old_handlers = {}
            for sig in shutdown_signals:
                old_handlers[sig] = signal.signal(sig, sig_handler)

            try:
                return func(*args, **kwargs)

            finally:
                # Restore the old handlers
                for sig, old_handler in old_handlers.items():
                    signal.signal(sig, old_handler)

        return wrapper

    return decorator
