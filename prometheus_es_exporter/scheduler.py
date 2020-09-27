import time
import logging

log = logging.getLogger(__name__)


def schedule_job(scheduler, executor, interval, func, *args, **kwargs):
    """
    Schedule a function to be run on a fixed interval.

    Works with schedulers from the stdlib sched module.
    """

    def scheduled_run(scheduled_time, *args, **kwargs):
        def run_func(func, *args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception:
                log.exception('Error while running scheduled job.')

        if executor is not None:
            executor.submit(run_func, func, *args, **kwargs)
        else:
            run_func(func, *args, **kwargs)

        current_time = time.monotonic()
        next_scheduled_time = scheduled_time + interval
        while next_scheduled_time < current_time:
            next_scheduled_time += interval

        scheduler.enterabs(time=next_scheduled_time,
                           priority=1,
                           action=scheduled_run,
                           argument=(next_scheduled_time, *args),
                           kwargs=kwargs)

    next_scheduled_time = time.monotonic()
    scheduler.enterabs(time=next_scheduled_time,
                       priority=1,
                       action=scheduled_run,
                       argument=(next_scheduled_time, *args),
                       kwargs=kwargs)
