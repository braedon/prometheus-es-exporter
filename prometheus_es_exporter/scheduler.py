import time
import logging

log = logging.getLogger(__name__)


def schedule_jobs(last_schedule, scheduler, executor, load, func, client):
    """
    Schedule a function to be run on a fixed interval.

    def run_query(es_client, query_name, indices, query,
              timeout, on_error, on_missing):

    Works with schedulers from the stdlib sched module.
    """
    def scheduled_run_once(executor, func, *args, **kwargs):
        log.info(f"Runing scheduled {args}")
        def run_func(func, *args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception:
                log.exception('Error while running scheduled job.')

        if executor is not None:
            executor.submit(run_func, func, *args, **kwargs)
        else:
            run_func(func, *args, **kwargs)

    queries = load.load()
    time_mono = time.monotonic()
    for query_name, (interval, timeout, indices, query,
                        on_error, on_missing) in queries.items():
        if (query_name not in last_schedule) or (last_schedule[query_name] + interval <= time_mono):
            log.info(f"SHEDULED NOW: {query_name}")
            last_schedule[query_name] = time_mono
            scheduler.enterabs(time=time.monotonic(),
                            priority=1,
                            action=scheduled_run_once,
                            argument=(executor, func, client, query_name, indices, query,
                            timeout, on_error, on_missing),
                            )
    time.sleep(0.01)
    scheduler.enterabs(time=time.monotonic(),
                    priority=1,
                    action=schedule_jobs,
                    argument=(last_schedule, scheduler, executor, load, func, client),
                    )

