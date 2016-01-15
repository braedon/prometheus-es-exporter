#!/usr/bin/python3
import configparser
import json
import sched
import time

from elasticsearch import Elasticsearch
from prometheus_client import start_http_server, Gauge

from parser import parse_response

gauges = {}

def format_label_value(value_list):
    return '_'.join(value_list)

def format_metric_name(name_list):
    return '_'.join(name_list)

def update_gauges(metrics):
    for (name_list, label_dict, value) in metrics:
        metric_name = format_metric_name(name_list)
        labels = {
            key: format_label_value(value_list)
            for key, value_list in label_dict.items()
        }

        if metric_name not in gauges:
            gauges[metric_name] = Gauge(metric_name, '', labels.keys())
        gauge = gauges[metric_name]

        if labels:
            gauge.labels(labels).set(value)
        else:
            gauge.set(value)

def run_query(query):
    response = client.search(body=query)
    return parse_response(response)

def run_scheduler(scheduler, es_client, name, interval, query):
    def scheduled_run(scheduled_time, interval):
        response = es_client.search(body=query)
        metrics = parse_response(response, [name])
        update_gauges(metrics)

        next_scheduled_time = scheduled_time + interval
        scheduler.enterabs(
            next_scheduled_time,
            1,
            scheduled_run,
            (next_scheduled_time, interval)
        )

    next_scheduled_time = time.monotonic()
    scheduler.enterabs(
        next_scheduled_time,
        1,
        scheduled_run,
        (next_scheduled_time, interval)
    )

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('exporter.cfg')

    port = config.getint('exporter', 'Port')
    es_hosts = config.get('elasticsearch', 'Hosts').split(',')

    query_prefix = 'query_'
    queries = {}
    for section in config.sections():
        if section.startswith(query_prefix):
            query_name = section[len(query_prefix):]
            query_interval = config.getfloat(section, 'QueryIntervalSecs')
            query = json.loads(config.get(section, 'QueryJson'))

            queries[query_name] = (query_interval, query)

    es_client = Elasticsearch(es_hosts)

    scheduler = sched.scheduler()

    print('Starting server...')
    start_http_server(port)
    print('Server started on port {}'.format(port))

    for name, (interval, query) in queries.items():
        run_scheduler(scheduler, es_client, name, interval, query)

    try:
        scheduler.run()
    except KeyboardInterrupt:
        pass

    print('Shutting down')
