import argparse
import configparser
import json
import logging
import sched
import signal
import sys
import time

from elasticsearch import Elasticsearch
from functools import partial
from logstash_formatter import LogstashFormatterV1
from prometheus_client import start_http_server, Gauge

from prometheus_es_exporter import cluster_health_parser
from prometheus_es_exporter import indices_stats_parser
from prometheus_es_exporter import nodes_stats_parser
from prometheus_es_exporter.parser import parse_response

gauges = {}


def format_label_value(value_list):
    return '_'.join(value_list).replace('.', '_')


def format_metric_name(name_list):
    return '_'.join(name_list).replace('.', '_')


def update_gauges(metrics):
    metric_dict = {}
    for (name_list, label_dict, value) in metrics:
        metric_name = format_metric_name(name_list)
        if metric_name not in metric_dict:
            metric_dict[metric_name] = (tuple(label_dict.keys()), {})

        label_keys = metric_dict[metric_name][0]
        label_values = tuple([
            format_label_value(label_dict[key])
            for key in label_keys
        ])

        metric_dict[metric_name][1][label_values] = value

    for metric_name, (label_keys, value_dict) in metric_dict.items():
        if metric_name in gauges:
            (old_label_values_set, gauge) = gauges[metric_name]
        else:
            old_label_values_set = set()
            gauge = Gauge(metric_name, '', label_keys)

        new_label_values_set = set(value_dict.keys())

        for label_values in old_label_values_set - new_label_values_set:
            gauge.remove(*label_values)

        for label_values, value in value_dict.items():
            if label_values:
                gauge.labels(*label_values).set(value)
            else:
                gauge.set(value)

        gauges[metric_name] = (new_label_values_set, gauge)


def run_query(es_client, name, indices, query):
    try:
        response = es_client.search(index=indices, body=query)

        metrics = parse_response(response, [name])
    except Exception:
        logging.exception('Error while querying indices [%s], query [%s].', indices, query)
    else:
        update_gauges(metrics)


def get_cluster_health(es_client, level):
    try:
        response = es_client.cluster.health(level=level)

        metrics = cluster_health_parser.parse_response(response, ['es', 'cluster_health'])
    except Exception:
        logging.exception('Error while fetching cluster health.')
    else:
        update_gauges(metrics)


def get_nodes_stats(es_client):
    try:
        response = es_client.nodes.stats()

        metrics = nodes_stats_parser.parse_response(response, ['es', 'nodes_stats'])
    except Exception:
        logging.exception('Error while fetching nodes stats.')
    else:
        update_gauges(metrics)


def get_indices_stats(es_client, parse_indices):
    try:
        response = es_client.indices.stats()

        metrics = indices_stats_parser.parse_response(response, parse_indices, ['es', 'indices_stats'])
    except Exception:
        logging.exception('Error while fetching indices stats.')
    else:
        update_gauges(metrics)


def run_scheduler(scheduler, interval, func):
    def scheduled_run(scheduled_time,):
        try:
            func()
        except Exception:
            logging.exception('Error while running scheduled job.')

        current_time = time.monotonic()
        next_scheduled_time = scheduled_time + interval
        while next_scheduled_time < current_time:
            next_scheduled_time += interval

        scheduler.enterabs(
            next_scheduled_time,
            1,
            scheduled_run,
            (next_scheduled_time,)
        )

    next_scheduled_time = time.monotonic()
    scheduler.enterabs(
        next_scheduled_time,
        1,
        scheduled_run,
        (next_scheduled_time,)
    )


def shutdown():
    logging.info('Shutting down')
    sys.exit(1)


def signal_handler(signum, frame):
    shutdown()


def main():
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(description='Export ES query results to Prometheus.')
    parser.add_argument('-e', '--es-cluster', default='localhost',
                        help='addresses of nodes in a Elasticsearch cluster to run queries on. Nodes should be separated by commas e.g. es1,es2. Ports can be provided if non-standard (9200) e.g. es1:9999 (default: localhost)')
    parser.add_argument('-p', '--port', type=int, default=8080,
                        help='port to serve the metrics endpoint on. (default: 8080)')
    parser.add_argument('--query-disable', action='store_true',
                        help='disable query monitoring. Config file does not need to be present if query monitoring is disabled.')
    parser.add_argument('-c', '--config-file', default='exporter.cfg',
                        help='path to query config file. Can be absolute, or relative to the current working directory. (default: exporter.cfg)')
    parser.add_argument('--cluster-health-disable', action='store_true',
                        help='disable cluster health monitoring.')
    parser.add_argument('--cluster-health-interval', type=float, default=10,
                        help='polling interval for cluster health monitoring in seconds. (default: 10)')
    parser.add_argument('--cluster-health-level', default='indices', choices=['cluster', 'indices', 'shards'],
                        help='level of detail for cluster health monitoring.  (default: indices)')
    parser.add_argument('--nodes-stats-disable', action='store_true',
                        help='disable nodes stats monitoring.')
    parser.add_argument('--nodes-stats-interval', type=float, default=10,
                        help='polling interval for nodes stats monitoring in seconds. (default: 10)')
    parser.add_argument('--indices-stats-disable', action='store_true',
                        help='disable indices stats monitoring.')
    parser.add_argument('--indices-stats-interval', type=float, default=10,
                        help='polling interval for indices stats monitoring in seconds. (default: 10)')
    parser.add_argument('--indices-stats-mode', default='cluster', choices=['cluster', 'indices'],
                        help='detail mode for indices stats monitoring.  (default: cluster)')
    parser.add_argument('-j', '--json-logging', action='store_true',
                        help='turn on json logging.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='turn on verbose logging.')
    args = parser.parse_args()

    log_handler = logging.StreamHandler()
    log_format = '[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s'
    formatter = LogstashFormatterV1() if args.json_logging else logging.Formatter(log_format)
    log_handler.setFormatter(formatter)

    logging.basicConfig(
        handlers=[log_handler],
        level=logging.DEBUG if args.verbose else logging.INFO
    )
    logging.captureWarnings(True)

    port = args.port
    es_cluster = args.es_cluster.split(',')
    es_client = Elasticsearch(es_cluster, verify_certs=False)

    scheduler = sched.scheduler()

    if not args.query_disable:
        config = configparser.ConfigParser()
        config.read_file(open(args.config_file))

        query_prefix = 'query_'
        queries = {}
        for section in config.sections():
            if section.startswith(query_prefix):
                query_name = section[len(query_prefix):]
                query_interval = config.getfloat(section, 'QueryIntervalSecs')
                query_indices = config.get(section, 'QueryIndices', fallback='_all')
                query = json.loads(config.get(section, 'QueryJson'))

                queries[query_name] = (query_interval, query_indices, query)

        if queries:
            for name, (interval, indices, query) in queries.items():
                func = partial(run_query, es_client, name, indices, query)
                run_scheduler(scheduler, interval, func)
        else:
            logging.warn('No queries found in config file %s', args.config_file)

    if not args.cluster_health_disable:
        cluster_health_func = partial(get_cluster_health, es_client, args.cluster_health_level)
        run_scheduler(scheduler, args.cluster_health_interval, cluster_health_func)

    if not args.nodes_stats_disable:
        nodes_stats_func = partial(get_nodes_stats, es_client)
        run_scheduler(scheduler, args.nodes_stats_interval, nodes_stats_func)

    if not args.indices_stats_disable:
        parse_indices = args.indices_stats_mode == 'indices'
        indices_stats_func = partial(get_indices_stats, es_client, parse_indices)
        run_scheduler(scheduler, args.indices_stats_interval, indices_stats_func)

    logging.info('Starting server...')
    start_http_server(port)
    logging.info('Server started on port %s', port)

    try:
        scheduler.run()
    except KeyboardInterrupt:
        pass

    shutdown()
