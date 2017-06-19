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
from jog import JogFormatter
from prometheus_client import start_http_server, Gauge
from prometheus_client.core import GaugeMetricFamily, REGISTRY

from prometheus_es_exporter import cluster_health_parser
from prometheus_es_exporter import indices_stats_parser
from prometheus_es_exporter import nodes_stats_parser
from prometheus_es_exporter.parser import parse_response

gauges = {}


def format_label_value(value_list):
    return '_'.join(value_list).replace('.', '_')


def format_metric_name(name_list):
    return '_'.join(name_list).replace('.', '_')


def group_metrics(metrics):
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

    return metric_dict


def update_gauges(metrics):
    metric_dict = group_metrics(metrics)

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


def gauge_generator(metrics):
    metric_dict = group_metrics(metrics)

    for metric_name, (label_keys, value_dict) in metric_dict.items():
        # If we have label keys we may have multiple different values,
        # each with their own label values.
        if label_keys:
            gauge = GaugeMetricFamily(metric_name, '', labels=label_keys)

            for label_values, value in value_dict.items():
                gauge.add_metric(label_values, value)

        # No label keys, so we must have only a single value.
        else:
            gauge = GaugeMetricFamily(metric_name, '', value=list(value_dict.values())[0])

        yield gauge


def run_query(es_client, name, indices, query, timeout):
    try:
        response = es_client.search(index=indices, body=query, request_timeout=timeout)

        metrics = parse_response(response, [name])
    except Exception:
        logging.exception('Error while querying indices [%s], query [%s].', indices, query)
    else:
        update_gauges(metrics)


class ClusterHealthCollector(object):
    def __init__(self, es_client, level):
        self.es_client = es_client
        self.level = level

    def collect(self):
        try:
            response = self.es_client.cluster.health(level=self.level)

            metrics = cluster_health_parser.parse_response(response, ['es', 'cluster_health'])
        except Exception:
            logging.exception('Error while fetching cluster health.')
        else:
            yield from gauge_generator(metrics)


class NodesStatsCollector(object):
    def __init__(self, es_client, metrics=None):
        self.es_client = es_client
        self.metrics = metrics

    def collect(self):
        try:
            response = self.es_client.nodes.stats(metric=self.metrics)

            metrics = nodes_stats_parser.parse_response(response, ['es', 'nodes_stats'])
        except Exception:
            logging.exception('Error while fetching nodes stats.')
        else:
            yield from gauge_generator(metrics)


class IndicesStatsCollector(object):
    def __init__(self, es_client, parse_indices, metrics=None, fields=None):
        self.es_client = es_client
        self.parse_indices = parse_indices
        self.metrics = metrics
        self.fields = fields

    def collect(self):
        try:
            response = self.es_client.indices.stats(metric=self.metrics, fields=self.fields)

            metrics = indices_stats_parser.parse_response(response, self.parse_indices, ['es', 'indices_stats'])
        except Exception:
            logging.exception('Error while fetching indices stats.')
        else:
            yield from gauge_generator(metrics)


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


def csv_choice_arg_parser(choices, arg):
    metrics = arg.split(',')

    invalid_metrics = []
    for metric in metrics:
        if metric not in choices:
            invalid_metrics.append(metric)

    if invalid_metrics:
        msg = 'invalid metric(s): "{}" in "{}" (choose from {})' \
            .format(','.join(invalid_metrics), arg, ','.join(choices))
        raise argparse.ArgumentTypeError(msg)

    return metrics


# https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-nodes-stats.html#_nodes_statistics
NODES_STATS_METRICS_OPTIONS = [
    'indices', 'fs', 'http', 'jvm', 'os',
    'process', 'thread_pool', 'transport',
    'breaker', 'discovery', 'ingest'
]
nodes_stats_metrics_parser = partial(csv_choice_arg_parser, NODES_STATS_METRICS_OPTIONS)


'completion,docs,fielddata,flush,get,indexing,merge,query_cache,recovery,refresh,request_cache,search,segments,store,suggest,translog,warmer'

# https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-nodes-stats.html#node-indices-stats
INDICES_STATS_METRICS_OPTIONS = [
    'completion', 'docs', 'fielddata',
    'flush', 'get', 'indexing', 'merge',
    'query_cache', 'recovery', 'refresh',
    'request_cache', 'search', 'segments',
    'store', 'suggest', 'translog', 'warmer'
]
indices_stats_metrics_parser = partial(csv_choice_arg_parser, INDICES_STATS_METRICS_OPTIONS)


def indices_stats_fields_parser(arg):
    if arg == '*':
        return arg
    else:
        return arg.split(',')


def main():
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(description='Export ES query results to Prometheus.')
    parser.add_argument('-e', '--es-cluster', default='localhost',
                        help='addresses of nodes in a Elasticsearch cluster to run queries on. Nodes should be separated by commas e.g. es1,es2. Ports can be provided if non-standard (9200) e.g. es1:9999 (default: localhost)')
    parser.add_argument('--ca-certs',
                        help='path to a CA certificate bundle. Can be absolute, or relative to the current working directory. If not specified, SSL certificate verification is disabled.')
    parser.add_argument('-p', '--port', type=int, default=9206,
                        help='port to serve the metrics endpoint on. (default: 9206)')
    parser.add_argument('--query-disable', action='store_true',
                        help='disable query monitoring. Config file does not need to be present if query monitoring is disabled.')
    parser.add_argument('-c', '--config-file', default='exporter.cfg',
                        help='path to query config file. Can be absolute, or relative to the current working directory. (default: exporter.cfg)')
    parser.add_argument('--cluster-health-disable', action='store_true',
                        help='disable cluster health monitoring.')
    parser.add_argument('--cluster-health-level', default='indices', choices=['cluster', 'indices', 'shards'],
                        help='level of detail for cluster health monitoring.  (default: indices)')
    parser.add_argument('--nodes-stats-disable', action='store_true',
                        help='disable nodes stats monitoring.')
    parser.add_argument('--nodes-stats-metrics', type=nodes_stats_metrics_parser,
                        help='limit nodes stats to specific metrics. Metrics should be separated by commas e.g. indices,fs.')
    parser.add_argument('--indices-stats-disable', action='store_true',
                        help='disable indices stats monitoring.')
    parser.add_argument('--indices-stats-mode', default='cluster', choices=['cluster', 'indices'],
                        help='detail mode for indices stats monitoring. (default: cluster)')
    parser.add_argument('--indices-stats-metrics', type=indices_stats_metrics_parser,
                        help='limit indices stats to specific metrics. Metrics should be separated by commas e.g. indices,fs.')
    parser.add_argument('--indices-stats-fields', type=indices_stats_fields_parser,
                        help='include fielddata info for specific fields. Fields should be separated by commas e.g. indices,fs. Use \'*\' for all.')
    parser.add_argument('-j', '--json-logging', action='store_true',
                        help='turn on json logging.')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='detail level to log. (default: INFO)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='turn on verbose (DEBUG) logging. Overrides --log-level.')
    args = parser.parse_args()

    log_handler = logging.StreamHandler()
    log_format = '[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s'
    formatter = JogFormatter(log_format) if args.json_logging else logging.Formatter(log_format)
    log_handler.setFormatter(formatter)

    log_level = getattr(logging, args.log_level)
    logging.basicConfig(
        handlers=[log_handler],
        level=logging.DEBUG if args.verbose else log_level
    )
    logging.captureWarnings(True)

    port = args.port
    es_cluster = args.es_cluster.split(',')
    if args.ca_certs:
        es_client = Elasticsearch(es_cluster, verify_certs=True, ca_certs=args.ca_certs)
    else:
        es_client = Elasticsearch(es_cluster, verify_certs=False)

    scheduler = None

    if not args.query_disable:
        scheduler = sched.scheduler()

        config = configparser.ConfigParser()
        config.read_file(open(args.config_file))

        query_prefix = 'query_'
        queries = {}
        for section in config.sections():
            if section.startswith(query_prefix):
                query_name = section[len(query_prefix):]
                query_interval = config.getfloat(section, 'QueryIntervalSecs', fallback=15)
                query_timeout = config.getfloat(section, 'QueryTimeoutSecs', fallback=10)
                query_indices = config.get(section, 'QueryIndices', fallback='_all')
                query = json.loads(config.get(section, 'QueryJson'))

                queries[query_name] = (query_interval, query_timeout, query_indices, query)

        if queries:
            for name, (interval, timeout, indices, query) in queries.items():
                func = partial(run_query, es_client, name, indices, query, timeout)
                run_scheduler(scheduler, interval, func)
        else:
            logging.warn('No queries found in config file %s', args.config_file)

    if not args.cluster_health_disable:
        REGISTRY.register(ClusterHealthCollector(es_client, args.cluster_health_level))

    if not args.nodes_stats_disable:
        REGISTRY.register(NodesStatsCollector(es_client, metrics=args.nodes_stats_metrics))

    if not args.indices_stats_disable:
        parse_indices = args.indices_stats_mode == 'indices'
        REGISTRY.register(IndicesStatsCollector(es_client, parse_indices,
                                                metrics=args.indices_stats_metrics,
                                                fields=args.indices_stats_fields))

    logging.info('Starting server...')
    start_http_server(port)
    logging.info('Server started on port %s', port)

    try:
        if scheduler:
            scheduler.run()
        else:
            while True:
                time.sleep(5)
    except KeyboardInterrupt:
        pass

    shutdown()
