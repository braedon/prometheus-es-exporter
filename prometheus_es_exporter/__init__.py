import click
import configparser
import glob
import json
import logging
import os
import re
import sched
import signal
import sys
import time

from collections import OrderedDict
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionTimeout
from functools import partial
from jog import JogFormatter
from prometheus_client import start_http_server, Gauge
from prometheus_client.core import GaugeMetricFamily, REGISTRY

from prometheus_es_exporter import cluster_health_parser
from prometheus_es_exporter import indices_stats_parser
from prometheus_es_exporter import nodes_stats_parser
from prometheus_es_exporter.parser import parse_response

CONTEXT_SETTINGS = {
    'help_option_names': ['-h', '--help']
}

gauges = {}

metric_invalid_chars = re.compile(r'[^a-zA-Z0-9_:]')
metric_invalid_start_chars = re.compile(r'^[^a-zA-Z_:]')
label_invalid_chars = re.compile(r'[^a-zA-Z0-9_]')
label_invalid_start_chars = re.compile(r'^[^a-zA-Z_]')
label_start_double_under = re.compile(r'^__+')


def format_label_key(label_key):
    label_key = re.sub(label_invalid_chars, '_', label_key)
    label_key = re.sub(label_invalid_start_chars, '_', label_key)
    label_key = re.sub(label_start_double_under, '_', label_key)
    return label_key


def format_label_value(value_list):
    return '_'.join(value_list)


def format_metric_name(name_list):
    metric = '_'.join(name_list)
    metric = re.sub(metric_invalid_chars, '_', metric)
    metric = re.sub(metric_invalid_start_chars, '_', metric)
    return metric


def group_metrics(metrics):
    metric_dict = {}
    for (name_list, label_dict, value) in metrics:
        metric_name = format_metric_name(name_list)
        label_dict = OrderedDict([(format_label_key(k), format_label_value(v))
                                  for k, v in label_dict.items()])

        if metric_name not in metric_dict:
            metric_dict[metric_name] = (tuple(label_dict.keys()), {})

        label_keys = metric_dict[metric_name][0]
        label_values = tuple([label_dict[key]
                              for key in label_keys])

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


def collector_up_gauge(name_list, description, succeeded=True):
    metric_name = format_metric_name(name_list + ['up'])
    description = 'Did the {} fetch succeed.'.format(description)
    return GaugeMetricFamily(metric_name, description, value=int(succeeded))


class ClusterHealthCollector(object):
    def __init__(self, es_client, timeout, level):
        self.metric_name_list = ['es', 'cluster_health']
        self.description = 'Cluster Health'

        self.es_client = es_client
        self.timeout = timeout
        self.level = level

    def collect(self):
        try:
            response = self.es_client.cluster.health(level=self.level, request_timeout=self.timeout)

            metrics = cluster_health_parser.parse_response(response, self.metric_name_list)
        except ConnectionTimeout:
            logging.warn('Timeout while fetching %s (timeout %ss).', self.description, self.timeout)
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        except Exception:
            logging.exception('Error while fetching %s.', self.description)
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        else:
            yield from gauge_generator(metrics)
            yield collector_up_gauge(self.metric_name_list, self.description)


class NodesStatsCollector(object):
    def __init__(self, es_client, timeout, metrics=None):
        self.metric_name_list = ['es', 'nodes_stats']
        self.description = 'Nodes Stats'

        self.es_client = es_client
        self.timeout = timeout
        self.metrics = metrics

    def collect(self):
        try:
            response = self.es_client.nodes.stats(metric=self.metrics, request_timeout=self.timeout)

            metrics = nodes_stats_parser.parse_response(response, self.metric_name_list)
        except ConnectionTimeout:
            logging.warn('Timeout while fetching %s (timeout %ss).', self.description, self.timeout)
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        except Exception:
            logging.exception('Error while fetching %s.', self.description)
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        else:
            yield from gauge_generator(metrics)
            yield collector_up_gauge(self.metric_name_list, self.description)


class IndicesStatsCollector(object):
    def __init__(self, es_client, timeout, parse_indices=False, metrics=None, fields=None):
        self.metric_name_list = ['es', 'indices_stats']
        self.description = 'Indices Stats'

        self.es_client = es_client
        self.timeout = timeout
        self.parse_indices = parse_indices
        self.metrics = metrics
        self.fields = fields

    def collect(self):
        try:
            response = self.es_client.indices.stats(metric=self.metrics, fields=self.fields, request_timeout=self.timeout)

            metrics = indices_stats_parser.parse_response(response, self.parse_indices, self.metric_name_list)
        except ConnectionTimeout:
            logging.warn('Timeout while fetching %s (timeout %ss).', self.description, self.timeout)
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        except Exception:
            logging.exception('Error while fetching %s.', self.description)
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        else:
            yield from gauge_generator(metrics)
            yield collector_up_gauge(self.metric_name_list, self.description)


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


# Based on click.Choice
class MultiChoice(click.ParamType):
    """The choice type allows a value to be checked against a fixed set
    of supported values. All of these values have to be strings.

    Multiple values can be provided, separated by commas.

    You should only pass a list or tuple of choices. Other iterables
    (like generators) may lead to surprising results.

    :param case_sensitive: Set to false to make choices case
        insensitive. Defaults to true.
    """

    name = 'multi-choice'

    def __init__(self, choices, case_sensitive=True):
        self.choices = choices
        self.case_sensitive = case_sensitive

    def get_metavar(self, param):
        return '[%s]' % '|'.join(self.choices)

    def get_missing_message(self, param):
        return 'Choose one or more from:\n\t%s.' % ',\n\t'.join(self.choices)

    def convert_one(self, value, param, ctx):
        # Exact match
        if value in self.choices:
            return value

        # Match through normalization and case sensitivity
        # first do token_normalize_func, then lowercase
        # preserve original `value` to produce an accurate message in
        # `self.fail`
        normed_value = value
        normed_choices = self.choices

        if ctx is not None and \
           ctx.token_normalize_func is not None:
            normed_value = ctx.token_normalize_func(value)
            normed_choices = [ctx.token_normalize_func(choice) for choice in
                              self.choices]

        if not self.case_sensitive:
            normed_value = normed_value.lower()
            normed_choices = [choice.lower() for choice in normed_choices]

        if normed_value in normed_choices:
            return normed_value

        return None

    def convert(self, value, param, ctx):
        values = value.split(',')

        valid_choices = []
        invalid_values = []
        for value in values:
            choice = self.convert_one(value, param, ctx)
            if choice is None:
                invalid_values.append(value)
            else:
                valid_choices.append(choice)

        if invalid_values:
            msg = 'invalid choice(s): %s (choose from %s)' % \
                (', '.join(invalid_values), ', '.join(self.choices))
            self.fail(msg, param, ctx)

        return valid_choices

    def __repr__(self):
        return 'MultiChoice(%r)' % list(self.choices)


# https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-nodes-stats.html#_nodes_statistics
NODES_STATS_METRICS_OPTIONS = [
    'indices', 'fs', 'http', 'jvm', 'os',
    'process', 'thread_pool', 'transport',
    'breaker', 'discovery', 'ingest'
]


# https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-nodes-stats.html#node-indices-stats
INDICES_STATS_METRICS_OPTIONS = [
    'completion', 'docs', 'fielddata',
    'flush', 'get', 'indexing', 'merge',
    'query_cache', 'recovery', 'refresh',
    'request_cache', 'search', 'segments',
    'store', 'suggest', 'translog', 'warmer'
]


def indices_stats_fields_parser(ctx, param, value):
    if value is None:
        return None

    if value == '*':
        return value
    else:
        return value.split(',')


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--es-cluster', '-e', default='localhost',
              help='Addresses of nodes in a Elasticsearch cluster to run queries on. '
                   'Nodes should be separated by commas e.g. es1,es2. '
                   'Ports can be provided if non-standard (9200) e.g. es1:9999. '
                   'Include the scheme for non-http nodes e.g. https://es1:9200. '
                   '--ca-certs must be provided for SSL certificate verification. '
                   '(default: localhost)')
@click.option('--ca-certs',
              help='Path to a CA certificate bundle. '
                   'Can be absolute, or relative to the current working directory. '
                   'If not specified, SSL certificate verification is disabled.')
@click.option('--client-cert',
              help='Path to a SSL client certificate. '
                   'Can be absolute, or relative to the current working directory. '
                   'If not specified, SSL client authentication is disabled.')
@click.option('--client-key',
              help='Path to a SSL client key. '
                   'Can be absolute, or relative to the current working directory. '
                   'Must be specified if "--client-cert" is provided.')
@click.option('--basic-user',
              help='Username for basic authentication with nodes. '
                   'If not specified, basic authentication is disabled.')
@click.option('--basic-password',
              help='Password for basic authentication with nodes. '
                   'Must be specified if "--basic-user" is provided.')
@click.option('--port', '-p', default=9206,
              help='Port to serve the metrics endpoint on. (default: 9206)')
@click.option('--query-disable', default=False, is_flag=True,
              help='Disable query monitoring. '
                   'Config file does not need to be present if query monitoring is disabled.')
@click.option('--config-file', '-c', default='exporter.cfg', type=click.File(),
              help='Path to query config file. '
                   'Can be absolute, or relative to the current working directory. '
                   '(default: exporter.cfg)')
@click.option('--config-dir', default='./config', type=click.Path(file_okay=False),
              help='Path to query config directory. '
                   'If present, any files ending in ".cfg" in the directory '
                   'will be parsed as additional query config files. '
                   'Merge order is main config file, then config directory files '
                   'in filename order. '
                   'Can be absolute, or relative to the current working directory. '
                   '(default: ./config)')
@click.option('--cluster-health-disable', default=False, is_flag=True,
              help='Disable cluster health monitoring.')
@click.option('--cluster-health-timeout', default=10.0,
              help='Request timeout for cluster health monitoring, in seconds. (default: 10)')
@click.option('--cluster-health-level', default='indices',
              type=click.Choice(['cluster', 'indices', 'shards']),
              help='Level of detail for cluster health monitoring.  (default: indices)')
@click.option('--nodes-stats-disable', default=False, is_flag=True,
              help='Disable nodes stats monitoring.')
@click.option('--nodes-stats-timeout', default=10.0,
              help='Request timeout for nodes stats monitoring, in seconds. (default: 10)')
@click.option('--nodes-stats-metrics',
              type=MultiChoice(NODES_STATS_METRICS_OPTIONS),
              help='Limit nodes stats to specific metrics. '
                   'Metrics should be separated by commas e.g. indices,fs.')
@click.option('--indices-stats-disable', default=False, is_flag=True,
              help='Disable indices stats monitoring.')
@click.option('--indices-stats-timeout', default=10.0,
              help='Request timeout for indices stats monitoring, in seconds. (default: 10)')
@click.option('--indices-stats-mode', default='cluster',
              type=click.Choice(['cluster', 'indices']),
              help='Detail mode for indices stats monitoring. (default: cluster)')
@click.option('--indices-stats-metrics',
              type=MultiChoice(INDICES_STATS_METRICS_OPTIONS),
              help='Limit indices stats to specific metrics. '
                   'Metrics should be separated by commas e.g. indices,fs.')
@click.option('--indices-stats-fields',
              callback=indices_stats_fields_parser,
              help='Include fielddata info for specific fields. '
                   'Fields should be separated by commas e.g. indices,fs. '
                   'Use \'*\' for all.')
@click.option('--json-logging', '-j', default=False, is_flag=True,
              help='Turn on json logging.')
@click.option('--log-level', default='INFO',
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
              help='Detail level to log. (default: INFO)')
@click.option('--verbose', '-v', default=False, is_flag=True,
              help='Turn on verbose (DEBUG) logging. Overrides --log-level.')
def cli(**options):
    """Export Elasticsearch query results to Prometheus."""

    signal.signal(signal.SIGTERM, signal_handler)

    if options['basic_user'] and options['basic_password'] is None:
        click.BadOptionUsage('basic_user', 'Username provided with no password.')
    elif options['basic_user'] is None and options['basic_password']:
        click.BadOptionUsage('basic_password', 'Password provided with no username.')
    elif options['basic_user']:
        http_auth = (options['basic_user'], options['basic_password'])
    else:
        http_auth = None

    if not options['ca_certs'] and options['client_cert']:
        click.BadOptionUsage('client_cert', '--client-cert can only be used when --ca-certs is provided.')
    elif not options['ca_certs'] and options['client_key']:
        click.BadOptionUsage('client_key', '--client-key can only be used when --ca-certs is provided.')
    elif options['client_cert'] and not options['client_key']:
        click.BadOptionUsage('client_cert', '--client-key must be provided when --client-cert is used.')
    elif not options['client_cert'] and options['client_key']:
        click.BadOptionUsage('client_key', '--client-cert must be provided when --client-key is used.')

    log_handler = logging.StreamHandler()
    log_format = '[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s'
    formatter = JogFormatter(log_format) if options['json_logging'] else logging.Formatter(log_format)
    log_handler.setFormatter(formatter)

    log_level = getattr(logging, options['log_level'])
    logging.basicConfig(
        handlers=[log_handler],
        level=logging.DEBUG if options['verbose'] else log_level
    )
    logging.captureWarnings(True)

    port = options['port']
    es_cluster = options['es_cluster'].split(',')

    if options['ca_certs']:
        es_client = Elasticsearch(es_cluster,
                                  verify_certs=True,
                                  ca_certs=options['ca_certs'],
                                  client_cert=options['client_cert'],
                                  client_key=options['client_key'],
                                  http_auth=http_auth)
    else:
        es_client = Elasticsearch(es_cluster,
                                  verify_certs=False,
                                  http_auth=http_auth)

    scheduler = None

    if not options['query_disable']:
        scheduler = sched.scheduler()

        config = configparser.ConfigParser()
        config.read_file(options['config_file'])

        config_dir_file_pattern = os.path.join(options['config_dir'], '*.cfg')
        config_dir_sorted_files = sorted(glob.glob(config_dir_file_pattern))
        config.read(config_dir_sorted_files)

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
            logging.warn('No queries found in config file %s', options['config_file'])

    if not options['cluster_health_disable']:
        REGISTRY.register(ClusterHealthCollector(es_client,
                                                 options['cluster_health_timeout'],
                                                 options['cluster_health_level']))

    if not options['nodes_stats_disable']:
        REGISTRY.register(NodesStatsCollector(es_client,
                                              options['nodes_stats_timeout'],
                                              metrics=options['nodes_stats_metrics']))

    if not options['indices_stats_disable']:
        parse_indices = options['indices_stats_mode'] == 'indices'
        REGISTRY.register(IndicesStatsCollector(es_client,
                                                options['indices_stats_timeout'],
                                                parse_indices=parse_indices,
                                                metrics=options['indices_stats_metrics'],
                                                fields=options['indices_stats_fields']))

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


def main():
    cli(auto_envvar_prefix='ES_EXPORTER')
