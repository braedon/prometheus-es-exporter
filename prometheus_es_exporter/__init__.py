import click
import click_config_file
import concurrent.futures
import configparser
import glob
import json
import logging
import os
import sched
import time

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionTimeout
from jog import JogFormatter
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY

from . import cluster_health_parser
from . import indices_aliases_parser
from . import indices_mappings_parser
from . import indices_stats_parser
from . import nodes_stats_parser
from .metrics import (group_metrics, gauge_generator,
                      format_metric_name, merge_metric_dicts)
from .parser import parse_response
from .scheduler import schedule_job
from .utils import log_exceptions, nice_shutdown

log = logging.getLogger(__name__)

CONTEXT_SETTINGS = {
    'help_option_names': ['-h', '--help']
}

METRICS_BY_QUERY = {}


def collector_up_gauge(name_list, description, succeeded=True):
    metric_name = format_metric_name(*name_list, 'up')
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
            metric_dict = group_metrics(metrics)
        except ConnectionTimeout:
            log.warning('Timeout while fetching %(description)s (timeout %(timeout_s)ss).',
                        {'description': self.description, 'timeout_s': self.timeout})
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        except Exception:
            log.exception('Error while fetching %(description)s.',
                          {'description': self.description})
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        else:
            yield from gauge_generator(metric_dict)
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
            metric_dict = group_metrics(metrics)
        except ConnectionTimeout:
            log.warning('Timeout while fetching %(description)s (timeout %(timeout_s)ss).',
                        {'description': self.description, 'timeout_s': self.timeout})
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        except Exception:
            log.exception('Error while fetching %(description)s.',
                          {'description': self.description})
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        else:
            yield from gauge_generator(metric_dict)
            yield collector_up_gauge(self.metric_name_list, self.description)


class IndicesAliasesCollector(object):
    def __init__(self, es_client, timeout):
        self.metric_name_list = ['es', 'indices_aliases']
        self.description = 'Indices Aliases'

        self.es_client = es_client
        self.timeout = timeout

    def collect(self):
        try:
            response = self.es_client.indices.get_alias(request_timeout=self.timeout)

            metrics = indices_aliases_parser.parse_response(response, self.metric_name_list)
            metric_dict = group_metrics(metrics)
        except ConnectionTimeout:
            log.warning('Timeout while fetching %(description)s (timeout %(timeout_s)ss).',
                        {'description': self.description, 'timeout_s': self.timeout})
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        except Exception:
            log.exception('Error while fetching %(description)s.',
                          {'description': self.description})
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        else:
            yield from gauge_generator(metric_dict)
            yield collector_up_gauge(self.metric_name_list, self.description)


class IndicesMappingsCollector(object):
    def __init__(self, es_client, timeout):
        self.metric_name_list = ['es', 'indices_mappings']
        self.description = 'Indices Mappings'

        self.es_client = es_client
        self.timeout = timeout

    def collect(self):
        try:
            response = self.es_client.indices.get_mapping(request_timeout=self.timeout)

            metrics = indices_mappings_parser.parse_response(response, self.metric_name_list)
            metric_dict = group_metrics(metrics)
        except ConnectionTimeout:
            log.warning('Timeout while fetching %(description)s (timeout %(timeout_s)ss).',
                        {'description': self.description, 'timeout_s': self.timeout})
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        except Exception:
            log.exception('Error while fetching %(description)s.',
                          {'description': self.description})
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        else:
            yield from gauge_generator(metric_dict)
            yield collector_up_gauge(self.metric_name_list, self.description)


class IndicesStatsCollector(object):
    def __init__(self, es_client, timeout, parse_indices=False,
                 indices=None, metrics=None, fields=None):
        self.metric_name_list = ['es', 'indices_stats']
        self.description = 'Indices Stats'

        self.es_client = es_client
        self.timeout = timeout
        self.parse_indices = parse_indices
        self.indices = indices
        self.metrics = metrics
        self.fields = fields

    def collect(self):
        try:
            response = self.es_client.indices.stats(index=self.indices,
                                                    metric=self.metrics,
                                                    fields=self.fields,
                                                    request_timeout=self.timeout)

            metrics = indices_stats_parser.parse_response(response,
                                                          self.parse_indices,
                                                          self.metric_name_list)
            metric_dict = group_metrics(metrics)
        except ConnectionTimeout:
            log.warning('Timeout while fetching %(description)s (timeout %(timeout_s)ss).',
                        {'description': self.description, 'timeout_s': self.timeout})
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        except Exception:
            log.exception('Error while fetching %(description)s.',
                          {'description': self.description})
            yield collector_up_gauge(self.metric_name_list, self.description, succeeded=False)
        else:
            yield from gauge_generator(metric_dict)
            yield collector_up_gauge(self.metric_name_list, self.description)


class QueryMetricCollector(object):

    def collect(self):
        # Copy METRICS_BY_QUERY before iterating over it
        # as it may be updated by other threads.
        # (only first level - lower levels are replaced
        # wholesale, so don't worry about them)
        query_metrics = METRICS_BY_QUERY.copy()
        for metric_dict in query_metrics.values():
            yield from gauge_generator(metric_dict)


def run_query(es_client, query_name, indices, query,
              timeout, on_error, on_missing):

    try:
        response = es_client.search(index=indices, body=query, request_timeout=timeout)

        metrics = parse_response(response, [query_name])
        metric_dict = group_metrics(metrics)

    except Exception:
        log.exception('Error while querying indices %(indices)s, query %(query)s.',
                      {'indices': indices, 'query': query})

        # If this query has successfully run before, we need to handle any
        # metrics produced by that previous run.
        if query_name in METRICS_BY_QUERY:
            old_metric_dict = METRICS_BY_QUERY[query_name]

            if on_error == 'preserve':
                metric_dict = old_metric_dict

            elif on_error == 'drop':
                metric_dict = {}

            elif on_error == 'zero':
                # Merging the old metric dict with an empty one, and zeroing
                # any missing metrics, produces a metric dict with the same
                # metrics, but all zero values.
                metric_dict = merge_metric_dicts(old_metric_dict, {},
                                                 zero_missing=True)

            METRICS_BY_QUERY[query_name] = metric_dict

    else:
        # If this query has successfully run before, we need to handle any
        # missing metrics.
        if query_name in METRICS_BY_QUERY:
            old_metric_dict = METRICS_BY_QUERY[query_name]

            if on_missing == 'preserve':
                metric_dict = merge_metric_dicts(old_metric_dict, metric_dict,
                                                 zero_missing=False)

            elif on_missing == 'drop':
                pass  # use new metric dict untouched

            elif on_missing == 'zero':
                metric_dict = merge_metric_dicts(old_metric_dict, metric_dict,
                                                 zero_missing=True)

        METRICS_BY_QUERY[query_name] = metric_dict


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


def indices_stats_indices_parser(ctx, param, value):
    if value is None:
        return None

    if value in ('*', '_all', ''):
        return value
    else:
        return value.split(',')


def indices_stats_fields_parser(ctx, param, value):
    if value is None:
        return None

    if value == '*':
        return value
    else:
        return value.split(',')


def split_http_header(header_string):
    """Splits a colon-separated string into header and value"""
    parts = tuple(part.strip() for part in header_string.split(":", 1))
    if len(parts) != 2 or any(part == '' for part in parts):
        msg = "Invalid header '{}'. Use colon to separate name and value".format(header_string)
        raise click.BadParameter(msg)
    return parts


def http_headers_parser(ctx, param, headers):
    """Turn header list into a dict {header: value, ...}"""
    if headers is None:
        return None

    return dict(split_http_header(header_string) for header_string in headers)


def configparser_enum_conv(enum):
    lower_enums = tuple(e.lower() for e in enum)

    def conv(value):
        lower_value = value.lower()
        if lower_value in lower_enums:
            return lower_value
        else:
            raise ValueError('Value {} not value. Must be one of {}'.format(
                             value, ','.join(enum)))

    return conv


CONFIGPARSER_CONVERTERS = {
    'enum': configparser_enum_conv(('preserve', 'drop', 'zero'))
}


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
@click.option('--header', '-H',
              multiple=True,
              callback=http_headers_parser,
              help='HTTP header to include in requests to ElasticSearch cluster. '
                   'Header name and value should be separated by colon, e.g. '
                   '"Authorization: Bearer xxxxx". Several headers can be added '
                   'by repeating the -H parameter.')
@click.option('--port', '-p', default=9206,
              help='Port to serve the metrics endpoint on. (default: 9206)')
@click.option('--query-disable', default=False, is_flag=True,
              help='Disable query monitoring. '
                   'No config files/queries need to be present if query monitoring is disabled.')
@click.option('--config-file', '-c', default='exporter.cfg', type=click.Path(dir_okay=False),
              help='Path to the main query config file. '
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
@click.option('--threads', type=click.IntRange(min=1), default=1,
              help='Enables concurrent query execution using the number of threads specified. '
                   '(default: 1)')
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
@click.option('--indices-aliases-disable', default=False, is_flag=True,
              help='Disable indices aliases monitoring.')
@click.option('--indices-aliases-timeout', default=10.0,
              help='Request timeout for indices aliases monitoring, in seconds. (default: 10)')
@click.option('--indices-mappings-disable', default=False, is_flag=True,
              help='Disable indices mappings monitoring.')
@click.option('--indices-mappings-timeout', default=10.0,
              help='Request timeout for indices mappings monitoring, in seconds. (default: 10)')
@click.option('--indices-stats-disable', default=False, is_flag=True,
              help='Disable indices stats monitoring.')
@click.option('--indices-stats-timeout', default=10.0,
              help='Request timeout for indices stats monitoring, in seconds. (default: 10)')
@click.option('--indices-stats-mode', default='cluster',
              type=click.Choice(['cluster', 'indices']),
              help='Detail mode for indices stats monitoring. (default: cluster)')
@click.option('--indices-stats-indices',
              callback=indices_stats_indices_parser,
              help='Limit indices stats to specific indices. '
                   'Only takes effect if "--indices-stats-mode=indices". '
                   'Indices should be separated by commas e.g. index1,index2.')
@click.option('--indices-stats-metrics',
              type=MultiChoice(INDICES_STATS_METRICS_OPTIONS),
              help='Limit indices stats to specific metrics. '
                   'Metrics should be separated by commas e.g. indices,fs.')
@click.option('--indices-stats-fields',
              callback=indices_stats_fields_parser,
              help='Include fielddata info for specific fields. '
                   'Fields should be separated by commas e.g. field1,field2. '
                   'Use \'*\' for all.')
@click.option('--json-logging', '-j', default=False, is_flag=True,
              help='Turn on json logging.')
@click.option('--log-level', default='INFO',
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
              help='Detail level to log. (default: INFO)')
@click.option('--verbose', '-v', default=False, is_flag=True,
              help='Turn on verbose (DEBUG) logging. Overrides --log-level.')
@click_config_file.configuration_option()
def cli(**options):
    """Export Elasticsearch query results to Prometheus."""
    if options['basic_user'] and options['basic_password'] is None:
        raise click.BadOptionUsage('basic_user', 'Username provided with no password.')
    elif options['basic_user'] is None and options['basic_password']:
        raise click.BadOptionUsage('basic_password', 'Password provided with no username.')
    elif options['basic_user']:
        http_auth = (options['basic_user'], options['basic_password'])
    else:
        http_auth = None

    if not options['ca_certs'] and options['client_cert']:
        raise click.BadOptionUsage('client_cert',
                                   '--client-cert can only be used when --ca-certs is provided.')
    elif not options['ca_certs'] and options['client_key']:
        raise click.BadOptionUsage('client_key',
                                   '--client-key can only be used when --ca-certs is provided.')
    elif options['client_cert'] and not options['client_key']:
        raise click.BadOptionUsage('client_cert',
                                   '--client-key must be provided when --client-cert is used.')
    elif not options['client_cert'] and options['client_key']:
        raise click.BadOptionUsage('client_key',
                                   '--client-cert must be provided when --client-key is used.')

    if options['indices_stats_indices'] and options['indices_stats_mode'] != 'indices':
        raise click.BadOptionUsage('indices_stats_indices',
                                   '--indices-stats-mode must be "indices" for '
                                   '--indices-stats-indices to be used.')

    executor = None
    num_threads = options['threads']
    if num_threads > 1:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)

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
                                  headers=options['header'],
                                  http_auth=http_auth)
    else:
        es_client = Elasticsearch(es_cluster,
                                  verify_certs=False,
                                  headers=options['header'],
                                  http_auth=http_auth)

    scheduler = None

    if not options['query_disable']:
        config = configparser.ConfigParser(converters=CONFIGPARSER_CONVERTERS)
        config.read(options['config_file'])

        config_dir_file_pattern = os.path.join(options['config_dir'], '*.cfg')
        config_dir_sorted_files = sorted(glob.glob(config_dir_file_pattern))
        config.read(config_dir_sorted_files)

        query_prefix = 'query_'
        queries = {}
        for section in config.sections():
            if section.startswith(query_prefix):
                query_name = section[len(query_prefix):]
                interval = config.getfloat(section, 'QueryIntervalSecs',
                                           fallback=15)
                timeout = config.getfloat(section, 'QueryTimeoutSecs',
                                          fallback=10)
                indices = config.get(section, 'QueryIndices',
                                     fallback='_all')
                query = json.loads(config.get(section, 'QueryJson'))
                on_error = config.getenum(section, 'QueryOnError',
                                          fallback='drop')
                on_missing = config.getenum(section, 'QueryOnMissing',
                                            fallback='drop')

                queries[query_name] = (interval, timeout, indices, query,
                                       on_error, on_missing)

        scheduler = sched.scheduler()

        if queries:
            for query_name, (interval, timeout, indices, query,
                             on_error, on_missing) in queries.items():
                schedule_job(scheduler, executor, interval,
                             run_query, es_client, query_name, indices, query,
                             timeout, on_error, on_missing)
        else:
            log.error('No queries found in config file(s)')
            return

    if not options['cluster_health_disable']:
        REGISTRY.register(ClusterHealthCollector(es_client,
                                                 options['cluster_health_timeout'],
                                                 options['cluster_health_level']))

    if not options['nodes_stats_disable']:
        REGISTRY.register(NodesStatsCollector(es_client,
                                              options['nodes_stats_timeout'],
                                              metrics=options['nodes_stats_metrics']))

    if not options['indices_aliases_disable']:
        REGISTRY.register(IndicesAliasesCollector(es_client,
                                                  options['indices_aliases_timeout']))

    if not options['indices_mappings_disable']:
        REGISTRY.register(IndicesMappingsCollector(es_client,
                                                   options['indices_mappings_timeout']))

    if not options['indices_stats_disable']:
        parse_indices = options['indices_stats_mode'] == 'indices'
        REGISTRY.register(IndicesStatsCollector(es_client,
                                                options['indices_stats_timeout'],
                                                parse_indices=parse_indices,
                                                indices=options['indices_stats_indices'],
                                                metrics=options['indices_stats_metrics'],
                                                fields=options['indices_stats_fields']))

    if scheduler:
        REGISTRY.register(QueryMetricCollector())

    log.info('Starting server...')
    start_http_server(port)
    log.info('Server started on port %(port)s', {'port': port})

    if scheduler:
        scheduler.run()
    else:
        while True:
            time.sleep(5)


@log_exceptions(exit_on_exception=True)
@nice_shutdown()
def main():
    cli(auto_envvar_prefix='ES_EXPORTER')
