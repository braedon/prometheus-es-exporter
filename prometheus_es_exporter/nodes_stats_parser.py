from collections import OrderedDict

from .metrics import format_metric_name, format_labels
from .utils import merge_dicts_ordered

singular_forms = {
    'pools': 'pool',
    'collectors': 'collector',
    'buffer_pools': 'buffer_pool',
}
excluded_keys = [
    'timestamp',
]
bucket_dict_keys = [
    'pools',
    'collectors',
    'buffer_pools',
    'thread_pool',
]
bucket_list_keys = {
    'data': 'path',
    'devices': 'device_name'
}


def parse_block(block, metric=None, labels=None):
    if metric is None:
        metric = []
    if labels is None:
        labels = OrderedDict()

    metrics = []

    for key, value in block.items():
        if key not in excluded_keys:
            if isinstance(value, bool):
                metrics.append((metric + [key], '', labels, int(value)))
            elif isinstance(value, (int, float)):
                metrics.append((metric + [key], '', labels, value))
            elif isinstance(value, dict):
                if key in bucket_dict_keys:
                    if key in singular_forms:
                        singular_key = singular_forms[key]
                    else:
                        singular_key = key
                    for n_key, n_value in value.items():
                        metrics.extend(parse_block(n_value, metric=metric + [key], labels=merge_dicts_ordered(labels, {singular_key: [n_key]})))
                else:
                    metrics.extend(parse_block(value, metric=metric + [key], labels=labels))
            elif isinstance(value, list) and key in bucket_list_keys:
                bucket_name_key = bucket_list_keys[key]

                for n, n_value in enumerate(value):
                    if bucket_name_key in n_value:
                        bucket_name = n_value[bucket_name_key]
                    else:
                        # If the expected bucket name key isn't present, fall back to using the
                        # bucket's position in the list as the bucket name. It's not guaranteed that
                        # the buckets will remain in the same order between calls, but it's the best
                        # option available.
                        # e.g. For AWS managed Elasticsearch instances, the `path` key is missing
                        #      from the filesystem `data` directory buckets.
                        bucket_name = str(n)
                    metrics.extend(parse_block(n_value, metric=metric + [key], labels=merge_dicts_ordered(labels, {bucket_name_key: [bucket_name]})))

    return metrics


def parse_node(node, metric=None, labels=None):
    if metric is None:
        metric = []
    if labels is None:
        labels = OrderedDict()

    labels = merge_dicts_ordered(labels, node_name=[node['name']])

    return parse_block(node, metric=metric, labels=labels)


def parse_response(response, metric=None):
    if metric is None:
        metric = []

    metrics = []

    if '_nodes' not in response or not response['_nodes']['failed']:
        for key, value in response['nodes'].items():
            metrics.extend(parse_node(value, metric=metric, labels=OrderedDict({'node_id': [key]})))

    return [
        (format_metric_name(*metric_name),
         metric_doc,
         format_labels(label_dict),
         value)
        for metric_name, metric_doc, label_dict, value
        in metrics
    ]
