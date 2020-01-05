from collections import OrderedDict

from .metrics import format_metric_name, format_labels
from .utils import merge_dicts_ordered

singular_forms = {
    'fields': 'field'
}
excluded_keys = []
bucket_dict_keys = [
    'fields'
]
bucket_list_keys = {}


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

                for n_value in value:
                    bucket_name = n_value[bucket_name_key]
                    metrics.extend(parse_block(n_value, metric=metric + [key], labels=merge_dicts_ordered(labels, {bucket_name_key: [bucket_name]})))

    return metrics


def parse_response(response, parse_indices=False, metric=None):
    if metric is None:
        metric = []

    metrics = []

    if '_shards' not in response or not response['_shards']['failed']:
        if parse_indices:
            for key, value in response['indices'].items():
                metrics.extend(parse_block(value, metric=metric, labels=OrderedDict({'index': [key]})))
        else:
            metrics.extend(parse_block(response['_all'], metric=metric, labels=OrderedDict({'index': ['_all']})))

    return [
        (format_metric_name(*metric_name),
         metric_doc,
         format_labels(label_dict),
         value)
        for metric_name, metric_doc, label_dict, value
        in metrics
    ]
