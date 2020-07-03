from collections import OrderedDict

from .metrics import format_metric_name, format_labels
from .utils import merge_dicts_ordered


def parse_index(index, aliases, metric=None):
    if metric is None:
        metric = []

    metric = metric + ['alias']
    labels = OrderedDict([('index', index)])

    metrics = []
    for alias in aliases.keys():
        metrics.append((metric, '', merge_dicts_ordered(labels, alias=alias), 1))

    return metrics


def parse_response(response, metric=None):
    if metric is None:
        metric = []

    metrics = []

    for index, data in response.items():
        metrics.extend(parse_index(index, data['aliases'], metric=metric))

    return [
        (format_metric_name(*metric_name),
         metric_doc,
         format_labels(label_dict),
         value)
        for metric_name, metric_doc, label_dict, value
        in metrics
    ]
