import re

from collections import OrderedDict
from prometheus_client.core import GaugeMetricFamily


METRIC_INVALID_CHARS = re.compile(r'[^a-zA-Z0-9_:]')
METRIC_INVALID_START_CHARS = re.compile(r'^[^a-zA-Z_:]')
LABEL_INVALID_CHARS = re.compile(r'[^a-zA-Z0-9_]')
LABEL_INVALID_START_CHARS = re.compile(r'^[^a-zA-Z_]')
LABEL_START_DOUBLE_UNDER = re.compile(r'^__+')


def format_label_key(label_key):
    """
    Construct a label key.

    Disallowed characters are replaced with underscores.
    """
    label_key = LABEL_INVALID_CHARS.sub('_', label_key)
    label_key = LABEL_INVALID_START_CHARS.sub('_', label_key)
    label_key = LABEL_START_DOUBLE_UNDER.sub('_', label_key)
    return label_key


def format_label_value(*values):
    """
    Construct a label value.

    If multiple value components are provided, they are joined by underscores.
    """
    return '_'.join(values)


def format_labels(label_dict):
    """
    Formats metric label dictionaries.

    Takes metric labels as a dictionary of label key -> label value.

    Label values can be list of strings. These will be joined together with
    underscores.

    Disallowed characters in label keys and values will be replaced with
    underscores.
    """
    formatted_label_dict = OrderedDict()
    for label_key, label_value in label_dict.items():
        formatted_label_key = format_label_key(label_key)

        if isinstance(label_value, str):
            formatted_label_value = format_label_value(label_value)
        else:
            formatted_label_value = format_label_value(*label_value)

        formatted_label_dict[formatted_label_key] = formatted_label_value

    return formatted_label_dict


def format_metric_name(*names):
    """
    Construct a metric name.

    If multiple name components are provided, they are joined by underscores.
    Disallowed characters are replaced with underscores.
    """
    metric = '_'.join(names)
    metric = METRIC_INVALID_CHARS.sub('_', metric)
    metric = METRIC_INVALID_START_CHARS.sub('_', metric)
    return metric


def group_metrics(metrics):
    """
    Groups metrics with the same name but different label values.

    Takes metrics as a list of tuples containing:
    * metric name,
    * metric documentation,
    * dict of label key -> label value,
    * metric value.

    The metrics are grouped by metric name. All metrics with the same metric
    name must have the same set of label keys.

    A dict keyed by metric name is returned. Each metric name maps to a tuple
    containing:
    * metric documentation
    * label keys tuple,
    * dict of label values tuple -> metric value.
    """

    metric_dict = {}
    for (metric_name, metric_doc, label_dict, value) in metrics:
        curr_label_keys = tuple(label_dict.keys())

        if metric_name in metric_dict:
            label_keys = metric_dict[metric_name][1]
            assert set(curr_label_keys) == set(label_keys), \
                'Not all values for metric {} have the same keys. {} vs. {}.'.format(
                    metric_name, curr_label_keys, label_keys)
        else:
            label_keys = curr_label_keys
            metric_dict[metric_name] = (metric_doc, label_keys, {})

        label_values = tuple([label_dict[k] for k in label_keys])

        metric_dict[metric_name][2][label_values] = value

    return metric_dict


def merge_value_dicts(old_value_dict, new_value_dict, zero_missing=False):
    """
    Merge an old and new value dict together, returning the merged value dict.

    Value dicts map from label values tuple -> metric value.

    Values from the new value dict have precidence. If any label values tuples
    from the old value dict are not present in the new value dict and
    zero_missing is set, their values are reset to zero.
    """
    value_dict = new_value_dict.copy()
    value_dict.update({
        label_values: 0 if zero_missing else old_value
        for label_values, old_value
        in old_value_dict.items()
        if label_values not in new_value_dict
    })
    return value_dict


def merge_metric_dicts(old_metric_dict, new_metric_dict, zero_missing=False):
    """
    Merge an old and new metric dict together, returning the merged metric dict.

    Metric dicts are keyed by metric name. Each metric name maps to a tuple
    containing:
    * metric documentation
    * label keys tuple,
    * dict of label values tuple -> metric value.

    Values from the new metric dict have precidence. If any metric names from
    the old metric dict are not present in the new metric dict and zero_missing
    is set, their values are reset to zero.

    Merging (and missing value zeroing, if set) is performed on the value dicts
    for each metric, not just on the top level metrics themselves.
    """
    metric_dict = new_metric_dict.copy()
    metric_dict.update({
        metric_name: (
            metric_doc,
            label_keys,
            merge_value_dicts(
                old_value_dict,
                new_value_dict=new_metric_dict[metric_name][2]
                if metric_name in new_metric_dict else {},
                zero_missing=zero_missing
            )
        )
        for metric_name, (metric_doc, label_keys, old_value_dict)
        in old_metric_dict.items()
    })
    return metric_dict


def gauge_generator(metric_dict):
    """
    Generates GaugeMetricFamily instances for a list of metrics.

    Takes metrics as a dict keyed by metric name. Each metric name maps to a
    tuple containing:
    * metric documentation
    * label keys tuple,
    * dict of label values tuple -> metric value.

    Yields a GaugeMetricFamily instance for each unique metric name, containing
    children for the various label combinations. Suitable for use in a collect()
    method of a Prometheus collector.
    """

    for metric_name, (metric_doc, label_keys, value_dict) in metric_dict.items():
        # If we have label keys we may have multiple different values,
        # each with their own label values.
        if label_keys:
            gauge = GaugeMetricFamily(metric_name, metric_doc, labels=label_keys)

            for label_values in sorted(value_dict.keys()):
                value = value_dict[label_values]
                gauge.add_metric(label_values, value)

        # No label keys, so we must have only a single value.
        else:
            gauge = GaugeMetricFamily(metric_name, metric_doc, value=list(value_dict.values())[0])

        yield gauge
