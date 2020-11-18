from collections import OrderedDict

from .metrics import format_metric_name, format_labels


def add_label(label_key, label_value, labels):
    labels = labels.copy()

    if label_key in labels.keys():
        labels[label_key] = labels[label_key] + [label_value]
    else:
        labels[label_key] = [label_value]

    return labels


def parse_buckets(agg_key, buckets, metric=None, labels=None):
    if metric is None:
        metric = []
    if labels is None:
        labels = OrderedDict()

    result = []

    for index, bucket in enumerate(buckets):
        labels_nest = labels.copy()

        if 'key' in bucket.keys():
            # Keys for composite aggregation buckets are dicts with multiple key/value pairs.
            if isinstance(bucket['key'], dict):
                for comp_key, comp_value in bucket['key'].items():
                    label_key = '_'.join([agg_key, comp_key])
                    labels_nest = add_label(label_key, str(comp_value), labels_nest)

            else:
                labels_nest = add_label(agg_key, str(bucket['key']), labels_nest)

            # Delete the key so it isn't parsed for metrics.
            del bucket['key']

        else:
            bucket_key = 'filter_' + str(index)
            labels_nest = add_label(agg_key, bucket_key, labels_nest)

        result.extend(parse_agg(agg_key, bucket, metric=metric, labels=labels_nest))

    return result


def parse_buckets_fixed(agg_key, buckets, metric=None, labels=None):
    if metric is None:
        metric = []
    if labels is None:
        labels = OrderedDict()

    result = []

    for bucket_key, bucket in buckets.items():
        labels_nest = labels.copy()
        labels_next = add_label(agg_key, bucket_key, labels_nest)
        result.extend(parse_agg(agg_key, bucket, metric=metric, labels=labels_next))

    return result


def parse_agg(agg_key, agg, metric=None, labels=None):
    if metric is None:
        metric = []
    if labels is None:
        labels = OrderedDict()

    result = []

    for key, value in agg.items():
        if key == 'buckets' and isinstance(value, list):
            result.extend(parse_buckets(agg_key, value, metric=metric, labels=labels))
        elif key == 'buckets' and isinstance(value, dict):
            result.extend(parse_buckets_fixed(agg_key, value, metric=metric, labels=labels))
        elif key == 'after_key' and 'buckets' in agg:
            # `after_key` is used for paging composite aggregations - don't parse for metrics.
            # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html#_pagination
            continue
        elif isinstance(value, dict):
            result.extend(parse_agg(key, value, metric=metric + [key], labels=labels))
        # We only want numbers as metrics.
        # Anything else (with the exception of sub-objects,
        # which are handled above) is ignored.
        elif isinstance(value, (int, float)):
            result.append((metric + [key], '', labels, value))

    return result


def parse_response(response, metric=None):
    if metric is None:
        metric = []

    metrics = []

    if not response['timed_out']:
        total = response['hits']['total']
        # In ES7, hits.total changed from an integer to
        # a dict with a 'value' key.
        if isinstance(total, dict):
            total = total['value']
        metrics.append((metric + ['hits'], '', {}, total))
        metrics.append((metric + ['took', 'milliseconds'], '', {}, response['took']))

        if 'aggregations' in response.keys():
            for key, value in response['aggregations'].items():
                metrics.extend(parse_agg(key, value, metric=metric + [key]))

    return [
        (format_metric_name(*metric_name),
         metric_doc,
         format_labels(label_dict),
         value)
        for metric_name, metric_doc, label_dict, value
        in metrics
    ]
