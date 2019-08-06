from collections import OrderedDict


def parse_buckets(agg_key, buckets, metric=None, labels=None):
    if metric is None:
        metric = []
    if labels is None:
        labels = OrderedDict()

    result = []

    for index, bucket in enumerate(buckets):
        labels_next = labels.copy()

        if 'key' in bucket.keys():
            bucket_key = str(bucket['key'])
            if agg_key in labels_next.keys():
                labels_next[agg_key] = labels_next[agg_key] + [bucket_key]
            else:
                labels_next[agg_key] = [bucket_key]
            del bucket['key']
        else:
            bucket_key = 'filter_' + str(index)
            if agg_key in labels_next.keys():
                labels_next[agg_key] = labels_next[agg_key] + [bucket_key]
            else:
                labels_next[agg_key] = [bucket_key]

        result.extend(parse_agg(bucket_key, bucket, metric=metric, labels=labels_next))

    return result


def parse_buckets_fixed(agg_key, buckets, metric=None, labels=None):
    if metric is None:
        metric = []
    if labels is None:
        labels = OrderedDict()

    result = []

    for bucket_key, bucket in buckets.items():
        labels_next = labels.copy()

        if agg_key in labels_next.keys():
            labels_next[agg_key] = labels_next[agg_key] + [bucket_key]
        else:
            labels_next[agg_key] = [bucket_key]

        result.extend(parse_agg(bucket_key, bucket, metric=metric, labels=labels_next))

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
        elif isinstance(value, dict):
            result.extend(parse_agg(key, value, metric=metric + [key], labels=labels))
        # We only want numbers as metrics.
        # Anything else (with the exception of sub-objects,
        # which are handled above) is ignored.
        elif isinstance(value, (int, float)):
            result.append((metric + [key], labels, value))

    return result


def parse_response(response, metric=None):
    if metric is None:
        metric = []

    result = []

    if not response['timed_out']:
        total = response['hits']['total']
        # In ES7, hits.total changed from an integer to
        # a dict with a 'value' key.
        if isinstance(total, dict):
            total = total['value']
        result.append((metric + ['hits'], {}, total))
        result.append((metric + ['took', 'milliseconds'], {}, response['took']))

        if 'aggregations' in response.keys():
            for key, value in response['aggregations'].items():
                result.extend(parse_agg(key, value, metric=metric + [key]))

    return result
