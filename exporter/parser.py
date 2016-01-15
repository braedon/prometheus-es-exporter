def parse_buckets(buckets, metric=[], labels={}):
    result = []

    for index, bucket in enumerate(buckets):
        labels_next = labels.copy()

        if 'key' in bucket.keys():
            bucket_key = bucket['key']
            if 'bucket' in labels_next.keys():
                labels_next['bucket'] = labels_next['bucket'] + [bucket_key]
            else:
                labels_next['bucket'] = [bucket_key]
            del bucket['key']
        else:
            bucket_key = 'filter_' + str(index)
            if 'bucket' in labels_next.keys():
                labels_next['bucket'] = labels_next['bucket'] + [bucket_key]
            else:
                labels_next['bucket'] = [bucket_key]

        result.extend(parse_agg(bucket, metric=metric, labels=labels_next))

    return result

def parse_buckets_fixed(buckets, metric=[], labels={}):
    result = []

    for bucket_key, bucket in buckets.items():
        labels_next = labels.copy()

        if 'bucket' in labels_next.keys():
            labels_next['bucket'] = labels_next['bucket'] + [bucket_key]
        else:
            labels_next['bucket'] = [bucket_key]

        result.extend(parse_agg(bucket, metric=metric, labels=labels_next))

    return result

def parse_agg(agg, metric=[], labels={}):
    result = []

    for key, value in agg.items():
        if key == 'buckets' and isinstance(value, list):
            result.extend(parse_buckets(value, metric=metric, labels=labels))
        elif key == 'buckets' and isinstance(value, dict):
            result.extend(parse_buckets_fixed(value, metric=metric, labels=labels))
        elif isinstance(value, dict):
            result.extend(parse_agg(value, metric=metric + [key], labels=labels))
        else:
            result.append((metric + [key], labels, value))

    return result

def parse_response(response, metric=[]):
    result = []

    if not response['timed_out']:
        result.append((metric + ['hits'], {}, response['hits']['total']))

        if 'aggregations' in response.keys():
            for key, value in response['aggregations'].items():
                result.extend(parse_agg(value, metric=metric + [key]))

    return result
