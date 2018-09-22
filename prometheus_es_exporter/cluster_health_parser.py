from collections import OrderedDict
from .utils import merge_dicts_ordered

singular_forms = {
    'indices': 'index',
    'shards': 'shard'
}


def parse_block(block, metric=None, labels=None):
    if metric is None:
        metric = []
    if labels is None:
        labels = OrderedDict()

    result = []

    # Green is 0, so if we add statuses of mutiple blocks together
    # (e.g. all the indices) we don't need to know how many there were
    # to know if things are good.
    # i.e. 0 means all green, > 0 means something isn't green.
    status = block['status']
    if status == 'green':
        status_int = 0
    elif status == 'yellow':
        status_int = 1
    elif status == 'red':
        status_int = 2
    result.append((metric + ['status'], labels, status_int))

    for key, value in block.items():
        if isinstance(value, bool):
            result.append((metric + [key], labels, int(value)))
        elif isinstance(value, (int, float)):
            result.append((metric + [key], labels, value))
        elif isinstance(value, dict):
            if key in singular_forms:
                singular_key = singular_forms[key]
            else:
                singular_key = key
            for n_key, n_value in value.items():
                result.extend(parse_block(n_value, metric=metric + [key], labels=merge_dicts_ordered(labels, {singular_key: [n_key]})))

    return result


def parse_response(response, metric=None):
    if metric is None:
        metric = []

    result = []

    # Create a shallow copy as we are going to modify it
    response = response.copy()

    if not response['timed_out']:
        # Delete this field as we don't want to parse it as metric
        del response['timed_out']

        result.extend(parse_block(response, metric=metric))

    return result
