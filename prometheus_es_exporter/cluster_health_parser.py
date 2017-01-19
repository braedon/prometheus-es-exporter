singular_forms = {
    'indices': 'index',
    'shards': 'shard'
}


def parse_block(block, metric=[], labels={}):
    result = []

    block = block.copy()

    # Green is 0, so if we add statuses of mutiple blocks together
    # (e.g. all the indices) we don't need to know how many there were
    # to know if things are good.
    # i.e. 0 means all green, > 0 means something isn't green.
    status = block['status']
    if status == 'green':
        status_int = 0
    if status == 'yellow':
        status_int = 1
    elif status == 'green':
        status_int = 2
    result.append((metric + ['status'], labels, status_int))
    del block['status']

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
                result.extend(parse_block(n_value, metric=metric + [key], labels={**labels, singular_key: [n_key]}))

    return result


def parse_response(response, metric=[]):
    result = []

    data = response.copy()

    if not data['timed_out']:
        del data['timed_out']

        labels = {'cluster_name': [data['cluster_name']]}
        del data['cluster_name']

        result.extend(parse_block(data, metric=metric, labels=labels))

    return result
