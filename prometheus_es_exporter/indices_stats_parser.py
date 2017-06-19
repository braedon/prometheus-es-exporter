singular_forms = {
    'fields': 'field'
}
excluded_keys = []
bucket_dict_keys = [
    'fields'
]
bucket_list_keys = {}


def parse_block(block, metric=[], labels={}):
    result = []

    for key, value in block.items():
        if key not in excluded_keys:
            if isinstance(value, bool):
                result.append((metric + [key], labels, int(value)))
            elif isinstance(value, (int, float)):
                result.append((metric + [key], labels, value))
            elif isinstance(value, dict):
                if key in bucket_dict_keys:
                    if key in singular_forms:
                        singular_key = singular_forms[key]
                    else:
                        singular_key = key
                    for n_key, n_value in value.items():
                        result.extend(parse_block(n_value, metric=metric + [key], labels={**labels, singular_key: [n_key]}))
                else:
                    result.extend(parse_block(value, metric=metric + [key], labels=labels))
            elif isinstance(value, list) and key in bucket_list_keys:
                bucket_name_key = bucket_list_keys[key]

                for n_value in value:
                    bucket_name = n_value[bucket_name_key]
                    result.extend(parse_block(n_value, metric=metric + [key], labels={**labels, bucket_name_key: [bucket_name]}))

    return result


def parse_response(response, parse_indices=False, metric=[]):
    result = []

    if '_shards' not in response or not response['_shards']['failed']:
        if parse_indices:
            for key, value in response['indices'].items():
                result.extend(parse_block(value, metric=metric, labels={'index': [key]}))
        else:
            result.extend(parse_block(response['_all'], metric=metric, labels={'index': ['_all']}))

    return result
