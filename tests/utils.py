from prometheus_es_exporter import (format_metric_name,
                                    format_label_key,
                                    format_label_value)


def format_label(key, value_list):
    return format_label_key(key) + '="' + format_label_value(value_list) + '"'


def format_metric(name_list, label_dict):
    name = format_metric_name(name_list)

    if len(label_dict) > 0:
        sorted_keys = sorted(label_dict.keys())
        labels = '{'
        labels += ','.join([format_label(k, label_dict[k]) for k in sorted_keys])
        labels += '}'
    else:
        labels = ''

    return name + labels


# Converts the parse_response() result into a psuedo-prometheus format
# that is useful for comparing results in tests.
def convert_result(result):
    return {
        format_metric(name_list, label_dict): value
        for (name_list, label_dict, value) in result
    }
