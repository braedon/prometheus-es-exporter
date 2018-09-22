from prometheus_es_exporter import group_metrics


def format_label(key, value):
    return key + '="' + value + '"'


def format_metrics(metric_name, label_keys, value_dict):
    metrics = {}

    for label_values, value in value_dict.items():
        if len(label_keys) > 0:
            # sorted_keys = sorted(label_keys)
            labels = '{'
            labels += ','.join([format_label(label_keys[i], label_values[i])
                                for i in range(len(label_keys))])
            labels += '}'
        else:
            labels = ''

        metrics[metric_name + labels] = value

    return metrics


# Converts the parse_response() result into a psuedo-prometheus format
# that is useful for comparing results in tests.
# Uses the 'group_metrics()' function used by the exporter, so effectively
# tests that function.
def convert_result(result):
    metric_dict = group_metrics(result)
    return {
        metric: value
        for metric_name, (label_keys, value_dict) in metric_dict.items()
        for metric, value in format_metrics(metric_name, label_keys, value_dict).items()
    }
