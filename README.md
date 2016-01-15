Prometheus Elasticsearch Exporter
====
This Prometheus exporter periodically runs configured queries against an Elasticsearch cluster and exports the results as Prometheus gauge metrics.
Values are parsed out of the Elasticsearch results automatically, with the path through the JSON to the value being used to construct metric names.
Metrics are only extracted from aggregation results, with the exception of the query doc count. The keys of any buckets are converted to labels, rather than being inserted into the metric name.
See `test_parser.py` for all the supported queries/metrics.

You will need Python 3 installed, along with some dependencies:
```
> sudo pip3 install elasticsearch prometheus_client
```

Start the exporter by running `exporter.py`:
```
> ./exporter.py
```

Configuration is provided by an `exporter.cfg` file in your working directory. See the provided file for configuration examples and explanation.
