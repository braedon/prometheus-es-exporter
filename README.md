Prometheus Elasticsearch Exporter
====
This Prometheus exporter collects metrics from queries run on an Elasticsearch cluster's data, and metrics about the cluster itself.

[Source Code](https://github.com/braedon/prometheus-es-exporter) | [Python Package](https://pypi.org/project/prometheus-es-exporter) | [Docker Image](https://hub.docker.com/r/braedon/prometheus-es-exporter) | [Helm Chart](https://braedon.github.io/helm/prometheus-es-exporter)

## Query Metrics
The exporter periodically runs configured queries against the Elasticsearch cluster and exports the results as Prometheus gauge metrics.

Values are parsed out of the Elasticsearch results automatically, with the path through the JSON to the value being used to construct metric names.

Metrics are only extracted from aggregation results, with the exception of the query `hits.total` count (exposed as `hits`) and `took` time (exposed as `took_milliseconds`). The keys of any buckets are converted to labels, rather than being inserted into the metric name.

### Supported Aggregations
A limited set of aggregations are explicitly supported with tests. See [tests/test_parser.py](tests/test_parser.py) for example queries using these aggregations, and the metrics they produce. Most other aggregations should also work, so long as their result format is similar in structure to one of the explicitly supported aggregations.

If you would like to use a particular aggregation but it is not working correctly (and it isn't explicitly unsupported), please raise an issue or PR.

### Unsupported Aggregations
Some aggregations are explicitly unsupported - they don't work correctly, and this can't/won't be fixed for some reason.

#### `top_hits`
The `top_hits` aggregation returns documents, not metrics about documents. Extracting metrics from arbitrary documents is out of scope for this exporter due to the complexities involved.

## Cluster Metrics
The exporter queries the Elasticsearch cluster's `_cluster/health`, `_nodes/stats`, and `_stats` endpoints whenever its metrics endpoint is called, and exports the results as Prometheus gauge metrics.

Endpoint responses are parsed into metrics as generically as possible so that (hopefully) all versions of Elasticsearch (past and future) can be reasonably supported with the same code. This results in less than ideal metrics in some cases - e.g. redundancy between some metrics, no distinction between gauges and counters (everything's a gauge). If you spot something you think can be reasonably improved let me know via a Github issue (or better yet - a PR).

See [tests/test_cluster_health_parser.py](tests/test_cluster_health_parser.py), [tests/test_nodes_stats_parser.py](tests/test_nodes_stats_parser.py), and [tests/test_indices_stats_parser.py](tests/test_indices_stats_parser.py) for examples of responses and the metrics produced.

The exporter also produces the following metrics:

### `es_indices_aliases_alias{index, alias}` (gauge)
Index aliases and the indices they point to. Parsed from the `_alias` endpoint.

Metrics for current aliases have the value `1`. If an alias is removed, or changes the index it points too, old metrics will be dropped immediately.

Note that Prometheus may [keep returning dropped metrics for a short period](https://prometheus.io/docs/prometheus/latest/querying/basics/#staleness) (usually 5 minutes). In the future, old metrics may return `0` for a period before being dropped to mitigate this issue. As such, it's best to check the metric value, not just its existence.

Filter other index metrics by alias using the `and` operator and this metric, e.g.:
```
es_indices_mappings_field_count and on(index) (es_indices_aliases_alias{alias="logstash-today"} == 1)
```

### `es_indices_mappings_field_count{index, field_type}` (gauge)
The number of mapped fields of a given type in an index. Parsed from the `_mappings` endpoint.

Sum by index to calculate the total fields per index. Useful for checking if an index is at risk of reaching a field limit, e.g.
```
sum(es_indices_mappings_field_count) by(index) > 900
```
Note that these counts don't include system fields (ones prefixed with `_`, e.g. `_id`), so may be slightly lower than the field count used by Elasticsearch to check the field limit.

# Installation
The exporter requires Python 3 and Pip 3 to be installed.

To install the latest published version via Pip, run:
```bash
> pip3 install prometheus-es-exporter
```
Note that you may need to add the start script location (see pip output) to your `PATH`.

# Usage
Once installed, you can run the exporter with the `prometheus-es-exporter` command.

By default, it will bind to port 9206, query Elasticsearch on `localhost:9200` and run queries configured in a file `exporter.cfg` in the working directory. You can change these defaults as required by passing in options:
```bash
> prometheus-es-exporter -p <port> -e <elasticsearch nodes> -c <path to query config file>
```
Run with the `-h` flag to see details on all the available options.

Note that all options can be set via environment variables. The environment variable names are prefixed with `ES_EXPORTER`, e.g. `ES_EXPORTER_BASIC_USER=fred` is equivalent to `--basic-user fred`. CLI options take precedence over environment variables.

Command line options can also be set from a configuration file, by passing `--config FILE`. The format of the file should be [Configobj's unrepre mode](https://configobj.readthedocs.io/en/latest/configobj.html#unrepr-mode), so instead of `--basic-user fred` you could use a configuration file `config_file` with `basic-user="fred"` in it, and pass `--config config_file`. CLI options and environment variables take precedence over configuration files.

CLI options, environment variables, and configuration files all override any default options. The full resolution order for a given option is: CLI > Environment > Configuration file > Default.

See the provided [exporter.cfg](exporter.cfg) file for query configuration examples and explanation.

# Docker
Docker images for released versions can be found on Docker Hub (note that no `latest` version is provided):
```bash
> sudo docker pull braedon/prometheus-es-exporter:<version>
```
To run a container successfully, you will need to mount a query config file to `/usr/src/app/exporter.cfg` and map container port 9206 to a port on the host. Any options placed after the image name (`prometheus-es-exporter`) will be passed to the process inside the container. For example, you will need to use this to configure the elasticsearch node(s) using `-e`.
```bash
> sudo docker run --rm --name exporter \
    -v <path to query config file>:/usr/src/app/exporter.cfg \
    -p <host port>:9206 \
    braedon/prometheus-es-exporter:<version> -e <elasticsearch nodes>
```
If you don't want to mount the query config file in at run time, you could extend an existing image with your own Dockerfile that copies the config file in at build time.

# Helm
A Helm chart is available from the Helm repo at [https://braedon.github.io/helm](https://braedon.github.io/helm/).
```bash
> helm repo add braedon https://braedon.github.io/helm
> helm repo update

> helm install braedon/prometheus-es-exporter --name <release name> \
                                              --set elasticsearch.cluster=<elasticsearch nodes> \
                                              --set image.tag=<image tag>
```
See the [`prometheus-es-exporter` chart README](https://braedon.github.io/helm/prometheus-es-exporter/) for more details on how to configure the chart.

# Development
To install directly from the git repo, run the following in the root project directory:
```bash
> pip3 install .
```
The exporter can be installed in "editable" mode, using pip's `-e` flag. This allows you to test out changes without having to re-install.
```bash
> pip3 install -e .
```
To run tests (as usual, from the root project directory), use:
```bash
> python3 -m unittest
```
Note that these tests currently only cover the response parsing functionality - there are no automated system tests as of yet.

To build a docker image directly from the git repo, run the following in the root project directory:
```bash
> sudo docker build -t <your repository name and tag> .
```

To develop in a docker container, first build the image, and then run the following in the root project directory:
```bash
> sudo docker run --rm -it --name exporter --entrypoint bash -v $(pwd):/usr/src/app <your repository name and tag>
```
This will mount all the files inside the container, so editing tests or application code will be synced live. You can run the tests with `python -m unittest`. You may need to run `pip install -e .` again after running the container if you get an error like
```
pkg_resources.DistributionNotFound: The 'prometheus-es-exporter' distribution was not found and is required by the application
```

Send me a PR if you have a change you want to contribute!
