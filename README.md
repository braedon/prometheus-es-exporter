Prometheus Elasticsearch Exporter
====
This Prometheus exporter periodically runs configured queries against an Elasticsearch cluster and exports the results as Prometheus gauge metrics.

Values are parsed out of the Elasticsearch results automatically, with the path through the JSON to the value being used to construct metric names.

Metrics are only extracted from aggregation results, with the exception of the query doc count. The keys of any buckets are converted to labels, rather than being inserted into the metric name. See [tests/test_parser.py](tests/test_parser.py) for all the supported queries/metrics.

# Installation
The exporter requires Python 3 and Pip 3 to be installed.

To install the latest published version via Pip, run:
```
> pip3 install prometheus-es-exporter
```
Note that you may need to add the start script location (see pip output) to your `PATH`.

# Usage
Once installed, you can run the exporter with the `prometheus-es-exporter` command.

By default, it will bind to port 8080, query Elasticsearch on `localhost:9200` and run queries configured in a file `exporter.cfg` in the working directory. You can change these defaults as required by passing in options:
```
> prometheus-es-exporter -p <port> -e <elasticsearch nodes> -c <path to query config file>
```
Run with the `-h` flag to see details on all the available options.

See the provided [exporter.cfg](exporter.cfg) file for query configuration examples and explanation.

# Docker
Docker images for released versions can be found on Docker Hub (note that no `latest` version is provided):
```
> sudo docker pull braedon/prometheus-es-exporter:<version>
```
To run a container successfully, you will need to mount a query config file to `/usr/src/app/exporter.cfg` and map container port 8080 to a port on the host. Any options placed after the image name (`prometheus-es-exporter`) will be passed to the process inside the container. For example, you will need to use this to configure the elasticsearch node(s) using `-e`.
```
> sudo docker run --rm --name exporter \
    -v <path to query config file>:/usr/src/app/exporter.cfg \
    -p <host port>:8080 \
    braedon/prometheus-es-exporter:<version> -e <elasticsearch nodes>
```
If you don't want to mount the query config file in at run time, you could extend an existing image with your own Dockerfile that copies the config file in at build time.

# Development
To install directly from the git repo, run the following in the root project directory:
```
> pip3 install .
```
The exporter can be installed in "editable" mode, using pip's `-e` flag. This allows you to test out changes without having to re-install.
```
> pip3 install -e .
```
To run tests (as usual, from the root project directory), use:
```
> python3 -m tests.test_parser
```
Note that these tests currently only cover the response parsing functionality - there are no automated system tests as of yet.

To build a docker image directly from the git repo, run the following in the root project directory:
```
> sudo docker build -t <your repository name and tag> .
```
Send me a PR if you have a change you want to contribute!
