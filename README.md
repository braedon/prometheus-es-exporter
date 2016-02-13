Prometheus Elasticsearch Exporter
====
This Prometheus exporter periodically runs configured queries against an Elasticsearch cluster and exports the results as Prometheus gauge metrics.

Values are parsed out of the Elasticsearch results automatically, with the path through the JSON to the value being used to construct metric names.

Metrics are only extracted from aggregation results, with the exception of the query doc count. The keys of any buckets are converted to labels, rather than being inserted into the metric name. See `tests/test_parser.py` for all the supported queries/metrics.

# Installation
You will need Python 3 and pip 3 installed to run the exporter.

Run the following in the root project directory to install (i.e. download dependencies, create start script):
```
> pip3 install .
```
Note that you may need to add the start script location (see pip output) to your `PATH`.

# Usage
Once installed, you can run the exporter with the `prometheus-es-exporter` command.

By default, it will bind to port 8080, query Elasticsearch on `localhost:9200` and run queries configured in a file `exporter.cfg` in the working directory. You can change these defaults as required by passing in options:
```
> prometheus-es-exporter -p <port> -e <elasticsearch nodes> -c <path to query config file>
```
Run with the `-h` flag to see details on all the available options.

See the provided `exporter.cfg` file for query configuration examples and explanation.

# Docker
You can build a docker image using the provided Dockerfile. Run the following in the root project directory:
```
> sudo docker build -t prometheus-es-exporter .
```
To run a container successfully, you will need to mount a query config file to `/usr/src/app/exporter.cfg` and map container port 8080 to a port on the host. Any options placed after the image name (`prometheus-es-exporter`) will be passed to the process inside the container. You will also need to use this to configure the elasticsearch node(s) using `-e`.
```
> sudo docker run --rm --name exporter \
    -v <path to query config file>:/usr/src/app/exporter.cfg \
    -p 8080:8080 \
    prometheus-es-exporter -e <elasticsearch nodes>
```
You can change other options in the same way as `-e`. For example, you could change where the query config file is read from using `-c`.

If you don't want to mount the query config file in at run time, you could modify the Dockerfile to copy it in when building the image.

# Development
The exporter can be installed in "editable" mode, using pip's `-e` flag. This allows you to test out changes without having to re-install.
```
> pip3 install -e .
```
To run tests (as usual, from the root project directory), use:
```
> python3 -m tests.test_parser
```
Note that these tests currently only cover the response parsing functionality - there are no automated system tests as of yet.

Send me a PR if you have a change you want to contribute!
