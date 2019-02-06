# Pin to 3.6, as prometheus-client has a memory leak in 3.7
# https://github.com/prometheus/client_python/issues/340
# TODO: unpin when patched prometheus-client version is released
FROM python:3.6-slim

WORKDIR /usr/src/app

COPY setup.py /usr/src/app/
RUN pip install .

COPY prometheus_es_exporter/*.py /usr/src/app/prometheus_es_exporter/
RUN pip install -e .

COPY LICENSE /usr/src/app/
COPY README.md /usr/src/app/

EXPOSE 9206

ENTRYPOINT ["python", "-u", "/usr/local/bin/prometheus-es-exporter"]
