FROM python:3.8-slim

WORKDIR /usr/src/app

COPY setup.py /usr/src/app/
COPY README.md /usr/src/app/
# Elasticsearch switched to a non open source license from version 7.11 onwards.
# Limit to earlier versions to avoid license and compatibility issues.
RUN pip install -e . 'elasticsearch<7.11'

COPY prometheus_es_exporter/*.py /usr/src/app/prometheus_es_exporter/
COPY LICENSE /usr/src/app/

EXPOSE 9206

ENTRYPOINT ["python", "-u", "/usr/local/bin/prometheus-es-exporter"]
