FROM python:3-slim

WORKDIR /usr/src/app

COPY prometheus_es_exporter/*.py /usr/src/app/prometheus_es_exporter/
COPY setup.py /usr/src/app/
COPY LICENSE /usr/src/app/
COPY README.md /usr/src/app/
COPY MANIFEST.in /usr/src/app/

RUN pip install -e .

EXPOSE 8080

ENTRYPOINT ["python", "-u", "/usr/local/bin/prometheus-es-exporter"]
