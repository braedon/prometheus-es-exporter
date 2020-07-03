FROM python:3.8-slim

WORKDIR /usr/src/app

COPY setup.py /usr/src/app/
COPY README.md /usr/src/app/
RUN pip install -e .

COPY prometheus_es_exporter/*.py /usr/src/app/prometheus_es_exporter/
COPY LICENSE /usr/src/app/

EXPOSE 9206

ENTRYPOINT ["python", "-u", "/usr/local/bin/prometheus-es-exporter"]
