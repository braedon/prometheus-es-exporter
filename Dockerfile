FROM python:3-slim

WORKDIR /usr/src/app

COPY exporter/*.py /usr/src/app/exporter/
COPY setup.py /usr/src/app/
COPY LICENSE /usr/src/app/

RUN pip install -e .

CMD ["python", "-u", "/usr/local/bin/prometheus-es-exporter"]
