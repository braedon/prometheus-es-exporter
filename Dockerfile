FROM remote-docker.artifactory.swisscom.com/python:3.8-slim

RUN pip install --upgrade pip

RUN adduser worker
USER worker
WORKDIR /home/worker

RUN pip install --user pipenv
ENV PATH="/home/worker/.local/bin:${PATH}"

COPY --chown=worker:worker setup.py /home/worker
COPY --chown=worker:worker README.md /home/worker
# Elasticsearch switched to a non open source license from version 7.11 onwards.
# Limit to earlier versions to avoid license and compatibility issues.
RUN pip install --user -e . 'elasticsearch<7.11'
RUN pip install --user -e . 'wsgi_basic_auth'

COPY --chown=worker:worker prometheus_es_exporter/*.py /home/worker/prometheus_es_exporter/
COPY --chown=worker:worker LICENSE /home/worker

EXPOSE 9206

ENTRYPOINT ["python", "-u", "/home/worker/prometheus_es_exporter"]
