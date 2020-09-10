#!/bin/bash
GIT_HASH=$(git rev-parse HEAD)
TAG="${GIT_HASH:0:4}"
echo "TAG=${TAG}"
export TAG="${TAG}"

sudo docker build -t tbcasoft/es-prometheus-exporter:${TAG} -f ./Dockerfile .