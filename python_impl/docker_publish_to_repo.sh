#!/bin/bash
# run 'gcloud auth configure-docker' if unauthorized error message
PROJECT="private-my-google-project"

DOCKER_IMAGE_NAME='serenade-python:master'
docker build -t eu.gcr.io/${PROJECT}/${DOCKER_IMAGE_NAME} -f Dockerfile-py .
docker push eu.gcr.io/${PROJECT}/${DOCKER_IMAGE_NAME}

DOCKER_IMAGE_NAME='serenade-sql:master'
docker build -t eu.gcr.io/${PROJECT}/${DOCKER_IMAGE_NAME} -f Dockerfile-sql .
docker push eu.gcr.io/${PROJECT}/${DOCKER_IMAGE_NAME}
