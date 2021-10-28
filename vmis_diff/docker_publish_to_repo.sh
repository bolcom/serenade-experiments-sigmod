#!/bin/bash
# run 'gcloud auth configure-docker' if unauthorized error message
PROJECT="my-google-cloud-project"

DOCKER_IMAGE_NAME='serenade-dd:master'
docker build -t eu.gcr.io/${PROJECT}/${DOCKER_IMAGE_NAME} -f Dockerfile-dd .
docker push eu.gcr.io/${PROJECT}/${DOCKER_IMAGE_NAME}