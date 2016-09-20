#!/bin/bash
set -e
ORG=hsldevcom
DOCKER_IMAGE=roisto

rm -Rf source-for-build
mkdir source-for-build
git ls-files -z | xargs -0 cp --parents -t source-for-build

docker build --tag="$ORG/$DOCKER_IMAGE" .
