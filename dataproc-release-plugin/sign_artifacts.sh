#!/usr/bin/bash
# set -ex

if [ -z "${DATAPROC_SIGNING_COMMAND}" ]; then
  echo Error: DATAPROC_SIGNING_COMMAND environment variable needs to be set
  exit 1
fi

targets=$(find target -name "*.jar" -o -name "*.pom" -o -name "*.zip")

for target in ${targets}; do
  echo Signing $target
  ${DATAPROC_SIGNING_COMMAND} $(pwd)/${target}
done
