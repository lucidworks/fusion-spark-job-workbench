#!/bin/bash

set -x
set -e

# Find location of this script
sdir="$(dirname "${BASH_SOURCE-$0}")"

FUSION_API=http://localhost:8080/api/v1

FILE_NAME=$1
BLOB_NAME=

if [ -z ${FILE_NAME} ]; then
  echo "Pass in the shaded jar or python file as argument to the script"
  exit
fi
BLOB_NAME="${FILE_NAME##*/}"
curl -vX PUT ${FUSION_API}/blobs/${BLOB_NAME} -F "data=@${FILE_NAME}"
