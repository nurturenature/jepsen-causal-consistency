#!/bin/bash
set -e

# FROM Jepsen images to ElectricSQL images
docker build \
       -t electricsql-node \
       --build-arg JEPSEN_REGISTRY="ghcr.io/nurturenature/jepsen-docker/" \
       --no-cache-filter electricsql-node \
       ./electricsql-node

docker build \
       -t electricsql-control \
       --build-arg JEPSEN_REGISTRY="ghcr.io/nurturenature/jepsen-docker/" \
       --no-cache-filter electricsql-control \
       ./electricsql-control

echo
echo "Jepsen + ElectricSQL Docker images have been built."
echo "Bring up a Jepsen + ElectricSQL cluster with ./electricsql-up.sh" 