#!/bin/bash
set -e

docker exec -t \
       -w /jepsen/jepsen-causal-consistency \
       jepsen-control \
       lein run serve
 