#!/bin/bash
# set -e test may fail, still want echo

# pass Jepsen cli to run in docker as args

docker exec \
       -t \
       -w /jepsen/jepsen-causal-consistency \
       jepsen-control \
       bash -c "source /root/.bashrc && cd /jepsen/jepsen-causal-consistency && $*"

jepsen_exit=$?

echo
echo "The test is complete"
echo "Run the test webserver, ./electricsql-web.sh"

exit ${jepsen_exit}
