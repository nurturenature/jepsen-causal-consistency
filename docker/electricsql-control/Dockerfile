#
# Custom ElectricSQL control
#
ARG JEPSEN_REGISTRY

FROM ${JEPSEN_REGISTRY:-}jepsen-control AS electricsql-control

# jepsen-causal-consistency
RUN git clone --depth 1 --branch main \
    "https://github.com/nurturenature/jepsen-causal-consistency.git" \
    /jepsen/jepsen-causal-consistency
RUN cd /jepsen/jepsen-causal-consistency && lein deps
