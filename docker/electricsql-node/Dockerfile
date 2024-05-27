#
# Custom ElectricSQL node
#
ARG JEPSEN_REGISTRY

FROM ${JEPSEN_REGISTRY:-}jepsen-node AS electricsql-setup

# deps
RUN apt-get -qy update && \
    apt-get -qy install \
    git gpg sqlite3 wget

# nodejs
RUN wget --quiet -O - "https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key" | gpg --dearmor | apt-key add -
RUN echo "deb https://deb.nodesource.com/node_20.x nodistro main" >> /etc/apt/sources.list.d/nodejs.list
RUN apt-get -qy update && \
    apt-get -qy install \
    nodejs

FROM electricsql-setup AS electricsql-node 

# jepsen-causal-consistency
RUN git clone --depth 1 "https://github.com/nurturenature/jepsen-causal-consistency.git" /jepsen/jepsen-causal-consistency
WORKDIR /jepsen/jepsen-causal-consistency/electric-sqlite
RUN npm install
