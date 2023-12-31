FROM debian

ENV container=docker
STOPSIGNAL SIGRTMIN+3

# Basic system stuff
RUN apt-get -qy update && \
    apt-get -qy install \
    apt-transport-https

# Install packages
RUN apt-get -qy update && \
    apt-get -qy install \
    sqlite3 postgresql-client

RUN mkdir /setup
ADD setup-electric.sh /setup
RUN chmod +x /setup/setup-electric.sh

CMD ["/usr/bin/bash"]
