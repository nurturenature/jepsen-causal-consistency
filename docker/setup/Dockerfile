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

RUN mkdir /root/setup
ADD electric-setup.sh /root/setup
RUN chmod +x /root/setup/electric-setup.sh

CMD ["/usr/bin/bash"]
