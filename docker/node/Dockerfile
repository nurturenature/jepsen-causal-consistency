# See https://salsa.debian.org/jgoerzen/docker-debian-base
# See https://hub.docker.com/r/jgoerzen/debian-base-standard
FROM jgoerzen/debian-base-standard:bookworm

ENV container=docker
STOPSIGNAL SIGRTMIN+3

# Basic system stuff
RUN apt-get -qy update && \
    apt-get -qy install \
    apt-transport-https

# Install packages
RUN apt-get -qy update && \
    apt-get -qy install \
    dos2unix openssh-server pwgen

# When run, boot-debian-base will call this script, which does final
# per-db-node setup stuff.
ADD setup-jepsen.sh /usr/local/preinit/03-setup-jepsen
RUN chmod +x /usr/local/preinit/03-setup-jepsen

# Configure SSHD
RUN sed -i "s/#PermitRootLogin prohibit-password/PermitRootLogin yes/g" /etc/ssh/sshd_config

# Enable SSH server
ENV DEBBASE_SSH enabled

# Install Jepsen deps
RUN apt-get -qy update && \
    apt-get -qy install \
    build-essential bzip2 ca-certificates curl dirmngr dnsutils faketime iproute2 iptables iputils-ping libzip4 logrotate lsb-release man man-db netcat-openbsd net-tools ntpdate psmisc python3 rsyslog sudo tar tcpdump unzip vim wget

## electricsql
RUN apt-get -qy update && \
    apt-get -qy install \
    gpg wget
RUN wget --quiet -O - "https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key" | gpg --dearmor | apt-key add -
RUN echo "deb https://deb.nodesource.com/node_20.x nodistro main" > /etc/apt/sources.list.d/nodejs.list
RUN apt-get -qy update && \
    apt-get -qy install \
    nodejs

RUN apt-get -qy update && \
    apt-get -qy install \
    git gpg postgresql-client sqlite3

## jepsen-causal-consistency
## RUN git clone https://github.com/nurturenature/jepsen-causal-consistency.git /root/jepsen-causal-consistency
## RUN cd /root/jepsen-causal-consistency/electricsql; npm install

EXPOSE 22
CMD ["/usr/local/bin/boot-debian-base"]
