#! /usr/bin/bash
set -e

if [ -z "$1" ]; then
  echo "Usage: ./lxc-jepsen-node.sh node-name";
  exit 1;
fi

lxc stop   "$1" --force || true
lxc delete "$1"         || true

lxc launch images:debian/12 "$1"
sleep 10

lxc exec "$1" -- sh -c "apt-get -qy update && apt-get -qy install openssh-server sudo"

lxc exec "$1" -- sh -c "mkdir -p /root/.ssh && chmod 700 /root/.ssh/"
lxc file push ~/.ssh/id_rsa.pub "$1/root/.ssh/authorized_keys" --uid 0 --gid 0 --mode 644;

lxc exec "$1" -- bash -c 'echo -e "root\nroot\n" | passwd root'
lxc exec "$1" -- sed -i 's,^#\?PermitRootLogin .*,PermitRootLogin yes,g' /etc/ssh/sshd_config
lxc exec "$1" -- systemctl restart sshd

ssh-keygen -f ~/.ssh/known_hosts -R "$1" || true
ssh-keyscan -t rsa "$1" >> ~/.ssh/known_hosts
