# Setting Up a Jepsen Environment with LXD/LXC

## Debian 12 - Bookworm

For further information, [LXD - Debian Wiki](https://wiki.debian.org/LXD).

### Install host packages:
```bash
sudo apt install lxd lxd-tools dnsmasq-base btrfs-progs
```

### Initialize LXD:
```bash
# defaults are good
sudo lxd init

# add yourself to the LXD group
sudo usermod -aG lxd <username>

# will need to logout/login for new group to be active

# try creating a sample container if you want
lxc launch images:debian/12 scratch
lxc list
lxc exec scratch -t bash
lxc stop scratch
lxc delete scratch
```

### Create and start Jepsen's node containers:

```bash
for i in {1..10}; do lxc launch images:debian/12 n${i}; done
```

### Configure LXD bridge network:

`lxd` automatically creates the bridge network, and `lxc launch` automatically configures containers for it: 
```bash
lxc network list
+--------+----------+---------+----------------+---+-------------+---------+---------+
|  NAME  |   TYPE   | MANAGED |      IPV4      |...| DESCRIPTION | USED BY |  STATE  |
+--------+----------+---------+----------------+---+-------------+---------+---------+
| lxdbr0 | bridge   | YES     | 10.82.244.1/24 |...|             | 11      | CREATED |
+--------+----------+---------+----------------+---+-------------+---------+---------+
```

Assuming you are using `systemd-resolved`:

```bash
# confirm your settings
lxc network get lxdbr0 ipv4.address
lxc network get lxdbr0 ipv6.address
lxc network get lxdbr0 dns.domain    # will be blank if default lxd is used 

# create a systemd unit file
sudo nano /etc/systemd/system/lxd-dns-lxdbr0.service
# with the contents:
[Unit]
Description=LXD per-link DNS configuration for lxdbr0
BindsTo=sys-subsystem-net-devices-lxdbr0.device
After=sys-subsystem-net-devices-lxdbr0.device

[Service]
Type=oneshot
ExecStart=/usr/bin/resolvectl dns lxdbr0 10.82.244.1
ExecStart=/usr/bin/resolvectl domain lxdbr0 ~lxd
ExecStopPost=/usr/bin/resolvectl revert lxdbr0
RemainAfterExit=yes

[Install]
WantedBy=sys-subsystem-net-devices-lxdbr0.device

# bring up and confirm status
 sudo systemctl daemon-reload
 sudo systemctl enable --now lxd-dns-lxdbr0
 sudo systemctl status lxd-dns-lxdbr0.service
 sudo resolvectl status lxdbr0

ping n1
PING n1(n1 (fe80::216:3eff:fe22:bfe4%lxdbr0)) 56 data bytes
64 bytes from n1 (fe80::216:3eff:fe22:bfe4%lxdbr0): icmp_seq=1 ttl=64 time=0.111 ms
```

### Add required packages to node containers:

```bash
for i in {1..10}; do
  lxc exec n${i} -- sh -c "apt-get -qy update && apt-get -qy install openssh-server sudo";
done
```

### Configure SSH:

Slip your preferred SSH key into each node's `.ssh/.authorized-keys`:
```bash
for i in {1..10}; do
  lxc exec n${i} -- sh -c "mkdir -p /root/.ssh && chmod 700 /root/.ssh/";
  lxc file push ~/.ssh/id_rsa.pub n${i}/root/.ssh/authorized_keys --uid 0 --gid 0 --mode 644;
done
```

Reset the root password to root, and allow root logins with passwords on each container.
If you've got an SSH agent set up, Jepsen can use that instead.
```bash
for i in {1..10}; do
  lxc exec n${i} -- bash -c 'echo -e "root\nroot\n" | passwd root';
  lxc exec n${i} -- sed -i 's,^#\?PermitRootLogin .*,PermitRootLogin yes,g' /etc/ssh/sshd_config;
  lxc exec n${i} -- systemctl restart sshd;
done
```

Store the node keys unencrypted so that jsch can use them.
If you already have the node keys, they may be unreadable to Jepsen -- remove them from `~/.ssh/known_hosts` and rescan:
```bash
for n in {1..10}; do
  ssh-keyscan -t rsa n${n} >> ~/.ssh/known_hosts;
done
```

### Confirm that your host can ssh in:

```bash
ssh root@n1
```

### Stopping and deleting containers:

```bash
for i in {1..10}; do
  lxc stop n${i};
  lxc delete n${i};
done
```

----

## Misc

The `lxc` command's \<Tab\> completion works well, even autocompletes container names.

### LXD/LXC and Docker

There are issues with running LXD and Docker simultaneously, Docker grabs port forwarding.
Running Docker in an LXC container resolves the issue:
 [Prevent connectivity issues with LXD and Docker](https://documentation.ubuntu.com/lxd/en/latest/howto/network_bridge_firewalld/#prevent-connectivity-issues-with-lxd-and-docker).
