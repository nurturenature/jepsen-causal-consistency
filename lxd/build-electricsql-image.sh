#! /usr/bin/bash
set -e

echo "install deps..."
apt update  -qy
apt install -qy lsb-release gpg wget

echo "install postgresql-client..."
wget --quiet -O - "https://www.postgresql.org/media/keys/ACCC4CF8.asc" | sudo apt-key add -
echo "deb https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list
apt update  -qy

apt install -qy postgresql-client

echo "install erlang..."
apt install -qy build-essential autoconf m4 libncurses-dev libwxgtk3.2-dev libwxgtk-webview3.2-dev libgl1-mesa-dev libglu1-mesa-dev libpng-dev libssh-dev unixodbc-dev xsltproc fop libxml2-utils wget
mkdir /root/erlang
cd /root/erlang
wget "https://github.com/erlang/otp/releases/download/OTP-25.3.2.8/otp_src_25.3.2.8.tar.gz"
tar -xvzf "./otp_src_25.3.2.8.tar.gz"
cd otp_src_25.3.2.8
ERL_TOP=$(pwd)
export ERL_TOP=$ERL_TOP
./configure
make
make install
echo "erlang version:"
erl -eval "erlang:display (erlang:system_info (otp_release)), halt () ." -noshell

echo "install elixir..."
apt install -qy git
mkdir /root/elixir
cd /root/elixir
wget "https://github.com/elixir-lang/elixir/archive/refs/tags/v1.15.7.tar.gz"
tar -xvzf ./v1.15.7.tar.gz
cd elixir-1.15.7
make
ln -s "/root/elixir/elixir-1.15.7/bin/elixir"  "/usr/local/bin/elixir"
ln -s "/root/elixir/elixir-1.15.7/bin/elixirc" "/usr/local/bin/elixirc"
ln -s "/root/elixir/elixir-1.15.7/bin/iex"     "/usr/local/bin/iex"
ln -s "/root/elixir/elixir-1.15.7/bin/mix"     "/usr/local/bin/mix"
echo "elixir version:"
elixir -v

echo "install electricsql"
apt install -qy git
mkdir /root/electricsql
cd /root/electricsql
wget "https://github.com/electric-sql/electric/archive/refs/tags/electric-sql@0.9.0.tar.gz"
tar -xvzf ./electric-sql@0.9.0.tar.gz
cd electric-electric-sql-0.9.0
cd components/electric
mix deps.get
mix compile
export MIX_ENV=prod
export ELECTRIC_VERSION=0.9.0
mix release

echo "electricsql systemd..."
cat <<EOF > /etc/systemd/system/electricsql.service
[Unit]
Description=ElectricSQL.
Requires=network-online.target


[Service]
Type=simple
Restart=on-failure

Environment=HOME=/root
Environment=MIX_ENV=prod
Environment=DATABASE_URL=postgresql://postgres:postgres@postgresql
Environment=ELECTRIC_WRITE_TO_PG_MODE=direct_writes
Environment=PG_PROXY_PORT=65432
Environment=PG_PROXY_PASSWORD=postgres
Environment=AUTH_MODE=insecure
Environment=ELECTRIC_USE_IPV6=false

WorkingDirectory=/root/electricsql/electric-electric-sql-0.9.0/components/electric

ExecStart=/root/electricsql/electric-electric-sql-0.9.0/components/electric/_build/prod/rel/electric/bin/electric start
ExecStop=/root/electricsql/electric-electric-sql-0.9.0/components/electric/_build/prod/rel/electric/bin/electric stop


[Install]
WantedBy=multi-user.target
EOF
chmod 644 /etc/systemd/system/electricsql.service

systemctl enable electricsql
