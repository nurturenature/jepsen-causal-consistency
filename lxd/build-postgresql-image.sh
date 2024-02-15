#! /usr/bin/bash
set -e

echo "install deps..."
apt update  -qy
apt install -qy lsb-release gpg wget

echo "install postgresql..."
wget --quiet -O - "https://www.postgresql.org/media/keys/ACCC4CF8.asc" | sudo apt-key add -
echo "deb https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list
apt update  -qy

apt install -qy postgresql
pg_isready

echo "enable logical replication..."
su - postgres -c "psql -U postgres -c 'ALTER SYSTEM SET wal_level = logical;'"
systemctl restart postgresql
pg_isready
su - postgres -c "psql -U postgres -c 'show wal_level;'"

echo "configure access"
echo "listen_addresses = '*'" >> "/etc/postgresql/16/main/postgresql.conf"
echo "host all postgres all scram-sha-256" >> "/etc/postgresql/16/main/pg_hba.conf"
systemctl restart postgresql
pg_isready

echo "set electricsql password"
su - postgres -c "psql -U postgres -c \"ALTER USER postgres WITH PASSWORD 'postgres';\""

echo "PostgreSQL tables:"
su - postgres -c "psql -U postgres -c '\\dt';"
