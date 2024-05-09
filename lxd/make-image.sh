#! /usr/bin/bash
set -e

if [ -z "$1" ]; then
  echo "Usage: ./make-image.sh image-name";
  exit 1;
fi

lxc stop   "$1" --force || true
lxc delete "$1"         || true

lxc launch images:debian/12 "$1"
sleep 3

lxc file push "./build-$1-image.sh" "$1/" --uid 0 --gid 0 --mode 744

echo "building $1..."
lxc exec "$1" -- bash -c "/build-$1-image.sh"

echo "publishing $1..."
lxc stop "$1"
lxc image delete "$1" || true
lxc publish "$1" --alias "$1"

lxc delete "$1"
