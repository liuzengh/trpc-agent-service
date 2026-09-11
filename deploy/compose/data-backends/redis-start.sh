#!/bin/sh
set -eu
source_acl=${REDIS_ACL_FILE:-/run/redis-private/users.acl}
[ -r "$source_acl" ] || { echo 'REDIS_START=FAIL acl_unreadable' >&2; exit 1; }
# Preserve host-mounted ACL ownership; the official entrypoint drops to redis.
# Copy only hashes into the owned persistent volume before dropping privileges.
umask 077
cp "$source_acl" /data/users.acl.new
chmod 600 /data/users.acl.new
if [ "$(id -u)" = 0 ]; then chown redis:redis /data/users.acl.new; fi
mv /data/users.acl.new /data/users.acl
exec /usr/local/bin/docker-entrypoint.sh redis-server --dir /data --aclfile /data/users.acl --appendonly yes --appendfsync always --maxmemory-policy noeviction
