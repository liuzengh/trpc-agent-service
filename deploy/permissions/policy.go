// Package permissions renders explicit, fail-closed deployment policies. It
// never connects to databases, creates login credentials or applies changes.
package permissions

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var identifier = regexp.MustCompile(`^[a-z][a-z0-9_]{0,39}$`)
var redisPrefix = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$`)

var control = []string{"tenant", "agent_app", "agent_revision", "channel_binding", "backend_binding", "backend_migration"}
var roles = []string{"gateway", "worker", "relay", "sender", "jobs", "admin"}

// SQL creates NOLOGIN group roles. Apply only to a dedicated, already migrated
// schema as its owner. Existing roles abort the transaction, never get reset.
func SQL(schema, prefix string) (string, error) {
	if !identifier.MatchString(schema) || schema == "public" || !identifier.MatchString(prefix) {
		return "", errors.New("dedicated schema and safe role prefix required")
	}
	var out strings.Builder
	fmt.Fprintf(&out, "BEGIN;\nREVOKE ALL ON SCHEMA %s FROM PUBLIC;\nREVOKE ALL ON ALL TABLES IN SCHEMA %s FROM PUBLIC;\n", schema, schema)
	for _, role := range roles {
		name := prefix + "_" + role
		fmt.Fprintf(&out, "CREATE ROLE %s NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;\nGRANT USAGE ON SCHEMA %s TO %s;\n", name, schema, name)
		grants := roleGrants(role)
		tables := make([]string, 0, len(grants))
		for table := range grants {
			tables = append(tables, table)
		}
		sort.Strings(tables)
		for _, table := range tables {
			fmt.Fprintf(&out, "GRANT %s ON %s.%s TO %s;\n", strings.Join(grants[table], ", "), schema, table, name)
		}
	}
	out.WriteString("COMMIT;\n")
	return out.String(), nil
}

func roleGrants(role string) map[string][]string {
	m := map[string][]string{}
	add := func(priv string, tables ...string) {
		for _, table := range tables {
			for _, p := range strings.Split(priv, ",") {
				found := false
				for _, old := range m[table] {
					found = found || old == p
				}
				if !found {
					m[table] = append(m[table], p)
				}
			}
		}
	}
	add("INSERT", "audit_log")
	switch role {
	case "gateway":
		add("SELECT", "platform_backlog")
		add("SELECT", control...)
		add("SELECT,INSERT,UPDATE", "conversation", "inbound_message", "agent_run", "tool_approval", "approval_decision_message", "channel_poll_checkpoint")
		add("SELECT,INSERT", "queue_outbox", "outbound_message", "channel_poll_seen")
		add("SELECT,INSERT", "channel_message_rejection")
	case "worker":
		add("SELECT", control...)
		add("SELECT", "conversation")
		add("SELECT,UPDATE", "inbound_message", "agent_run")
		add("SELECT,INSERT", "outbound_message", "work_item")
		add("SELECT,INSERT,UPDATE", "tool_execution", "tool_operation", "tool_approval", "background_job")
		add("UPDATE", "backend_migration")
	case "relay":
		add("SELECT,UPDATE", "queue_outbox")
	case "sender":
		add("SELECT", "tenant", "agent_app", "channel_binding")
		add("SELECT,UPDATE", "outbound_message")
		add("SELECT,INSERT,UPDATE", "channel_delivery_attempt")
	case "jobs":
		add("SELECT", control...)
		add("UPDATE", "backend_binding", "backend_migration")
		add("SELECT,INSERT,UPDATE", "background_job")
	case "admin":
		add("SELECT", "platform_backlog")
		add("SELECT", control...)
		add("INSERT", "tenant", "agent_app", "agent_revision", "channel_binding", "backend_binding", "backend_migration")
		add("UPDATE", "agent_app", "channel_binding", "backend_binding", "backend_migration")
		add("SELECT", "audit_log", "work_item", "channel_poll_checkpoint", "channel_delivery_attempt")
		add("SELECT,UPDATE", "tool_execution", "tool_operation")
		add("SELECT,INSERT,UPDATE", "background_job")
		add("SELECT", "channel_message_rejection", "channel_checkpoint_recovery")
		add("INSERT", "channel_checkpoint_recovery")
		add("UPDATE", "channel_poll_checkpoint")
	}
	return m
}

// Redis emits Redis 7 selector syntax, with every account OFF and without a
// password. Enable it only after injecting a secret out of band. Data backends
// use a separate scoped credential; this policy covers platform coordination,
// quotas, idempotency, Streams and the pinned Redis Session/Memory key families.
// Optional backends with other prefixes need separately reviewed credentials.
func Redis(prefix, keyPrefix string) (string, error) {
	if !identifier.MatchString(prefix) || !redisPrefix.MatchString(keyPrefix) {
		return "", errors.New("safe Redis user/key prefixes required")
	}
	var out strings.Builder
	base := "reset off -@all +ping +hello +select +client|setinfo +client|setname"
	commands := "+get +set +del +exists +incr +decr +incrby +incrbyfloat +expire +pexpire +psetex +mget +eval +evalsha +script|load"
	selector := func(pattern, cmds string) string { return " (~" + keyPrefix + pattern + " " + cmds + ")" }
	for _, role := range roles {
		fmt.Fprintf(&out, "user %s_%s %s", prefix, role, base)
		switch role {
		case "gateway":
			out.WriteString(selector(":quota:rate:*", commands))
			out.WriteString(selector(":quota:usage:*", "+mget +get"))
			out.WriteString(selector(":channel-poll:coord:session:*", commands))
		case "worker":
			out.WriteString(selector(":quota:*", commands))
			out.WriteString(selector(":coord:session:*", commands))
			out.WriteString(selector(":idempotency:message:*", commands))
			out.WriteString(selector(":stream:*", "+xgroup|create +xreadgroup +xautoclaim +xack +xadd"))
		case "relay":
			out.WriteString(selector(":stream:*", "+xgroup|create +xadd"))
		}
		if role == "worker" || role == "jobs" {
			dataCommands := commands + " +hget +hgetall +hmget +hset +hdel +hexists +hscan +hincrby +zadd +zrange +zrevrange +zrangebyscore +zrevrangebyscore +zcard +zrem +zscore +sadd +srem +smembers +sscan +scard +persist +pttl +ttl +multi +exec +discard +watch +unwatch"
			for _, pattern := range []string{":hashidx:*", ":sess:*", ":appstate:*", ":userstate:*", ":mem:*"} {
				out.WriteString(selector(pattern, dataCommands))
			}
		}
		out.WriteByte('\n')
	}
	return out.String(), nil
}
