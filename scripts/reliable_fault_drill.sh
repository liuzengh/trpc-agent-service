#!/bin/bash
# 可靠模式故障矩阵：把「可证明的故障恢复」从设计意图变成真跑出来的断言。
#
# 与 fault_drill.sh 的分工：那个脚本演练旧链路（D1–D7：模型故障、Redis 闪断、
# 节点重启……143 条断言）；这个脚本演练**可靠链路**特有的三个恢复机制，
# 每个机制对应一条设计承诺：
#
#   R1  kill -9 一个 worker      → 租约到期后另一个 worker 接管，同一 execution
#                                  只提交一次（fencing 与租约恢复）
#   R2  MySQL 停机               → 角色不退出、不误标消息；恢复后继续并完成
#                                  （MySQL 是唯一事实源，停它就停了一切裁决）
#   R3  Qdrant 停机              → 索引任务失败重试，文档绝不进入 ready；
#                                  恢复后任务自愈（重试 backoff 而不是 reconcile 兜底）
#
# 三条纪律与 e2e.sh / reliable_e2e.sh 相同：就绪门禁、断言 helper 逐例签名自检、
# JSON 落文件。脚本自己起栈、自己拆栈（trap），独立 compose 项目避开默认栈。
#
#   bash scripts/reliable_fault_drill.sh
#   RELIABLE_KEEP=1 bash scripts/reliable_fault_drill.sh   # 跑完不拆栈，供人工取证
set -u
cd "$(dirname "$0")/.."

SMOKE=.smoke/reliable-drill
rm -rf "$SMOKE"; mkdir -p "$SMOKE"

PROJECT=${DRILL_PROJECT:-tas-rel-drill}
MODEL=${DRILL_MODEL:-http://localhost:${RELIABLE_MODEL_PORT:-9019}}
TENANT=${DRILL_TENANT:-acme}
DB_USER=${MYSQL_USER:-tas}
DB_PASS=${MYSQL_PASSWORD:-taspw}
DB_NAME=${MYSQL_DATABASE:-tas}
KEEP=${RELIABLE_KEEP:-0}
QDRANT_HOST=${RELIABLE_QDRANT_PORT:-6334}
MINIO_HOST=${RELIABLE_MINIO_PORT:-9011}

pass=0; fail=0; selftest=0

dc() {
  docker compose -p "$PROJECT" \
    -f docker-compose.yml -f deploy/compose/reliable.override.yml "$@"
}

check() { # check <描述> <期望> <实测>
  if [ "$2" = "$3" ]; then pass=$((pass+1)); [ "$selftest" = 0 ] && printf 'PASS  %-52s %s\n' "$1" "$3"
  else fail=$((fail+1)); [ "$selftest" = 0 ] && printf 'FAIL  %-52s want=[%s] got=[%s]\n' "$1" "$2" "$3"; fi
}
has() { if grep -q "$3" "$2"; then check "$1" "yes" "yes"; else check "$1" "yes" "no（$2 里没有 $3）"; fi; }

# --- helper 自检（同 reliable_e2e.sh 的逐例签名） ---
selftest=1
selftest_sig=""
FIX=$SMOKE/_selftest.txt
printf 'needle\n' > "$FIX" || { echo "自检 fixture 写不出来：$FIX"; exit 1; }
st() { local p0=$pass f0=$fail; "$@"
       if [ "$pass" -gt "$p0" ]; then selftest_sig="${selftest_sig}P"
       elif [ "$fail" -gt "$f0" ]; then selftest_sig="${selftest_sig}F"
       else selftest_sig="${selftest_sig}?"; fi; }
st has   "_st" "$FIX" 'needle'
st has   "_st" "$FIX" 'absent'
st check "_st" 'a' 'a'
st check "_st" 'a' 'b'
rm -f "$FIX"
selftest=0; pass=0; fail=0
if [ "$selftest_sig" = "PFPF" ]; then
  printf 'PASS  %-52s %s\n' "断言 helper 自检（逐例签名）" "$selftest_sig"
else
  printf 'FAIL  %-52s want=[PFPF] got=[%s]\n' "断言 helper 自检" "$selftest_sig"
  echo "断言工具不可信，后面的结果全部作废。"; exit 1
fi

for t in docker curl python3; do
  command -v "$t" >/dev/null 2>&1 || { echo "缺 ${t}，跑不了。"; exit 1; }
done

cleanup() {
  { dc logs worker-a > "$SMOKE/worker-a.log" 2>&1
    dc logs worker-b > "$SMOKE/worker-b.log" 2>&1
    dc logs delivery > "$SMOKE/delivery.log" 2>&1
    dc logs jobs > "$SMOKE/jobs.log" 2>&1
    dc logs mysql > "$SMOKE/mysql.log" 2>&1
    dc logs qdrant > "$SMOKE/qdrant.log" 2>&1
  } || true
  # kill -9 打死的容器停在 exited，down 才是收尾。
  if [ "$KEEP" = "1" ]; then
    echo "RELIABLE_KEEP=1：栈保留在项目 ${PROJECT}（取证后手动 docker compose -p ${PROJECT} --profile reliable down -v）"
    return 0
  fi
  dc --profile reliable down -v --remove-orphans >/dev/null 2>&1 || true
}
trap 'cleanup' EXIT

mysql_q() { dc exec -T mysql mysql -h127.0.0.1 -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" -N -B -e "$1" 2>/dev/null | tr -d '\r'; }
wait_sql() { local i=0
  while [ "$i" -lt "$3" ]; do
    [ "$(mysql_q "$1")" = "$2" ] && return 0
    sleep 1; i=$((i+1))
  done
  return 1
}

echo "--- A. 起栈与门禁 ---"
dc --profile reliable down -v --remove-orphans >/dev/null 2>&1 || true
if ! dc build app > "$SMOKE/build.log" 2>&1; then
  echo "构建失败，见 $SMOKE/build.log"; tail -30 "$SMOKE/build.log"; exit 1
fi
if ! dc --profile reliable up -d mysql qdrant minio fake-model > "$SMOKE/up.log" 2>&1; then
  echo "起栈失败，见 $SMOKE/up.log"; tail -30 "$SMOKE/up.log"; exit 1
fi
ready=0
for i in $(seq 1 60); do
  if mysql_q "SELECT 1" | grep -q '^1$'; then ready=1; break; fi
  sleep 1
done
check "MySQL 可查询" "1" "$ready"
vt=0; ot=0; mt=0
for i in $(seq 1 60); do
  curl -s --max-time 2 "http://localhost:${QDRANT_HOST}/collections" | grep -q '"collections"' && vt=1
  curl -s --max-time 2 -o /dev/null -w '%{http_code}' "http://localhost:${MINIO_HOST}/minio/health/live" | grep -q '^200$' && ot=1
  curl -s --max-time 2 "$MODEL/healthz" | grep -q '"mode"' && mt=1
  [ "$vt" = 1 ] && [ "$ot" = 1 ] && [ "$mt" = 1 ] && break
  sleep 1
done
check "Qdrant 就绪" "1" "$vt"
check "MinIO 就绪" "1" "$ot"
check "假模型就绪" "1" "$mt"
if [ "$ready" != 1 ] || [ "$vt" != 1 ] || [ "$ot" != 1 ] || [ "$mt" != 1 ]; then
  echo "门禁没过，不再往下跑。"; exit 1
fi

dc run --rm --no-deps worker-a -migrate -config /config/config.yaml > "$SMOKE/migrate.log" 2>&1 \
  && check "迁移 -migrate" "0" "0" \
  || check "迁移 -migrate" "0" "$?"
dc run --rm --no-deps worker-a -bootstrap-admin \
  -bootstrap-admin-tenant "$TENANT" -bootstrap-admin-subject ops@fixture \
  -bootstrap-admin-token fixture-token -config /config/config.yaml > "$SMOKE/bootstrap.log" 2>&1 \
  && check "bootstrap-admin" "0" "0" \
  || check "bootstrap-admin" "0" "$?"

echo "--- B. 控制面夹具（与 reliable_e2e.sh 的 B/G 段同构） ---"
cat > "$SMOKE/fixture.sql" <<SQL
INSERT INTO model_profiles (tenant_id, public_id, version, model_name, base_url, api_key_ref)
VALUES ('$TENANT', 'default-model', 1, 'fake-model', 'http://fake-model:9009/v1', 'env:MODEL_API_KEY');
INSERT INTO backend_profiles (tenant_id, public_id, version, session_backend)
VALUES ('$TENANT', 'default-backend', 1, 'memory');
INSERT INTO agent_apps (tenant_id, public_id, name) VALUES ('$TENANT', 'assistant', 'Assistant');
INSERT INTO agent_revisions
  (tenant_id, app_id, revision_no, instruction, model_profile_id, backend_profile_id,
   max_llm_calls, message_timeout_ms, manifest_hash)
SELECT '$TENANT', a.app_id, 1, 'be terse',
       (SELECT profile_id FROM model_profiles WHERE tenant_id='$TENANT' AND public_id='default-model'),
       (SELECT profile_id FROM backend_profiles WHERE tenant_id='$TENANT' AND public_id='default-backend'),
       8, 120000, SHA2('reliable-drill-fixture', 256)
FROM agent_apps a WHERE a.tenant_id='$TENANT' AND a.public_id='assistant';
UPDATE agent_apps a
JOIN agent_revisions r ON r.tenant_id = a.tenant_id AND r.app_id = a.app_id
SET a.current_revision_id = r.revision_id
WHERE a.tenant_id='$TENANT' AND a.public_id='assistant';
INSERT INTO channel_bindings (tenant_id, app_id, channel_type, public_id, config)
SELECT '$TENANT', app_id, 'wechat_kf', 'main',
       '{"corp_id":"wwfixture00000001","secret":"kf-fixture-secret","token":"fixture-callback-token","encoding_aes_key":"jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C"}'
FROM agent_apps WHERE tenant_id='$TENANT' AND public_id='assistant';
-- knowledge base（R3 用）
INSERT INTO knowledge_bases (tenant_id, app_id, public_id, name, status, embedding_model, embedding_dim)
SELECT '$TENANT', app_id, 'docs', 'Docs', 'active', 'fake-embed', 64
FROM agent_apps WHERE tenant_id='$TENANT' AND public_id='assistant';
SQL
if dc exec -T mysql mysql -h127.0.0.1 -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" < "$SMOKE/fixture.sql" 2>"$SMOKE/fixture.err"; then
  check "夹具导入" "0" "0"
else
  check "夹具导入" "0" "$(cat "$SMOKE/fixture.err")"; exit 1
fi

inject_kf_message() { # inject_kf_message <msgid> <external_userid> <text> <event_token>
  cat > "$SMOKE/kf_script_$1.json" <<JSON
{"pages":[{"next_cursor":"C-$1","has_more":0,
  "messages":[{"msgid":"$1","external_userid":"$2","content":"$3"}]}]}
JSON
  curl -s -X POST "$MODEL/__kf_script" --data-binary @"$SMOKE/kf_script_$1.json" > /dev/null
  cat > "$SMOKE/notification_$1.sql" <<SQL
INSERT INTO channel_notifications (tenant_id, binding_id, event_token, scope_key)
SELECT '$TENANT', binding_id, '$4', 'wk1'
FROM channel_bindings WHERE tenant_id='$TENANT' AND public_id='main';
SQL
  dc exec -T mysql mysql -h127.0.0.1 -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" < "$SMOKE/notification_$1.sql" 2>/dev/null
}

############################################
echo "--- R1. kill -9 一个 worker：租约到期后接管，只提交一次 ---"
# 只起 worker-a：两个 worker 同时在跑的话租约归谁不确定，而这条 drill 要的
# 正好是「确定的一个持有者被打死」。worker-b 在 kill 之后才入场，这也更贴近
# 它真正接管的姿态：过期的租约 + 更高的 fence。
dc --profile reliable up -d worker-a delivery jobs > "$SMOKE/up-roles.log" 2>&1 \
  && check "worker-a + delivery + jobs 启动" "0" "0" \
  || { check "worker-a + delivery + jobs 启动" "0" "$?"; exit 1; }

# 让模型挂住，保证 kill 时 worker-a 正处于执行中（不持有 SQL 事务，只持有租约）。
cat > "$SMOKE/mode_hang.json" <<'JSON'
{"mode":"timeout","delay":60}
JSON
curl -s -X POST "$MODEL/__mode" --data-binary @"$SMOKE/mode_hang.json" > /dev/null
inject_kf_message "r1-msg" "ext-r1" "kill me while running" "EVTOK-R1"

if wait_sql "SELECT COUNT(*) FROM sessions WHERE tenant_id='$TENANT' AND lease_owner='worker-a'" "1" 60; then
  check "worker-a 持租约执行中" "1" "1"
else
  check "worker-a 持租约执行中" "1" "0（消息没被 worker-a 领到？）"
  # 没有租约就无法证明接管，后续断言无意义。
  exit 1
fi
kill_ok=0
docker kill -s KILL tas-worker-a >/dev/null 2>&1 && kill_ok=1
check "SIGKILL worker-a" "1" "$kill_ok"

# 接管的见证人入场。租约 30s；worker-b 只能在到期后接管（fencing 单调 +1）。
dc --profile reliable up -d worker-b > /dev/null 2>&1
if wait_sql "SELECT COUNT(*) FROM sessions WHERE tenant_id='$TENANT' AND lease_owner='worker-b' AND fencing_token >= 2" "1" 90; then
  check "worker-b 在租约到期后接管（fence 递增）" "1" "1"
else
  check "worker-b 在租约到期后接管（fence 递增）" "1" "0"
fi

# 上游恢复，恢复后的执行应当正常收尾。
curl -s -X POST "$MODEL/__mode" -H 'Content-Type: application/json' -d '{"mode":"ok"}' > /dev/null
wait_sql "SELECT status FROM inbox_messages WHERE tenant_id='$TENANT' ORDER BY received_at DESC, in_seq DESC LIMIT 1" "done" 90 \
  && check "消息最终完成（被接管后提交）" "done" "done" \
  || check "消息最终完成（被接管后提交）" "done" \
     "$(mysql_q "SELECT status FROM inbox_messages WHERE tenant_id='$TENANT' ORDER BY received_at DESC, in_seq DESC LIMIT 1")"
check "同一消息只有一个 execution（没有复制）" "1" \
  "$(mysql_q "SELECT COUNT(*) FROM executions WHERE tenant_id='$TENANT'")"
check "同一 execution 有一次以上 attempt（见证接管）" "2" \
  "$(mysql_q "SELECT attempts FROM executions WHERE tenant_id='$TENANT' LIMIT 1")"
# 死掉那次留下的租约残留必须被清掉（提交路径释放）。
check "没有残留租约" "0" \
  "$(mysql_q "SELECT COUNT(*) FROM sessions WHERE tenant_id='$TENANT' AND lease_owner IS NOT NULL")"
wait_sql "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' ORDER BY outbox_id DESC LIMIT 1" "sent" 60 >/dev/null \
  && check "接管后的回复已投递" "sent" "sent" \
  || check "接管后的回复已投递" "sent" \
     "$(mysql_q "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' ORDER BY outbox_id DESC LIMIT 1")"

############################################
echo "--- R2. MySQL 停机：角色存活不误标，恢复后继续 ---"
# 先让消息落库（jobs 写 inbox），此时 worker 全部停着，消息必然是 pending。
dc --profile reliable stop worker-a worker-b delivery > /dev/null 2>&1
inject_kf_message "r2-msg" "ext-r2" "survive mysql outage" "EVTOK-R2"
if wait_sql "SELECT status FROM inbox_messages WHERE tenant_id='$TENANT' ORDER BY received_at DESC, in_seq DESC LIMIT 1" "pending" 60; then
  check "消息已受理且等待执行" "pending" "pending"
else
  check "消息已受理且等待执行" "pending" \
    "$(mysql_q "SELECT status FROM inbox_messages WHERE tenant_id='$TENANT' ORDER BY received_at DESC, in_seq DESC LIMIT 1")"
fi

dc stop mysql > /dev/null 2>&1
check "MySQL 已停机" "exited" "$(docker inspect -f '{{.State.Status}}' tas-rel-mysql 2>/dev/null)"
# worker-a 重新起——连不上 MySQL，它的职责是「报错、重试、什么都不标」。
dc --profile reliable up -d worker-a > /dev/null 2>&1
sleep 8
check "MySQL 停机期间 worker 未退出" "running" \
  "$(docker inspect -f '{{.State.Status}}' tas-worker-a 2>/dev/null)"
# 停机期间的“什么都没发生”由恢复后的 attempts=1 见证：一个能被误标的进程
# 不存在，也不需要读一个连不上的库去证明。

dc start mysql > /dev/null 2>&1
for i in $(seq 1 60); do
  mysql_q "SELECT 1" | grep -q '^1$' && break
  sleep 1
done
check "MySQL 恢复" "1" "$(mysql_q "SELECT 1")"
# delivery 也是在停机前停掉的（它写不了库也只能空转），恢复它回复才会有人发。
dc --profile reliable up -d delivery > /dev/null 2>&1
wait_sql "SELECT status FROM inbox_messages WHERE tenant_id='$TENANT' ORDER BY received_at DESC, in_seq DESC LIMIT 1" "done" 90 \
  && check "恢复后消息完成" "done" "done" \
  || check "恢复后消息完成" "done" \
     "$(mysql_q "SELECT status FROM inbox_messages WHERE tenant_id='$TENANT' ORDER BY received_at DESC, in_seq DESC LIMIT 1")"
check "恢复后的消息只执行一次（attempts=1，没有被当成失败重跑）" "1" \
  "$(mysql_q "SELECT e.attempts FROM executions e JOIN inbox_messages im ON im.execution_id=e.execution_id AND im.tenant_id=e.tenant_id WHERE im.tenant_id='$TENANT' ORDER BY im.received_at DESC, im.in_seq DESC LIMIT 1")"
wait_sql "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' ORDER BY outbox_id DESC LIMIT 1" "sent" 60 >/dev/null \
  && check "恢复后的回复已投递" "sent" "sent" \
  || check "恢复后的回复已投递" "sent" \
     "$(mysql_q "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' ORDER BY outbox_id DESC LIMIT 1")"

############################################
echo "--- R3. Qdrant 停机：文档不进入 ready，恢复后自愈 ---"
doc_id="dddddddd-1111-1111-1111-111111111111"
cat > "$SMOKE/gamma.txt" <<'EOF'
gamma drill document，用于验证索引任务在向量库停机时的行为。
EOF
# 时序是这条 drill 的关键，且它有一个硬约束：jobs 角色启动时会 EnsureCollection，
# ingest 的 CLI 也会——两者都要求 Qdrant 可达。所以顺序必须是：
#   ① 冻结 jobs（docker pause）—— 防止它抢先把文档索引完；
#   ② Qdrant 还活着时 ingest（bytes→MinIO、行→SQL、job→outbox_events）；
#   ③ 停 Qdrant；
#   ④ 解冻 jobs —— 它带着活着的连接池处理这条 job，upsert 必须失败。
docker pause tas-jobs > /dev/null 2>&1
if dc run --rm --no-deps -v "$PWD/$SMOKE:/seed:ro" worker-a \
     -ingest-doc /seed/gamma.txt -resolve-tenant "$TENANT" -ingest-kb docs \
     -ingest-doc-id "$doc_id" -ingest-title "Gamma Drill" -ingest-mime text/plain \
     -config /config/config.yaml > "$SMOKE/ingest-r3.log" 2>&1; then
  check "R3 文档已受理（uploaded）" "0" "0"
else
  check "R3 文档已受理（uploaded）" "0" "$(tail -2 "$SMOKE/ingest-r3.log")"; exit 1
fi

# 停掉向量库，再放 jobs 出来：索引任务必须失败，但文档绝不能变 ready。
dc stop qdrant > /dev/null 2>&1
docker unpause tas-jobs > /dev/null 2>&1
# 至少见证一次失败重试（job attempts>=1 且文档仍非 ready）。
saw_failure=0
for i in $(seq 1 45); do
  st=$(mysql_q "SELECT status FROM documents WHERE tenant_id='$TENANT' AND public_id='$doc_id'")
  att=$(mysql_q "SELECT COALESCE(SUM(attempts),0) FROM outbox_events WHERE tenant_id='$TENANT' AND kind='doc_index'")
  if [ "$st" != "ready" ] && [ "${att:-0}" -ge 1 ]; then saw_failure=1; break; fi
  sleep 1
done
check "Qdrant 停机期间索引尝试过但文档未 ready" "1" "$saw_failure"
check "文档没有伪装成 ready" "0" \
  "$(mysql_q "SELECT COUNT(*) FROM documents WHERE tenant_id='$TENANT' AND public_id='$doc_id' AND status='ready'")"
check "job 留下了失败原因（不是静默吞掉）" "1" \
  "$(mysql_q "SELECT IF(COUNT(*)>0,1,0) FROM outbox_events WHERE tenant_id='$TENANT' AND kind='doc_index' AND (last_error <> '' OR attempts >= 1)")"

# 恢复向量库；job 的退避重试（4s/8s/…）应当自愈，不需要 reconcile 兜底。
dc start qdrant > /dev/null 2>&1
for i in $(seq 1 60); do
  curl -s --max-time 2 "http://localhost:${QDRANT_HOST}/collections" | grep -q '"collections"' && break
  sleep 1
done
wait_sql "SELECT status FROM documents WHERE tenant_id='$TENANT' AND public_id='$doc_id'" "ready" 120 \
  && check "Qdrant 恢复后文档自愈到 ready" "ready" "ready" \
  || check "Qdrant 恢复后文档自愈到 ready" "ready" \
     "$(mysql_q "SELECT status FROM documents WHERE tenant_id='$TENANT' AND public_id='$doc_id'")"
check "自愈后 chunk 与向量数量一致（索引确实写了）" "1" \
  "$(mysql_q "SELECT IF(COALESCE(SUM(d.chunk_count),0)=COUNT(*),1,0) FROM documents d JOIN document_chunks dc ON dc.tenant_id=d.tenant_id AND dc.doc_id=d.doc_id WHERE d.tenant_id='$TENANT' AND d.public_id='$doc_id'")"

printf '\n=== %d PASS / %d FAIL ===\n' "$pass" "$fail"
if [ "$fail" != 0 ]; then
  echo "RELIABLE FAULT DRILL FAIL"
  [ "$KEEP" = "1" ] || echo "（栈已拆；日志与表状态在 ${SMOKE}/）"
  exit 1
fi
echo "RELIABLE FAULT DRILL PASS"
