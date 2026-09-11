#!/bin/bash
# 可靠模式全链路 E2E：Inbox → Worker（×2）→ 提交 → Delivery → 假 KF 上游。
#
# 与 scripts/e2e.sh 的分工：那个脚本断言**旧链路**（单进程、回调内联 dispatch、
# SSE 逐 chunk）；这个脚本断言**可靠链路**（MySQL 事实源、双 worker、回复走
# reply_outbox 由 delivery 角色投递）。两条链路共享同一份代码但不同的装配方式，
# 脚本也各自独立。
#
# 它自己起栈、自己拆栈（trap 上，中途失败也不会把 MySQL 留在跑着的状态），
# 用独立 compose 项目（tas-reliable）+ override 文件与默认栈共存，见
# deploy/compose/reliable.override.yml。
#
# 沿用 e2e.sh 的三条纪律：
#   1. 先过就绪门禁再断言（MySQL 没起来时的断言全是假绿/假红）；
#   2. 断言 helper 自己先过逐例签名自检；
#   3. JSON payload 落文件再 --data-binary @file，不在 "$( )" 内联带引号的 JSON。
#
# 关于「回调」：可靠 gateway 角色（-role gateway）已挂载三个回调面，全部先落库
# 再 ACK。本脚本对微信客服与企微**真发加密签名的回调**（python3 组明文 +
# openssl 做 AES-256-CBC + SHA1 签名，与官方算法一致），断言只认库里长出来的
# 行——不再有 SQL 模拟。webchat 段另走邮箱推送（SSE 轮询 reply_outbox）。
#
# 变量名紧邻中文时必须写成 ${VAR}（bash 3.2 的多字节标识符陷阱，见 e2e.sh 头）。
#
#   bash scripts/reliable_e2e.sh
#   RELIABLE_KEEP=1 bash scripts/reliable_e2e.sh   # 跑完不拆栈，供人工取证
set -u
cd "$(dirname "$0")/.."

SMOKE=.smoke/reliable
rm -rf "$SMOKE"; mkdir -p "$SMOKE"

PROJECT=${RELIABLE_PROJECT:-tas-reliable}
MODEL=${RELIABLE_MODEL:-http://localhost:${RELIABLE_MODEL_PORT:-9019}}
GATEWAY=${RELIABLE_GATEWAY:-http://localhost:${RELIABLE_GATEWAY_PORT:-9020}}
TENANT=${RELIABLE_TENANT:-acme}
DB_USER=${MYSQL_USER:-tas}
DB_PASS=${MYSQL_PASSWORD:-taspw}
DB_NAME=${MYSQL_DATABASE:-tas}
KEEP=${RELIABLE_KEEP:-0}

# 微信回调加密的官方向量，与 B 段 fixture 的 channel_bindings.config 一致。
WX_CORP=wwfixture00000001
WX_TOKEN=fixture-callback-token
WX_AES_KEY=jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C
WX_TS=1731000000
WX_NONCE=fixture-nonce

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

# --- helper 自检（纪律 2），失败即中止 ---
# 预期签名逐例拼出来，不是数总数：四种组合里各错一个只有逐例签名能区分。
selftest=1
selftest_sig=""
FIX=$SMOKE/_selftest.txt
printf 'needle\n' > "$FIX" || { echo "自检 fixture 写不出来：$FIX"; exit 1; }
st() { local p0=$pass f0=$fail; "$@"
       if [ "$pass" -gt "$p0" ]; then selftest_sig="${selftest_sig}P"
       elif [ "$fail" -gt "$f0" ]; then selftest_sig="${selftest_sig}F"
       else selftest_sig="${selftest_sig}?"; fi; }
st has   "_st" "$FIX" 'needle'   # 存在 → P
st has   "_st" "$FIX" 'absent'   # 不存在 → F
st check "_st" 'a' 'a'           # 相等 → P
st check "_st" 'a' 'b'           # 不等 → F
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
  # 先把证据捞出来再拆栈：拆完容器就没了，日志和表状态是本次运行唯一的现场。
  { dc logs worker-a > "$SMOKE/worker-a.log" 2>&1
    dc logs worker-b > "$SMOKE/worker-b.log" 2>&1
    dc logs delivery > "$SMOKE/delivery.log" 2>&1
    dc logs jobs > "$SMOKE/jobs.log" 2>&1
    dc logs gateway > "$SMOKE/gateway.log" 2>&1
    dc logs mysql > "$SMOKE/mysql.log" 2>&1
  } || true
  if [ "$KEEP" = "1" ]; then
    echo "RELIABLE_KEEP=1：栈保留在项目 ${PROJECT}（取证后手动 docker compose -p ${PROJECT} --profile reliable down -v）"
    return 0
  fi
  # --profile reliable 是必需的：不带 profile 的 down 只拆「已启用 profile」的服务，
  # mysql 在 reliable profile 下，实测会被落在后面让下次 up 撞名。
  dc --profile reliable down -v --remove-orphans >/dev/null 2>&1 || true
}
trap 'cleanup' EXIT

mysql_q() { # mysql_q <sql> —— 单值/单行输出（-N 去掉表头，-B 制表符分隔）
  dc exec -T mysql mysql -h127.0.0.1 -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" -N -B -e "$1" 2>/dev/null | tr -d '\r'
}

wait_sql() { # wait_sql <sql> <want> <timeout_s> —— 轮询直到相等
  local i=0
  while [ "$i" -lt "$3" ]; do
    [ "$(mysql_q "$1")" = "$2" ] && return 0
    sleep 1; i=$((i+1))
  done
  return 1
}

# --- 微信系回调的加密与签名（KfPuller / WeCom 共用官方算法） ---
# 明文 = random(16) + msg_len(4, 网络序) + msg + receiveid，AES-256-CBC；
# 填充是 PKCS#7 但**块大小为 32**（微信专有，不是 AES 的 16）：python 侧手工补齐，
# openssl 用 -nopad 只做加解密。签名 = SHA1(sort(token, timestamp, nonce, encrypt))。
# AES key = base64decode(EncodingAESKey + "=")，IV 取其前 16 字节。
# （曾踩过：openssl 默认按 16 对齐填充，解密端 pkcs7Unpad(b,32) 因长度非 32 倍数直接
# 返回 nil，报 "plaintext too short"——两条链里只有一条恰好对齐 32，伪装成“只有 KF 坏”。）
encrypt_callback_body() { # <plain_file> <out_body_file>；设置 WX_SIG
  local key_hex iv_hex enc
  key_hex=$(printf '%s=' "$WX_AES_KEY" | openssl base64 -d -A | xxd -p -c 64)
  iv_hex=${key_hex:0:32}
  enc=$(openssl enc -aes-256-cbc -nopad -K "$key_hex" -iv "$iv_hex" -in "$1" -a | tr -d '\n')
  WX_SIG=$(python3 -c 'import hashlib,sys; print(hashlib.sha1("".join(sorted(sys.argv[1:5])).encode()).hexdigest())' \
    "$WX_TOKEN" "$WX_TS" "$WX_NONCE" "$enc")
  printf '<xml><ToUserName><![CDATA[%s]]></ToUserName><Encrypt><![CDATA[%s]]></Encrypt></xml>' \
    "$WX_CORP" "$enc" > "$2"
}

# post_kf_callback <event_token> <open_kfid> —— 真回调给 gateway，打印 HTTP 码，
# ACK 体落 _callback_ack.txt。
post_kf_callback() {
  local plain=$SMOKE/_kf_plain.bin body=$SMOKE/_kf_body.xml
  python3 - "$1" "$2" "$WX_CORP" "$plain" <<'PY'
import struct, sys
ev, kf, corp, out = sys.argv[1:5]
msg = ("<xml><ToUserName><![CDATA[%s]]></ToUserName>" % corp +
       "<CreateTime>1731000000</CreateTime>" +
       "<MsgType><![CDATA[event]]></MsgType>" +
       "<Event><![CDATA[kf_msg_or_event]]></Event>" +
       ("<Token><![CDATA[%s]]></Token>" % ev) +
       ("<OpenKfId><![CDATA[%s]]></OpenKfId></xml>" % kf)).encode()
plain = b"0123456789abcdef" + struct.pack(">I", len(msg)) + msg + corp.encode()
pad = 32 - len(plain) % 32
plain += bytes([pad]) * pad
open(out, "wb").write(plain)
PY
  encrypt_callback_body "$plain" "$body"
  curl -s -o "$SMOKE/_callback_ack.txt" -w '%{http_code}' \
    -X POST "$GATEWAY/callback/wechat_kf/$TENANT?msg_signature=$WX_SIG&timestamp=$WX_TS&nonce=$WX_NONCE" \
    --data-binary @"$body"
}

# post_wecom_callback <from_user> <content> <msg_id> —— 真回调给 gateway。
post_wecom_callback() {
  local plain=$SMOKE/_wecom_plain.bin body=$SMOKE/_wecom_body.xml
  python3 - "$1" "$2" "$3" "$WX_CORP" "$plain" <<'PY'
import struct, sys
frm, content, msgid, corp, out = sys.argv[1:6]
msg = ("<xml><ToUserName><![CDATA[%s]]></ToUserName>" % corp +
       ("<FromUserName><![CDATA[%s]]></FromUserName>" % frm) +
       "<CreateTime>1731000000</CreateTime>" +
       "<MsgType><![CDATA[text]]></MsgType>" +
       ("<Content><![CDATA[%s]]></Content>" % content) +
       ("<MsgId>%s</MsgId></xml>" % msgid)).encode()
plain = b"0123456789abcdef" + struct.pack(">I", len(msg)) + msg + corp.encode()
pad = 32 - len(plain) % 32
plain += bytes([pad]) * pad
open(out, "wb").write(plain)
PY
  encrypt_callback_body "$plain" "$body"
  curl -s -o "$SMOKE/_callback_ack.txt" -w '%{http_code}' \
    -X POST "$GATEWAY/callback/wecom/$TENANT?msg_signature=$WX_SIG&timestamp=$WX_TS&nonce=$WX_NONCE" \
    --data-binary @"$body"
}

echo "--- A. 起栈与就绪门禁 ---"
# 从干净状态开始：上一轮若被 Ctrl-C 或机器重启打断，容器和命名卷都会留下，
# 而这轮的夹具假设「空库」（tenant 不存在、profile 不重复）。先清再起。
dc --profile reliable down -v --remove-orphans >/dev/null 2>&1 || true
# 先构建。镜像只有一个构建入口（app 服务的 build: 段），可靠角色只是引用同一个
# tag —— 启动列表里没有 app，所以 `up --build` 什么也不会构建（实测：拿旧镜像跑
# 出来的第一个错是 `flag provided but not defined: -migrate`）。
if ! dc build app > "$SMOKE/build.log" 2>&1; then
  echo "构建失败，见 $SMOKE/build.log"; tail -30 "$SMOKE/build.log"; exit 1
fi
if ! dc --profile reliable up -d mysql qdrant minio redis > "$SMOKE/up.log" 2>&1; then
  echo "起栈失败，见 $SMOKE/up.log"; tail -30 "$SMOKE/up.log"; exit 1
fi
ready=0
for i in $(seq 1 60); do
  if mysql_q "SELECT 1" | grep -q '^1$'; then ready=1; break; fi
  sleep 1
done
check "MySQL 可查询" "1" "$ready"
if [ "$ready" != 1 ]; then
  echo "门禁没过，不再往下跑。栈日志见 ${SMOKE}/mysql.log（cleanup 会捞）"; exit 1
fi
# Qdrant/MinIO 必须在角色启动前就绪：jobs 启动时就会 EnsureBucket/
# EnsureCollection，一个缺席的依赖会让角色直接退出。
QDRANT_HOST=${RELIABLE_QDRANT_PORT:-6334}
MINIO_HOST=${RELIABLE_MINIO_PORT:-9011}
vector_ready=0; object_ready=0
for i in $(seq 1 60); do
  if curl -s --max-time 2 "http://localhost:${QDRANT_HOST}/collections" | grep -q '"collections"'; then vector_ready=1; fi
  if curl -s --max-time 2 -o /dev/null -w '%{http_code}' "http://localhost:${MINIO_HOST}/minio/health/live" | grep -q '^200$'; then object_ready=1; fi
  [ "$vector_ready" = 1 ] && [ "$object_ready" = 1 ] && break
  sleep 1
done
check "Qdrant 就绪" "1" "$vector_ready"
check "MinIO 就绪" "1" "$object_ready"
if [ "$vector_ready" != 1 ] || [ "$object_ready" != 1 ]; then
  echo "知识库依赖没起来，不再往下跑。"; exit 1
fi
# Redis is the session-projection store for the jobs role; it needs no host
# port (the roles reach it over the compose network as redis:6379).
redis_ready=0
for i in $(seq 1 30); do
  if [ "$(dc exec -T redis redis-cli ping 2>/dev/null | tr -d '\r')" = "PONG" ]; then redis_ready=1; break; fi
  sleep 1
done
check "Redis 就绪（投影存储）" "1" "$redis_ready"
if [ "$redis_ready" != 1 ]; then echo "Redis 没起来，不再往下跑。"; exit 1; fi

# 迁移是显式的部署动作（不随角色启动执行），bootstrap 创建租户 + 首个 admin。
dc run --rm --no-deps worker-a -migrate -config /config/config.yaml > "$SMOKE/migrate.log" 2>&1 \
  && check "迁移 -migrate 退出码" "0" "0" \
  || check "迁移 -migrate 退出码" "0" "$?"
dc run --rm --no-deps worker-a -bootstrap-admin \
  -bootstrap-admin-tenant "$TENANT" -bootstrap-admin-subject ops@fixture \
  -bootstrap-admin-token fixture-token -config /config/config.yaml > "$SMOKE/bootstrap.log" 2>&1 \
  && check "bootstrap-admin 退出码" "0" "0" \
  || check "bootstrap-admin 退出码" "0" "$?"
check "租户由 bootstrap 创建" "$TENANT" "$(mysql_q "SELECT tenant_id FROM tenants WHERE tenant_id='$TENANT'")"
check "首个 admin 落在 tenant_users" "admin" "$(mysql_q "SELECT role FROM tenant_users WHERE tenant_id='$TENANT'")"
check "迁移落了五版 schema" "5" "$(mysql_q "SELECT COUNT(*) FROM schema_migrations")"

echo "--- B. 控制面夹具（等价于 Admin API 建好的 app/revision/binding） ---"
# 模型 profile 指向栈内的假模型；api_key_ref 是 env 引用，worker 侧靠
# MODEL_API_KEY + RELIABLE_SECRET_ENV_ALLOW 解析（compose 已设）。
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
       8, 120000, SHA2('reliable-e2e-fixture', 256)
FROM agent_apps a WHERE a.tenant_id='$TENANT' AND a.public_id='assistant';

UPDATE agent_apps a
JOIN agent_revisions r ON r.tenant_id = a.tenant_id AND r.app_id = a.app_id
SET a.current_revision_id = r.revision_id
WHERE a.tenant_id='$TENANT' AND a.public_id='assistant';

INSERT INTO channel_bindings (tenant_id, app_id, channel_type, public_id, config)
SELECT '$TENANT', app_id, 'wechat_kf', 'main',
       '{"corp_id":"wwfixture00000001","secret":"kf-fixture-secret","token":"fixture-callback-token","encoding_aes_key":"jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C"}'
FROM agent_apps WHERE tenant_id='$TENANT' AND public_id='assistant';

-- 企微 binding：H 段的真回调与回复投递都靠它（token/aes_key 与上方同一官方向量，
-- 加密 helper 因此只需一套）。
INSERT INTO channel_bindings (tenant_id, app_id, channel_type, public_id, config)
SELECT '$TENANT', app_id, 'wecom', 'main',
       '{"corp_id":"wwfixture00000001","corp_secret":"wecom-fixture-secret","agent_id":218,"token":"fixture-callback-token","encoding_aes_key":"jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C"}'
FROM agent_apps WHERE tenant_id='$TENANT' AND public_id='assistant';

-- webchat binding：H 段的邮箱段（无凭据，本机通道）。
INSERT INTO channel_bindings (tenant_id, app_id, channel_type, public_id, credential_ref)
SELECT '$TENANT', app_id, 'webchat', 'main', 'env:X'
FROM agent_apps WHERE tenant_id='$TENANT' AND public_id='assistant';

-- notification 不再由 SQL 直插：C/D 段的真回调经 gateway 落库（先落库后 ACK）。
SQL
if dc exec -T mysql mysql -h127.0.0.1 -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" < "$SMOKE/fixture.sql" 2>"$SMOKE/fixture.err"; then
  check "夹具导入" "0" "0"
else
  check "夹具导入" "0" "$(cat "$SMOKE/fixture.err")"; exit 1
fi

# 假 KF 的脚本必须在角色启动**之前**就装好。移到这里是被实测教的：脚本原本在 C 段
# 装载，而此刻回调会在下一瞬间发出——被刚起的 jobs 抢先拉走的话，它看到的是空队列，
# 于是把 notification 标 done、一条消息也没拉，后面的断言全部落空。
cat > "$SMOKE/kf_script1.json" <<'JSON'
{"reset":true,"pages":[{"next_cursor":"C1","has_more":0,
  "messages":[{"msgid":"seed-1","external_userid":"ext-seed-1","content":"hello from the reliable e2e"}]}]}
JSON

if ! dc --profile reliable up -d fake-model > "$SMOKE/up-model.log" 2>&1; then
  echo "启动假模型失败，见 $SMOKE/up-model.log"; exit 1
fi
model_ready=0
for i in $(seq 1 30); do
  if curl -s --max-time 2 "$MODEL/healthz" | grep -q "\"mode\""; then model_ready=1; break; fi
  sleep 1
done
check "假 KF 上游就绪" "1" "$model_ready"
if [ "$model_ready" != 1 ]; then echo "假模型没起来，不再往下跑"; exit 1; fi
curl -s -X POST "$MODEL/__kf_script" --data-binary @"$SMOKE/kf_script1.json" > "$SMOKE/kf_script1.out"
has "KF 脚本已装载（1 页）" "$SMOKE/kf_script1.out" '"pages":1'

echo "--- C. 启动角色进程与消息处理 ---"
# 角色在 schema 就绪之后才启动：它们一上来就轮询租户/任务，先于迁移启动会在日志
# 里刷一串 `Table 'tas.tenants' doesn't exist` —— 功能上无害（会重试），但把一个
# 正常时序变成看起来像故障的噪音。
if ! dc --profile reliable up -d worker-a worker-b delivery jobs gateway > "$SMOKE/up-roles.log" 2>&1; then
  echo "启动角色失败，见 $SMOKE/up-roles.log"; tail -30 "$SMOKE/up-roles.log"; exit 1
fi
gateway_ready=0
for i in $(seq 1 30); do
  if curl -s --max-time 2 "$GATEWAY/healthz" | grep -q '"ok"'; then gateway_ready=1; break; fi
  sleep 1
done
check "可靠 gateway 就绪" "1" "$gateway_ready"
if [ "$gateway_ready" != 1 ]; then echo "gateway 没起来，不再往下跑"; exit 1; fi

# 真回调：gateway 验签 → 落 channel_notifications → 才 ACK "success"。
kf_code=$(post_kf_callback "FIXTURE-EVTOK" "wk1")
check "KF 回调 HTTP 200" "200" "$kf_code"
check "KF 回调 ACK=success" "success" "$(cat "$SMOKE/_callback_ack.txt")"
check "真回调落 notification（先落库后 ACK）" "1" \
  "$(mysql_q "SELECT COUNT(*) FROM channel_notifications WHERE tenant_id='$TENANT'")"

if wait_sql "SELECT status FROM inbox_messages WHERE tenant_id='$TENANT' ORDER BY in_seq LIMIT 1" "done" 90; then
  check "消息被 inbox 受理并执行完成" "done" "done"
else
  check "消息被 inbox 受理并执行完成" "done" \
    "$(mysql_q "SELECT status FROM inbox_messages WHERE tenant_id='$TENANT' LIMIT 1")"
fi
wait_sql "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' LIMIT 1" "sent" 60 >/dev/null \
  && check "回复由 delivery 投递成功" "sent" "sent" \
  || check "回复由 delivery 投递成功" "sent" \
     "$(mysql_q "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' LIMIT 1")"

check "只产生一条 inbox 消息" "1" "$(mysql_q "SELECT COUNT(*) FROM inbox_messages WHERE tenant_id='$TENANT'")"
check "只产生一个 execution" "1" "$(mysql_q "SELECT COUNT(*) FROM executions WHERE tenant_id='$TENANT'")"
check "execution 终态 committed" "committed" "$(mysql_q "SELECT status FROM executions WHERE tenant_id='$TENANT' LIMIT 1")"
check "notification 已完成" "done" "$(mysql_q "SELECT status FROM channel_notifications WHERE tenant_id='$TENANT' LIMIT 1")"
check "游标推进到 C1" "C1" "$(mysql_q "SELECT value FROM channel_checkpoints WHERE tenant_id='$TENANT' AND scope_key='wk1'")"
check "会话状态 in_seq|head_seq|version = 1|2|1" "1	2	1" \
  "$(mysql_q "SELECT in_seq, head_seq, session_version FROM sessions WHERE tenant_id='$TENANT' LIMIT 1")"
check "审计留下 model_call + reply" "1	1" \
  "$(mysql_q "SELECT SUM(event='model_call'), SUM(event='reply') FROM audit_events WHERE tenant_id='$TENANT'")"
check "上游模型只被调用一次（两个 worker 竞争的结果）" "1" \
  "$(curl -s "$MODEL/__mode" | python3 -c 'import sys,json;print(json.load(sys.stdin)["requests"])')"

curl -s "$MODEL/__kf_sent" > "$SMOKE/kf_sent1.json"
has "假 KF 收到回复（发件人=external_userid）" "$SMOKE/kf_sent1.json" '"touser":"ext-seed-1"'
has "假 KF 收到回复（open_kfid 来自回复路由）" "$SMOKE/kf_sent1.json" '"open_kfid":"wk1"'
has "回复内容是假模型脚本全文" "$SMOKE/kf_sent1.json" 'Hello world, 内部资料 leaked. (done)'

echo "--- D. 第二条消息：同一会话继续，序号与游标各自前进 ---"
cat > "$SMOKE/kf_script2.json" <<'JSON'
{"pages":[{"next_cursor":"C2","has_more":0,
  "messages":[{"msgid":"seed-2","external_userid":"ext-seed-1","content":"a second turn"}]}]}
JSON
curl -s -X POST "$MODEL/__kf_script" --data-binary @"$SMOKE/kf_script2.json" > "$SMOKE/kf_script2.out"
has "KF 脚本已装载（第二页）" "$SMOKE/kf_script2.out" '"pages":1'

# 第二条真回调：同一 open_kfid、新 event token。
d_code=$(post_kf_callback "FIXTURE-EVTOK-2" "wk1")
check "第二条 KF 回调 HTTP 200" "200" "$d_code"

wait_sql "SELECT COUNT(*) FROM inbox_messages WHERE tenant_id='$TENANT'" "2" 60 \
  && check "第二条消息到达 inbox" "2" "2" \
  || check "第二条消息到达 inbox" "2" "$(mysql_q "SELECT COUNT(*) FROM inbox_messages WHERE tenant_id='$TENANT'")"
wait_sql "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' ORDER BY outbox_id DESC LIMIT 1" "sent" 60 >/dev/null \
  && check "第二条回复已投递" "sent" "sent" \
  || check "第二条回复已投递" "sent" \
     "$(mysql_q "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' ORDER BY outbox_id DESC LIMIT 1")"

check "会话共用一个 session_pk" "1" "$(mysql_q "SELECT COUNT(DISTINCT session_pk) FROM inbox_messages WHERE tenant_id='$TENANT'")"
check "会话状态推进到 2|3|2" "2	3	2" \
  "$(mysql_q "SELECT in_seq, head_seq, session_version FROM sessions WHERE tenant_id='$TENANT' LIMIT 1")"
check "游标推进到 C2" "C2" "$(mysql_q "SELECT value FROM channel_checkpoints WHERE tenant_id='$TENANT' AND scope_key='wk1'")"
check "两个 execution 都提交" "2" "$(mysql_q "SELECT COUNT(*) FROM executions WHERE tenant_id='$TENANT' AND status='committed'")"
check "上游模型共被调用两次" "2" \
  "$(curl -s "$MODEL/__mode" | python3 -c 'import sys,json;print(json.load(sys.stdin)["requests"])')"
check "没有遗留的未投递回复" "0" \
  "$(mysql_q "SELECT COUNT(*) FROM reply_outbox WHERE tenant_id='$TENANT' AND status IN ('pending','unknown','dead')")"

echo "--- D2. 会话投影：Redis 缓存追上 MySQL 事实源 ---"
# 每次提交都入队 session_project；jobs 排空后投影版本应等于 session_version。
# 用 wait 允许投影滞后（它是缓存），并用 python 比较两边的 version——
# 只 grep "version": 2 会把 MySQL 自己的版本号当证据，那是假绿。
cat > "$SMOKE/proj_check.py" <<'PY'
import json,sys
try:
    # dc run mixes compose chatter and the binary banner into stdout before
    # the JSON; the document starts at the first brace.
    raw=open(sys.argv[1]).read()
    d=json.loads(raw[raw.index("{"):])
    p=d.get("projection") or {}
    print(1 if p.get("present") and p.get("version")==d["mysql"]["version"] else 0)
except Exception:
    print(0)
PY
PK=$(mysql_q "SELECT session_pk FROM sessions WHERE tenant_id='$TENANT' LIMIT 1")
proj_file="$SMOKE/session_state.json"
proj_ok=0
for i in $(seq 1 30); do
  dc run --rm --no-deps worker-a -session-state "$PK" -resolve-tenant "$TENANT" \
    -config /config/config.yaml > "$proj_file" 2>&1
  [ "$(python3 "$SMOKE/proj_check.py" "$proj_file")" = "1" ] && proj_ok=1 && break
  sleep 1
done
check "投影存在且版本追上 MySQL（允许滞后，wait 收敛）" "1" "$proj_ok"
has "session-state 输出含 projection 段" "$proj_file" '"projection"'

echo "--- E. 受控工具：revision 固定 HTTP 工具，账本留证 ---"
# 工具段用新会话：存量会话固定 rev1（无工具），发布/回滚只影响新会话——这一步
# 本身就顺手验证了版本固定。
cat > "$SMOKE/fixture-tool.sql" <<SQL
INSERT INTO tool_bindings
  (tenant_id, app_id, name, kind, version, risk_level, side_effect, idempotent,
   spec, input_schema, timeout_ms, status)
SELECT '$TENANT', app_id, 'echo_upstream', 'http', 1, 'low', 'read', 0,
  JSON_OBJECT('method','POST','url','http://fake-model:9009/__tool/echo',
              'allow_hosts', JSON_ARRAY('fake-model'),
              'allow_cidrs', JSON_ARRAY('172.16.0.0/12')),
  CAST('{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}' AS JSON),
  4000, 'active'
FROM agent_apps WHERE tenant_id='$TENANT' AND public_id='assistant';

INSERT INTO agent_revisions
  (tenant_id, app_id, revision_no, instruction, model_profile_id, backend_profile_id,
   max_llm_calls, message_timeout_ms, tools, manifest_hash)
SELECT '$TENANT', a.app_id, 2, 'be terse',
  (SELECT profile_id FROM model_profiles WHERE tenant_id='$TENANT' AND public_id='default-model'),
  (SELECT profile_id FROM backend_profiles WHERE tenant_id='$TENANT' AND public_id='default-backend'),
  8, 120000, CAST('{"pinned":[{"name":"echo_upstream","version":1}]}' AS JSON),
  SHA2('reliable-e2e-tool-revision', 256)
FROM agent_apps a WHERE a.tenant_id='$TENANT' AND a.public_id='assistant';

UPDATE agent_apps a
JOIN agent_revisions r ON r.tenant_id = a.tenant_id AND r.app_id = a.app_id
SET a.current_revision_id = r.revision_id
WHERE a.tenant_id='$TENANT' AND a.public_id='assistant' AND r.revision_no = 2;
SQL
if dc exec -T mysql mysql -h127.0.0.1 -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" < "$SMOKE/fixture-tool.sql" 2>"$SMOKE/fixture-tool.err"; then
  check "工具夹具导入（binding + revision 2）" "0" "0"
else
  check "工具夹具导入" "0" "$(cat "$SMOKE/fixture-tool.err")"; exit 1
fi

cat > "$SMOKE/tool_mode.json" <<'JSON'
{"mode":"tools","tool":"echo_upstream","tool_args":"{\"text\":\"ping-from-tool\"}"}
JSON
curl -s -X POST "$MODEL/__mode" --data-binary @"$SMOKE/tool_mode.json" > "$SMOKE/tool_mode.out"
has "假模型切到工具模式" "$SMOKE/tool_mode.out" '"mode":"tools"'
curl -s -X POST "$MODEL/__tool/reset" > /dev/null

cat > "$SMOKE/kf_script3.json" <<'JSON'
{"pages":[{"next_cursor":"C3","has_more":0,
  "messages":[{"msgid":"seed-3","external_userid":"ext-tool-1","content":"use the tool please"}]}]}
JSON
curl -s -X POST "$MODEL/__kf_script" --data-binary @"$SMOKE/kf_script3.json" > "$SMOKE/kf_script3.out"
has "KF 脚本已装载（工具会话）" "$SMOKE/kf_script3.out" '"pages":1'

e_code=$(post_kf_callback "FIXTURE-EVTOK-3" "wk1")
check "工具会话回调 HTTP 200（第 3 条真回调）" "200" "$e_code"

wait_sql "SELECT status FROM tool_calls WHERE tenant_id='$TENANT' AND tool_name='echo_upstream' ORDER BY call_id DESC LIMIT 1" "succeeded" 90 \
  && check "工具调用在账本里落 succeeded" "succeeded" "succeeded" \
  || check "工具调用在账本里落 succeeded" "succeeded" \
     "$(mysql_q "SELECT status FROM tool_calls WHERE tenant_id='$TENANT' AND tool_name='echo_upstream' LIMIT 1")"
check "账本记录 kind=http 且只尝试一次（read 无重试）" "http	1" \
  "$(mysql_q "SELECT tool_kind, attempts FROM tool_calls WHERE tenant_id='$TENANT' AND tool_name='echo_upstream' LIMIT 1")"
check "物理尝试行记了 200" "succeeded	200" \
  "$(mysql_q "SELECT a.status, a.http_status FROM tool_call_attempts a JOIN tool_calls c ON c.call_id=a.call_id AND c.tenant_id=a.tenant_id WHERE a.tenant_id='$TENANT' AND c.tool_name='echo_upstream' LIMIT 1")"

curl -s "$MODEL/__tool/sent" > "$SMOKE/tool_sent1.json"
has "工具目标只被调用一次" "$SMOKE/tool_sent1.json" '"hits":1'
has "工具目标收到的参数来自模型" "$SMOKE/tool_sent1.json" 'ping-from-tool'

wait_sql "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' ORDER BY outbox_id DESC LIMIT 1" "sent" 60 >/dev/null \
  && check "工具轮最终回复已投递" "sent" "sent" \
  || check "工具轮最终回复已投递" "sent" \
     "$(mysql_q "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' ORDER BY outbox_id DESC LIMIT 1")"
check "最终回复是模型看到工具结果后的话" "tool round complete" \
  "$(mysql_q "SELECT text FROM reply_outbox WHERE tenant_id='$TENANT' ORDER BY outbox_id DESC LIMIT 1" | head -c 19)"
check "工具会话未被阻塞" "0" \
  "$(mysql_q "SELECT COUNT(*) FROM sessions WHERE tenant_id='$TENANT' AND actor_key='ext-tool-1' AND blocked_reason IS NOT NULL")"

curl -s "$MODEL/__kf_sent" > "$SMOKE/kf_sent3.json"
has "KF 投递到工具会话的回复" "$SMOKE/kf_sent3.json" '"touser":"ext-tool-1"'
has "工具轮回复文本" "$SMOKE/kf_sent3.json" 'tool round complete'

echo "--- F. 工具结果不确定：阻断、人工处置、重跑 ---"
# write 非幂等 + 上游挂起 = 外部副作用不确定。平台必须进入 unknown 阻断，而不是重试。
cat > "$SMOKE/fixture-tool2.sql" <<SQL
INSERT INTO tool_bindings
  (tenant_id, app_id, name, kind, version, risk_level, side_effect, idempotent,
   spec, input_schema, timeout_ms, status)
SELECT '$TENANT', app_id, 'charge_upstream', 'http', 1, 'low', 'write', 0,
  JSON_OBJECT('method','POST','url','http://fake-model:9009/__tool/echo',
              'allow_hosts', JSON_ARRAY('fake-model'),
              'allow_cidrs', JSON_ARRAY('172.16.0.0/12')),
  CAST('{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}' AS JSON),
  1500, 'active'
FROM agent_apps WHERE tenant_id='$TENANT' AND public_id='assistant';

INSERT INTO agent_revisions
  (tenant_id, app_id, revision_no, instruction, model_profile_id, backend_profile_id,
   max_llm_calls, message_timeout_ms, tools, manifest_hash)
SELECT '$TENANT', a.app_id, 3, 'be terse',
  (SELECT profile_id FROM model_profiles WHERE tenant_id='$TENANT' AND public_id='default-model'),
  (SELECT profile_id FROM backend_profiles WHERE tenant_id='$TENANT' AND public_id='default-backend'),
  8, 120000, CAST('{"pinned":[{"name":"charge_upstream","version":1}]}' AS JSON),
  SHA2('reliable-e2e-charge-revision', 256)
FROM agent_apps a WHERE a.tenant_id='$TENANT' AND a.public_id='assistant';

UPDATE agent_apps a
JOIN agent_revisions r ON r.tenant_id = a.tenant_id AND r.app_id = a.app_id
SET a.current_revision_id = r.revision_id
WHERE a.tenant_id='$TENANT' AND a.public_id='assistant' AND r.revision_no = 3;
SQL
dc exec -T mysql mysql -h127.0.0.1 -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" < "$SMOKE/fixture-tool2.sql" 2>/dev/null

cat > "$SMOKE/tool_mode2.json" <<'JSON'
{"mode":"tools","tool":"charge_upstream","tool_args":"{\"text\":\"99\"}"}
JSON
curl -s -X POST "$MODEL/__mode" --data-binary @"$SMOKE/tool_mode2.json" > /dev/null
curl -s -X POST "$MODEL/__tool/mode" -H 'Content-Type: application/json' -d '{"mode":"hang"}' > "$SMOKE/tool_hang.out"
has "工具目标切到挂起模式" "$SMOKE/tool_hang.out" '"mode":"hang"'

cat > "$SMOKE/kf_script4.json" <<'JSON'
{"pages":[{"next_cursor":"C4","has_more":0,
  "messages":[{"msgid":"seed-4","external_userid":"ext-tool-2","content":"charge my card"}]}]}
JSON
curl -s -X POST "$MODEL/__kf_script" --data-binary @"$SMOKE/kf_script4.json" > "$SMOKE/kf_script4.out"
has "KF 脚本已装载（charge 会话）" "$SMOKE/kf_script4.out" '"pages":1'
f_code=$(post_kf_callback "FIXTURE-EVTOK-4" "wk1")
check "charge 会话回调 HTTP 200（第 4 条真回调）" "200" "$f_code"

wait_sql "SELECT COUNT(*) FROM sessions WHERE tenant_id='$TENANT' AND actor_key='ext-tool-2' AND blocked_reason LIKE 'tool %'" "1" 90 \
  && check "不确定的写调用阻断了会话" "1" "1" \
  || check "不确定的写调用阻断了会话" "1" \
     "$(mysql_q "SELECT COUNT(*) FROM sessions WHERE tenant_id='$TENANT' AND actor_key='ext-tool-2' AND blocked_reason LIKE 'tool %'")"
check "账本与阻断一致：unknown 且未重试" "unknown	1" \
  "$(mysql_q "SELECT status, attempts FROM tool_calls WHERE tenant_id='$TENANT' AND tool_name='charge_upstream' LIMIT 1")"
check "该 execution 停留在 unknown" "unknown" \
  "$(mysql_q "SELECT e.status FROM executions e JOIN sessions s ON s.session_pk=e.session_pk AND s.tenant_id=e.tenant_id WHERE s.tenant_id='$TENANT' AND s.actor_key='ext-tool-2' LIMIT 1")"
check "队头未推进，消息等待处置" "1" \
  "$(mysql_q "SELECT head_seq FROM sessions WHERE tenant_id='$TENANT' AND actor_key='ext-tool-2'")"
check "阻断期间不向用户发任何回复" "0" \
  "$(mysql_q "SELECT COUNT(*) FROM reply_outbox r JOIN sessions s ON s.session_pk=r.session_pk AND s.tenant_id=r.tenant_id WHERE s.tenant_id='$TENANT' AND s.actor_key='ext-tool-2'")"

dc run --rm --no-deps worker-a -list-blocked -resolve-tenant "$TENANT" -config /config/config.yaml > "$SMOKE/list_blocked.out" 2>&1 \
  && check "-list-blocked 退出码" "0" "0" \
  || check "-list-blocked 退出码" "0" "$?"
has "-list-blocked 报出未决调用" "$SMOKE/list_blocked.out" '"tool_name": "charge_upstream"'

# 上游恢复健康 + 人工处置 cancelled（副作用未发生）→ 消息可重跑。
PK2=$(mysql_q "SELECT session_pk FROM sessions WHERE tenant_id='$TENANT' AND actor_key='ext-tool-2'")
check "charge 会话可定位" "1" "$(mysql_q "SELECT COUNT(*) FROM sessions WHERE tenant_id='$TENANT' AND actor_key='ext-tool-2'")"
curl -s -X POST "$MODEL/__tool/mode" -H 'Content-Type: application/json' -d '{"mode":"ok"}' > /dev/null
dc run --rm --no-deps worker-a -resolve-session "$PK2" -resolution cancelled \
  -resolve-by operator:fixture -resolve-tenant "$TENANT" -config /config/config.yaml > "$SMOKE/resolve.out" 2>&1 \
  && check "-resolve-session 退出码" "0" "0" \
  || check "-resolve-session 退出码" "0" "$?"
has "处置结果：取消并解锁" "$SMOKE/resolve.out" '"unblocked": true'
has "处置结果：取消（消息将重跑）" "$SMOKE/resolve.out" '"disposition": "cancelled"'

wait_sql "SELECT status FROM executions WHERE tenant_id='$TENANT' AND session_pk=$PK2" "committed" 90 \
  && check "重跑后 execution 提交" "committed" "committed" \
  || check "重跑后 execution 提交" "committed" \
     "$(mysql_q "SELECT status FROM executions WHERE tenant_id='$TENANT' AND session_pk=$PK2")"
check "重跑后队头推进到 2" "2" "$(mysql_q "SELECT head_seq FROM sessions WHERE tenant_id='$TENANT' AND session_pk=$PK2")"
check "账本留下两段历史：已处置的 unknown + 成功" "1	1" \
  "$(mysql_q "SELECT SUM(status='unknown' AND resolution='cancelled'), SUM(status='succeeded') FROM tool_calls WHERE tenant_id='$TENANT' AND tool_name='charge_upstream'")"
check "处置审计留痕" "1" \
  "$(mysql_q "SELECT COUNT(*) FROM audit_events WHERE tenant_id='$TENANT' AND event='tool_resolved' AND decision='cancelled'")"
wait_sql "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' ORDER BY outbox_id DESC LIMIT 1" "sent" 60 >/dev/null \
  && check "重跑后的最终回复已投递" "sent" "sent" \
  || check "重跑后的最终回复已投递" "sent" \
     "$(mysql_q "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' ORDER BY outbox_id DESC LIMIT 1")"
check "没有遗留的未投递回复（工具段结束）" "0" \
  "$(mysql_q "SELECT COUNT(*) FROM reply_outbox WHERE tenant_id='$TENANT' AND status IN ('pending','unknown','dead')")"

echo "--- G. 知识库：三类文档与问答引用 ---"
# KB + knowledge_search 绑定 + revision 4（含 kb pin）。夹具走 SQL 与 B/E 段同构，
# 内容与 Admin API 发布出来的一致。
cat > "$SMOKE/fixture-kb.sql" <<SQL
INSERT INTO knowledge_bases (tenant_id, app_id, public_id, name, status, embedding_model, embedding_dim)
SELECT '$TENANT', app_id, 'docs', 'Docs', 'active', 'fake-embed', 64
FROM agent_apps WHERE tenant_id='$TENANT' AND public_id='assistant';

INSERT INTO tool_bindings
  (tenant_id, app_id, name, kind, version, risk_level, side_effect, idempotent,
   spec, input_schema, timeout_ms, status)
SELECT '$TENANT', app_id, 'knowledge_search', 'go', 1, 'low', 'read', 0,
  CAST('{}' AS JSON), CAST('{"type":"object"}' AS JSON), 15000, 'active'
FROM agent_apps WHERE tenant_id='$TENANT' AND public_id='assistant';

INSERT INTO agent_revisions
  (tenant_id, app_id, revision_no, instruction, model_profile_id, backend_profile_id,
   max_llm_calls, message_timeout_ms, tools, knowledge_bases, manifest_hash)
SELECT '$TENANT', a.app_id, 4, 'be terse',
  (SELECT profile_id FROM model_profiles WHERE tenant_id='$TENANT' AND public_id='default-model'),
  (SELECT profile_id FROM backend_profiles WHERE tenant_id='$TENANT' AND public_id='default-backend'),
  8, 120000,
  CAST('{"pinned":[{"name":"knowledge_search","version":1}]}' AS JSON),
  CAST('{"pinned":[{"public_id":"docs"}]}' AS JSON),
  SHA2('reliable-e2e-kb-revision', 256)
FROM agent_apps a WHERE a.tenant_id='$TENANT' AND a.public_id='assistant';

INSERT INTO knowledge_bindings (tenant_id, revision_id, kb_id)
SELECT '$TENANT', r.revision_id, kb.kb_id
FROM agent_revisions r
JOIN knowledge_bases kb ON kb.tenant_id = r.tenant_id AND kb.public_id = 'docs'
WHERE r.tenant_id='$TENANT' AND r.revision_no = 4;

UPDATE agent_apps a
JOIN agent_revisions r ON r.tenant_id = a.tenant_id AND r.app_id = a.app_id
SET a.current_revision_id = r.revision_id
WHERE a.tenant_id='$TENANT' AND a.public_id='assistant' AND r.revision_no = 4;
SQL
if dc exec -T mysql mysql -h127.0.0.1 -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" < "$SMOKE/fixture-kb.sql" 2>"$SMOKE/fixture-kb.err"; then
  check "知识库夹具导入（kb + binding + revision 4）" "0" "0"
else
  check "知识库夹具导入" "0" "$(cat "$SMOKE/fixture-kb.err")"; exit 1
fi

# 三类文档（md/txt/csv），各自一个独特关键词。文件放在 $SMOKE，run 时挂到 /seed。
cat > "$SMOKE/alpha.md" <<'EOF'
# Alpha 服务手册

alpha rollout checklist：先灰度量，再全量，回滚开关在 alpha-switch。
EOF
cat > "$SMOKE/beta.txt" <<'EOF'
beta 运维约定：beta 集群每周三做演练，告警走 beta-oncall。
EOF
cat > "$SMOKE/gamma.csv" <<'EOF'
service,window,owner
gamma,02:00-04:00,gamma-team
EOF
seed_ids=""
for spec in "alpha.md:aaaaaaaa-1111-1111-1111-111111111111:Alpha 手册:text/markdown" \
            "beta.txt:bbbbbbbb-1111-1111-1111-111111111111:Beta 约定:text/plain" \
            "gamma.csv:cccccccc-1111-1111-1111-111111111111:Gamma 表:text/csv"; do
  file=${spec%%:*}; rest=${spec#*:}
  docid=${rest%%:*}; rest=${rest#*:}
  title=${rest%%:*}; mime=${rest#*:}
  if dc run --rm --no-deps -v "$PWD/$SMOKE:/seed:ro" worker-a \
       -ingest-doc "/seed/$file" -resolve-tenant "$TENANT" -ingest-kb docs \
       -ingest-doc-id "$docid" -ingest-title "$title" -ingest-mime "$mime" \
       -config /config/config.yaml > "$SMOKE/ingest-$file.log" 2>&1; then
    check "上传 $file" "0" "0"
  else
    check "上传 $file" "0" "$(tail -2 "$SMOKE/ingest-$file.log")"; exit 1
  fi
done

wait_sql "SELECT COUNT(*) FROM documents WHERE tenant_id='$TENANT' AND status='ready'" "3" 180 \
  && check "三类文档全部索引到 ready" "3" "3" \
  || check "三类文档全部索引到 ready" "3" \
     "$(mysql_q "SELECT GROUP_CONCAT(status) FROM documents WHERE tenant_id='$TENANT'")"
check "chunk 已切块入库" "3" \
  "$(mysql_q "SELECT COUNT(*) FROM documents WHERE tenant_id='$TENANT' AND chunk_count > 0")"
check "向量数量与 chunk 一致" "$(mysql_q "SELECT COALESCE(SUM(chunk_count),0) FROM documents WHERE tenant_id='$TENANT'")" \
  "$(mysql_q "SELECT COUNT(*) FROM document_chunks WHERE tenant_id='$TENANT'")"
check "embedding 端点被调用过" "1" \
  "$(curl -s "$MODEL/__mode" | python3 -c 'import sys,json;print(1 if json.load(sys.stdin)["embeddings"]>0 else 0)')"
echo "知识段结束：upload→index→ready 管道验证完毕；tool call 验证在集成测试中"
check "没有遗留的未投递回复（知识段结束）" "0" \
  "$(mysql_q "SELECT COUNT(*) FROM reply_outbox WHERE tenant_id='$TENANT' AND status IN ('pending','unknown','dead')")"

echo "--- H. 可靠 gateway：webchat 邮箱与企微投递 ---"
# webchat：接收端持久化（落库在 202 之前）；回复走 outbox → delivery 确认（webchat
# 的 sent 只是"可推送"）→ 邮箱把 sent 且未推送的行推给 SSE 连接并标记 pushed_at。
cat > "$SMOKE/webchat_msg.json" <<'JSON'
{"user":"webuser","msg_id":"web-1","text":"hello from webchat e2e"}
JSON
web_code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
  "$GATEWAY/callback/webchat/$TENANT" --data-binary @"$SMOKE/webchat_msg.json")
check "webchat 回调 202" "202" "$web_code"
wait_sql "SELECT COUNT(*) FROM sessions WHERE tenant_id='$TENANT' AND actor_key='webuser'" "1" 30 \
  && check "webchat 消息进入 inbox（会话建立）" "1" "1" \
  || check "webchat 消息进入 inbox（会话建立）" "1" \
     "$(mysql_q "SELECT COUNT(*) FROM sessions WHERE tenant_id='$TENANT' AND actor_key='webuser'")"
wait_sql "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' AND channel_type='webchat' LIMIT 1" "sent" 60 >/dev/null \
  && check "webchat 回复由 delivery 确认 sent" "sent" "sent" \
  || check "webchat 回复由 delivery 确认 sent" "sent" \
     "$(mysql_q "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' AND channel_type='webchat' LIMIT 1")"
# 连 4 秒（邮箱每秒轮询一次；已 sent 未推送的行会被补推）。curl 超时退出，吞掉。
curl -sN --max-time 4 "$GATEWAY/webchat/stream?tenant=$TENANT&user=webuser" > "$SMOKE/webchat_sse.log" 2>&1 || true
has "webchat SSE 收到 done 帧" "$SMOKE/webchat_sse.log" '"done":true'
check "webchat 回复已标记 pushed_at" "1" \
  "$(mysql_q "SELECT COUNT(*) FROM reply_outbox WHERE tenant_id='$TENANT' AND channel_type='webchat' AND pushed_at IS NOT NULL")"

# 企微：真回调 → inbox → worker → delivery 经企微 sender 投到假上游并记录。
wecom_code=$(post_wecom_callback "ext-wecom-1" "hello from wecom e2e" 4561255354251345929)
check "企微回调 HTTP 200" "200" "$wecom_code"
check "企微回调 ACK=success" "success" "$(cat "$SMOKE/_callback_ack.txt")"
wait_sql "SELECT COUNT(*) FROM sessions WHERE tenant_id='$TENANT' AND actor_key='ext-wecom-1'" "1" 30 \
  && check "企微消息进入 inbox（会话建立）" "1" "1" \
  || check "企微消息进入 inbox（会话建立）" "1" \
     "$(mysql_q "SELECT COUNT(*) FROM sessions WHERE tenant_id='$TENANT' AND actor_key='ext-wecom-1'")"
wait_sql "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' AND channel_type='wecom' LIMIT 1" "sent" 60 >/dev/null \
  && check "企微回复由 delivery 投递成功" "sent" "sent" \
  || check "企微回复由 delivery 投递成功" "sent" \
     "$(mysql_q "SELECT status FROM reply_outbox WHERE tenant_id='$TENANT' AND channel_type='wecom' LIMIT 1")"
curl -s "$MODEL/__wecom_sent" > "$SMOKE/wecom_sent.json"
has "假企微收到回复（收件人=FromUserName）" "$SMOKE/wecom_sent.json" '"touser":"ext-wecom-1"'
check "没有遗留的未投递回复（本段结束）" "0" \
  "$(mysql_q "SELECT COUNT(*) FROM reply_outbox WHERE tenant_id='$TENANT' AND status IN ('pending','unknown','dead')")"

printf '\n=== %d PASS / %d FAIL ===\n' "$pass" "$fail"
if [ "$fail" != 0 ]; then
  echo "RELIABLE E2E FAIL"
  [ "$KEEP" = "1" ] || echo "（栈已拆；日志与表状态在 ${SMOKE}/）"
  exit 1
fi
echo "RELIABLE E2E PASS"
