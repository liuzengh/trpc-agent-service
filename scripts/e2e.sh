#!/bin/bash
# 全链路联调：对一个**已经在跑**的地址做端到端断言（spec §2.6）。
#
# 与 .smoke/fake_model_e2e.sh 的分工：那个脚本自己拉起两个进程，验的是六种故障
# 注入模式在真链路上的行为（dep7 的产物）；这个脚本不管进程怎么起的 —— compose
# 栈、裸进程、还是别人起好的都行 —— 只验「一个部署好的平台，从 IM 回调到审计留痕
# 这条链是不是通的」。故障注入与容器编排属 scripts/fault_drill.sh。
#
# 它需要一个**假模型**上游。这不是偷懒：下面一半的断言钉在假模型那段固定脚本上
# （四个 chunk、「内部资料」跨 chunk 2/3 切分），换成真实供应商就没有可断言的对象。
# 探测不到就明确失败并说清怎么办，不降级成一个假装通过的 basic 模式。
#
# 三条纪律从 dep7 继承，都是被失败教出来的：
#   1. 先过就绪门禁再断言。没有门禁的断言可以因为完全无关的原因变绿（上一版第一条
#      消息打在 connection refused 上，却因为 ok 模式的回复也刚好耗时 2s 而「通过」）；
#   2. 断言 helper 自己先过逐例签名自检。数总数会假绿：fixture 写不出来时 4对/4错
#      刚好凑成期望值，工具全坏了却报 PASS；
#   3. JSON payload 一律 heredoc 落文件 + --data-binary @file，不在 "$( ... )" 里内联
#      带 \" 的 JSON —— bash 3.2 在「命令替换作为函数实参」这个位置会解坏内层引号
#      （事实 #18），且它在赋值形态下不发作，所以隔离测试全绿、只有真脚本会红。
#   4. 审计断言只看**本次运行新增**的行（AUDIT_BASE + tail）。审计落在命名卷上，
#      down 之后仍在，所以对着跑过一轮的栈再跑，上一轮的 stage=input 行会让 grep 直接
#      通过 —— 与 msg_id 需要 RUN 前缀是同一个陷阱的两面：历史数据冒充本次结果。
#
# 还有一条不属于纪律而属于事实：默认 compose 配置没有 telemetry 段，tracing 是关的，
# 于是审计里 trace_id 全为空 —— 这是 channels.go:200 traceIDOf 的刻意行为（noop
# provider 会渲染成全零，写进每一行是纯噪音），不是链路断了。所以 §E 在没开 tracing
# 的栈上记 SKIP 而不是 PASS，要拿到「全链路 trace_id」的证据就得开
# telemetry.traces.exporter: stdout（不需要 collector 镜像）再跑一遍，或用 --strict
# 让 SKIP 直接算失败。
#
# 变量名紧邻中文时必须写成 ${VAR}：bash 3.2 在非 UTF-8 locale 下把多字节字符的首字节
# 当成标识符的一部分（isalnum(0xEF) 为真），"$VAR（" 会去查一个叫 VAR\xef 的变量，
# set -u 直接判未绑定退出 —— 与事实 #18 同一家族。
#
# 会改动被测部署的一处：装输出 tripwire（§2.4 里 compose 配置故意不预置它，否则
# 每条基线回复都被截断，就没有「健康」样本可对照了）。跑完必须还原，还原挂在 trap
# 上，中途失败也不会把租户留在截断状态。注意任何一次 admin 写入都会走 commit 路径
# 重新 marshal 整个配置文件，**注释全部丢失**（事实 #19）—— 所以别拿它对着仓库里
# 那份 deploy/compose/config 跑，用一次性副本（fault_drill.sh 就是这么做的）。
set -u
cd "$(dirname "$0")/.."
STRICT=0
[ "${1:-}" = "--strict" ] && STRICT=1
SMOKE=.smoke/e2e
rm -rf "$SMOKE"; mkdir -p "$SMOKE"

BASE=${E2E_BASE:-http://localhost:8080}
MODEL=${E2E_MODEL:-http://localhost:9009}
TENANT=${E2E_TENANT:-demo}
BYSTANDER=${E2E_BYSTANDER:-demo-alt}   # 只用来断言租户列表里有两个（D7 需要）
CONTAINER=${E2E_CONTAINER:-tas-app}    # 审计文件在容器卷里时用它 docker cp
RUN=$$                                 # msg_id 前缀，见 send() 的注释
ORIG=$SMOKE/tenant.orig.json

pass=0; fail=0; skip=0; selftest=0

check() { # check <描述> <期望> <实测>
  if [ "$2" = "$3" ]; then pass=$((pass+1)); [ "$selftest" = 0 ] && printf 'PASS  %-46s %s\n' "$1" "$3"
  else fail=$((fail+1)); [ "$selftest" = 0 ] && printf 'FAIL  %-46s want=[%s] got=[%s]\n' "$1" "$2" "$3"; fi
}
has()   { if grep -q "$3" "$2"; then check "$1" "yes" "yes"; else check "$1" "yes" "no（$2 里没有 $3）"; fi; }
hasnt() { if grep -q "$3" "$2"; then check "$1" "no" "yes（$2 里出现了 $3）"; else check "$1" "no" "no"; fi; }
within() { # within <描述> <值> <下限> <上限> —— 秒，区间 [lo,hi)
  check "$1" "yes" "$(python3 -c "print('yes' if $3<=$2<$4 else 'no($2)')")"
}
num_gt() { if [ -n "${2:-}" ] && [ "$2" -gt "$3" ] 2>/dev/null; then check "$1" "yes" "yes"
  else check "$1" "yes" "no（got='${2:-}' want>'$3'）"; fi; }
calls() { curl -s "$MODEL/__mode" | python3 -c 'import sys,json;print(json.load(sys.stdin)["requests"])'; }
skipn() { # skipn <描述> <原因> —— 没拿到证据，不算通过也不算失败，但必须看得见
  skip=$((skip + 1)); printf 'SKIP  %-46s %s\n' "$1" "$2"; }

# --- helper 自检：逐例签名，不是数总数（纪律 2）---
selftest=1
FIX=$SMOKE/_selftest.txt
printf 'needle\n' > "$FIX" || { echo "自检 fixture 写不出来：$FIX"; exit 1; }
[ -s "$FIX" ] || { echo "自检 fixture 为空：$FIX"; exit 1; }
sig=""
st() { local p0=$pass f0=$fail; "$@"
       if [ "$pass" -gt "$p0" ]; then sig="${sig}P"; elif [ "$fail" -gt "$f0" ]; then sig="${sig}F"
       else sig="${sig}?"; fi; }
st has     "_st" "$FIX" 'needle'   # 存在 → P
st hasnt   "_st" "$FIX" 'needle'   # 存在 → F
st has     "_st" "$FIX" 'absent'   # 不存在 → F
st hasnt   "_st" "$FIX" 'absent'   # 不存在 → P
st check   "_st" 'a' 'a'           # P
st check   "_st" 'a' 'b'           # F
st within  "_st" 1.5 1.0 2.0       # 区间内 → P
st within  "_st" 2.5 1.0 2.0       # 区间外 → F
st num_gt  "_st" 5 3               # P
st num_gt  "_st" 1 3               # F
rm -f "$FIX"
selftest=0; pass=0; fail=0
WANT_SIG=PFFPPFPFPF
if [ "$sig" = "$WANT_SIG" ]; then
  printf 'PASS  %-46s %s\n' "断言 helper 自检（逐例签名）" "$sig"
else
  printf 'FAIL  %-46s want=[%s] got=[%s]\n' "断言 helper 自检" "$WANT_SIG" "$sig"
  echo "断言工具不可信，后面的结果全部作废。"; exit 1
fi

for t in curl python3; do
  command -v "$t" >/dev/null 2>&1 || { echo "缺 ${t}，跑不了。"; exit 1; }
done

# --- 就绪门禁（纪律 1）：三个都要在听，否则后面全是 connection refused ---
code() { curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$1"; }
for i in $(seq 1 30); do
  [ "$(code "$BASE/readyz")" = 200 ] && [ "$(code "$MODEL/healthz")" = 200 ] && break
  sleep 0.5
done
check "平台 /healthz 在听" "200" "$(code "$BASE/healthz")"
check "平台 /readyz 就绪"  "200" "$(code "$BASE/readyz")"
check "假模型 /healthz 在听" "200" "$(code "$MODEL/healthz")"
if [ "$fail" != 0 ]; then
  cat <<MSG

门禁没过，不再往下跑。E2E_BASE=$BASE E2E_MODEL=$MODEL
起栈：  CONFIG_DIR=<一次性副本目录> docker compose up -d --build
或直接：docker compose up -d --build   （会改动仓库里那份配置，见脚本头）
MSG
  exit 1
fi

# 审计轨迹：本机文件优先，其次从容器卷里 docker cp（scratch 里没有 shell，
# docker compose exec 什么也跑不了；macOS 上 volume inspect 给的 Mountpoint 是
# VM 内部路径，宿主机读不到 —— 两条都在 dep8 实测过）。取不到就明确说取不到。
AUDIT=$SMOKE/audit.jsonl
audit_dump() {
  if [ -n "${E2E_AUDIT:-}" ] && [ -r "${E2E_AUDIT:-}" ]; then cp "$E2E_AUDIT" "$AUDIT"; return 0; fi
  if command -v docker >/dev/null 2>&1 \
     && docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$CONTAINER"; then
    docker cp "$CONTAINER:/data/audit.jsonl" "$AUDIT" >/dev/null 2>&1 && return 0
  fi
  return 1
}
if audit_dump; then HAVE_AUDIT=1; else HAVE_AUDIT=0; fi
NEW=$SMOKE/audit.new.jsonl
AUDIT_BASE=0
[ "$HAVE_AUDIT" = 1 ] && AUDIT_BASE=$(wc -l < "$AUDIT" | tr -d ' ')
audit_new() { # 取回审计并截出本次运行新增的那些行（纪律 4）
  audit_dump || return 1
  tail -n "+$((AUDIT_BASE + 1))" "$AUDIT" > "$NEW"
}
NOAUDIT="取不到审计文件（E2E_AUDIT 未设且容器 ${CONTAINER} 不在跑）"

# setmode <json> —— 注入并**回读校验**。上一版的静默失败就出在这里：模式没设进去，
# 断言却因为另一个原因变绿。
setmode() {
  local got want real
  got=$(curl -s -X POST "$MODEL/__mode" -d "$1")
  want=$(python3 -c "import json,sys;print(json.loads(sys.argv[1])['mode'])" "$1")
  real=$(printf '%s' "$got" | python3 -c 'import sys,json;print(json.load(sys.stdin)["mode"])' 2>/dev/null)
  check "注入模式 ${want}（已回读校验）" "$want" "${real:-<读不回来>}"
  [ "$real" = "$want" ]
}

# send <user> <text> [tenant] -> 打印耗时；SSE 落在 $SMOKE/sse_<user>.log
# msg_id 带 RUN 前缀：dedup 是**进程内**结构（事实 #7），同一个 id 第二次投递会被
# 静默 ACK。所以对着一个长期在跑的栈重复执行本脚本时，不带前缀就会「通过」一条
# 根本没被执行的消息 —— 那正是 D3 要验的机制，反过来咬测试自己。
send() {
  # 声明与使用不能写在同一个 local 里：bash 先展开整串实参、再逐个赋值，
  # "$SMOKE/sse_$u.log" 展开时 u 还未绑定，set -u 直接判未绑定（bash 3.2 实测）。
  local u=$1 txt=$2 t=${3:-$TENANT} c t0 t1
  local log="$SMOKE/sse_$u.log"
  : > "$log"
  curl -sN "$BASE/webchat/stream?tenant=$t&user=$u" > "$log" 2>&1 &
  c=$!
  sleep 0.4   # 等 SSE 注册完，否则回复发出去时还没有订阅者
  cat > "$SMOKE/req.json" <<JSON
{"user":"$u","msg_id":"e2e-$RUN-$u","text":"$txt"}
JSON
  # 计时起点在 POST 之前取，而且必须用赋值形态：事实 #18 只咬「命令替换作为函数
  # 实参」那个位置，函数体内的 t0=$( ... ) 是安全的。
  t0=$(python3 -c 'import time;print(time.time())')
  curl -s -o "$SMOKE/cb.out" -w '%{http_code}' -X POST "$BASE/callback/webchat/$t" \
    --data-binary @"$SMOKE/req.json" > "$SMOKE/cb.code"
  for _ in $(seq 1 300); do
    grep -q '"done":true' "$log" 2>/dev/null && break
    sleep 0.1
  done
  t1=$(python3 -c 'import time;print(time.time())')
  # 显式收尸。不 wait 的话 shell 会把「Terminated: 15 curl -sN ...」异步打到 stderr，
  # 混在断言输出里像一条失败（而且它长得不像任何 helper 的输出格式，更容易误读）。
  kill "$c" 2>/dev/null
  wait "$c" 2>/dev/null
  python3 -c "print(f'{$t1-$t0:.2f}')"
}

echo "--- A. 拓扑与健康 ---"
curl -s "$BASE/admin/tenants" > "$SMOKE/tenants.json"
has "租户列表里有 $TENANT"        "$SMOKE/tenants.json" "\"$TENANT\""
has "租户列表里有 ${BYSTANDER}（D7 的旁观者）" "$SMOKE/tenants.json" "\"$BYSTANDER\""
has "default_tenant = $TENANT"    "$SMOKE/tenants.json" "\"default_tenant\":\"$TENANT\""
curl -s "$BASE/healthz" > "$SMOKE/hz.json"; curl -s "$BASE/readyz" > "$SMOKE/rz.json"
check "/healthz 体" '{"status":"ok"}'    "$(cat "$SMOKE/hz.json")"
check "/readyz 体"  '{"status":"ready"}' "$(cat "$SMOKE/rz.json")"
setmode '{"mode":"ok","reset":true}'
send probe "ping" > /dev/null
cbcode=$(cat "$SMOKE/cb.code")
check "回调端点收消息（202 已受理，异步执行）" "202" "$cbcode"

echo "--- B. 正常对话：逐 chunk 到达，终态 done ---"
setmode '{"mode":"ok","reset":true}'
el=$(send ok "hello")
has "chunk 1 到达"                 "$SMOKE/sse_ok.log" '"text":"Hello "'
has "chunk 2 到达（含半截关键词）" "$SMOKE/sse_ok.log" 'world, 内部'
has "chunk 3 到达"                 "$SMOKE/sse_ok.log" '资料 leaked.'
has "chunk 4 到达"                 "$SMOKE/sse_ok.log" '(done)'
has "终态 done"                    "$SMOKE/sse_ok.log" '"done":true'
within "ok 模式耗时 < 1s"           "$el" 0 1.0
check "一条消息 = 一次上游调用"     "1" "$(calls)"

echo "--- C. 输入 guardrail：拦在模型调用**之前** ---"
# compose 配置预置了 blocked_keywords: ["敏感词"]，所以这一步不需要改动部署。
setmode '{"mode":"ok","reset":true}'
el=$(send gin "请给我敏感词")
has "用户收到拦截话术"             "$SMOKE/sse_gin.log" '您的消息被租户安全策略'
hasnt "没有把模型回复发出去"       "$SMOKE/sse_gin.log" '"text":"Hello "'
check "被拦的消息一次上游都没打"    "0" "$(calls)"
if [ "$HAVE_AUDIT" = 1 ]; then
  audit_new
  has "审计记 stage=input rule=keyword（本次新增行）" "$NEW" '"stage":"input","rule":"keyword"'
else
  skipn "审计记 stage=input" "$NOAUDIT"
fi

echo "--- D. 输出 tripwire：跨 chunk 的关键词必须被截断 ---"
# 这一步会改租户，所以先存底本、把还原挂进 trap。还原直接 PUT 回 GET 的响应体：
# api_key 在里面是打码的（sk-****xxxx），而 update 走 keepSecret(old, in)，认出
# 打码值就保留原密钥 —— admin_test.go:161 那个 round-trip 测试钉住了这条，否则
# 「还原」会把真密钥替换成打码串，对着真实供应商就是一次静默破坏。
restore() {
  [ -s "$ORIG" ] || return 0
  curl -s -o /dev/null -X PUT --data-binary @"$ORIG" "$BASE/admin/tenants/$TENANT"
}
# 跑完不删 $SMOKE：SSE 原文、trace.out 这些是归档进 spec 的证据，而脚本开头已经
# rm -rf 过一次，每次都是干净的。trap 只负责还原租户。
trap 'restore' EXIT

check "GET 原租户（还原用的底本）" "200" "$(curl -s -o "$ORIG" -w '%{http_code}' "$BASE/admin/tenants/$TENANT")"
python3 - "$ORIG" "$SMOKE/tenant.tripped.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1], encoding='utf-8'))
g = d.get('guardrails') or {}
g['output_blocked_keywords'] = ['内部资料']
d['guardrails'] = g
json.dump(d, open(sys.argv[2], 'w', encoding='utf-8'), ensure_ascii=False)
PY
python3 -c 'import json,sys; json.load(open(sys.argv[1],encoding="utf-8"))' "$SMOKE/tenant.tripped.json" \
  && check "装上 tripwire 的 payload 是合法 JSON" "yes" "yes" \
  || check "装上 tripwire 的 payload 是合法 JSON" "yes" "no（python 拼坏了）"
check "PUT 装上 output tripwire" "200" "$(curl -s -o "$SMOKE/put.out" -w '%{http_code}' \
  -X PUT --data-binary @"$SMOKE/tenant.tripped.json" "$BASE/admin/tenants/$TENANT")"

setmode '{"mode":"ok","reset":true}'
el=$(send gw "leak it")
has "截断前的两块已到用户"         "$SMOKE/sse_gw.log" 'world, 内部'
hasnt "第三块被拦下"               "$SMOKE/sse_gw.log" '资料 leaked.'
has "用户收到截断话术"             "$SMOKE/sse_gw.log" '回复因触发租户安全策略'
has "终态 done（截断也要收场）"    "$SMOKE/sse_gw.log" '"done":true'
if [ "$HAVE_AUDIT" = 1 ]; then
  audit_new
  has "审计记 stage=output rule=keyword（本次新增行）" "$NEW" '"stage":"output","rule":"keyword"'
else
  skipn "审计记 stage=output" "$NOAUDIT"
fi

echo "--- D'. 还原：租户回到原样，基线回复恢复完整 ---"
restore
: > "$ORIG"   # 已还原，别让 trap 再 PUT 一次
curl -s "$BASE/admin/tenants/$TENANT" > "$SMOKE/tenant.after.json"
hasnt "output_blocked_keywords 已清掉" "$SMOKE/tenant.after.json" 'output_blocked_keywords'
has "预置的输入 guardrail 还在（还原没多删）" "$SMOKE/tenant.after.json" '敏感词'
setmode '{"mode":"ok","reset":true}'
el=$(send back "hello again")
has "还原后第三块重新到达"          "$SMOKE/sse_back.log" '资料 leaked.'
hasnt "还原后不再有截断话术"        "$SMOKE/sse_back.log" '回复因触发租户安全策略'

echo "--- E. 全链路 trace_id ---"
setmode '{"mode":"ok","reset":true}'
el=$(send tr "trace me")
if [ "$HAVE_AUDIT" != 1 ]; then
  skipn "全链路 trace_id" "$NOAUDIT"
else
  audit_new
  python3 - "$NEW" "tr" > "$SMOKE/trace.out" <<'PY'
import json, re, sys
rows = [json.loads(l) for l in open(sys.argv[1], encoding='utf-8') if l.strip()]
mine = [r for r in rows if r.get('user_id') == sys.argv[2]]
ids = sorted({r.get('trace_id', '') for r in mine})
# 三态必须分开，否则会拿 SKIP 掩盖真故障：刚才那条消息一定产生了 inbound 和 reply
# 两行（它们带 user_id），一行都没看到就说明审计取回或 AUDIT_BASE 切片坏了，
# 那是 FAIL；看到了但 trace_id 为空才是「这个栈没开 tracing」，那只是没证据，是 SKIP。
if not mine:
    print(0); print(0); print('norows'); print(''); print('')
    sys.exit(0)
# model_call 那几行**不带 user_id**：channels.go 只在 inbound / guardrail_block /
# throttled / reply 上填它。所以按 user_id 过滤会正好滤掉本节要断言的那一行，
# 必须用 trace_id 把它们捞回来 —— 这恰恰就是「全链路」要证的东西。
if len(ids) != 1 or not ids[0]:
    # 再分一刀：全为空是「tracing 没开」（SKIP），而多个不同 id 或空与非空混杂
    # 是「历史行混进来了」（FAIL）。实测过：把 AUDIT_BASE 退回 0 后，上一轮的行
    # 带着另一个 trace_id 进来，len(ids)==2，当时被归成「tracing 没开」—— 把一个
    # 真故障报成了没证据。
    if not [i for i in ids if i]:
        print(len(mine)); print(len(ids)); print('off'); print(''); print('')
    else:
        print(len(mine)); print(len(ids)); print('mixed'); print(','.join(ids)); print('')
    sys.exit(0)
tid = ids[0]
linked = [r for r in rows if r.get('trace_id') == tid]
print(len(linked))
print(1)
print('yes' if re.fullmatch(r'[0-9a-f]{32}', tid) else 'no(%s)' % tid)
print(' '.join(r.get('event', '?') for r in linked))
print(tid)
PY
  case "$(sed -n 3p "$SMOKE/trace.out")" in
  norows)
    check "本次消息留下了带 user_id 的审计行" "yes" \
      "no（一行都没看到：审计取回或 AUDIT_BASE 切片坏了，不是 tracing 的事）" ;;
  off)
    skipn "全链路 trace_id" "这个栈没开 tracing（本次新增的审计行里 trace_id 为空）。\
开法：配置里加 telemetry.traces.exporter: stdout（不需 collector 镜像），重起栈再跑一遍。" ;;
  mixed)
    check "user_id=tr 的行只属于**一个** trace_id" "1" "$(sed -n 2p "$SMOKE/trace.out")"
    printf '      trace_id 集合（多半是历史行混进来了，见纪律 4）: %s\n' \
      "$(sed -n 4p "$SMOKE/trace.out")" ;;
  *)
    num_gt "同一 trace_id 串起多行审计（含不带 user_id 的 model_call）" \
      "$(sed -n 1p "$SMOKE/trace.out")" 1
    check "user_id=tr 的行只属于**一个** trace_id" "1" "$(sed -n 2p "$SMOKE/trace.out")"
    check "trace_id 是 32 位十六进制"          "yes" "$(sed -n 3p "$SMOKE/trace.out")"
    has "事件序列里有 inbound"                 "$SMOKE/trace.out" 'inbound'
    has "事件序列里有模型调用"                 "$SMOKE/trace.out" 'model_call'
    has "事件序列里有 reply"                   "$SMOKE/trace.out" 'reply'
    printf '      trace_id: %s\n      事件序列: %s\n' \
      "$(sed -n 5p "$SMOKE/trace.out")" "$(sed -n 4p "$SMOKE/trace.out")" ;;
  esac
fi

printf '\n=== %d PASS / %d FAIL / %d SKIP ===\n' "$pass" "$fail" "$skip"
if [ "$fail" != 0 ]; then
  echo "E2E FAIL"
elif [ "$skip" != 0 ] && [ "$STRICT" = 1 ]; then
  echo "E2E FAIL（--strict：有 $skip 条性质没拿到证据，SKIP 算失败）"; fail=1
elif [ "$skip" != 0 ]; then
  echo "E2E PASS（但有 $skip 条没拿到证据，见上面的 SKIP 行；要完整证据用 --strict）"
else
  echo "E2E PASS"
fi
[ "$fail" = 0 ]
