#!/bin/bash
# 故障演练矩阵 D1–D7（spec §2.6）：在一个**由本脚本拉起并销毁**的 compose 栈上，
# 逐个注入故障并断言平台的行为。
#
# 与 scripts/e2e.sh 的分工：e2e 对「别人已经起好的地址」断言链路是通的，不动容器；
# 这个脚本必须动容器（stop redis、kill app、用 --no-deps 起一个注定失败的进程），
# 所以它自己起栈、自己收拾。两者共用假模型上游和同一套断言纪律。
#
# 四条纪律从 e2e.sh 继承（都是被失败教出来的，逐条的实测来历写在那边的头部）：
#   1. 先过就绪门禁再断言；
#   2. 断言 helper 自己先过逐例签名自检；
#   3. JSON payload 一律 heredoc 落文件 + --data-binary @file；
#   4. 审计断言只看本次新增的行（AUDIT_BASE + tail 切片）。
# 这里再加两条演练特有的：
#   5. **热更新会落盘，所以它跨重启**（实测：PUT message_timeout=7s 之后挂载的
#      配置文件里立即变成 7s，kill 再 start，新进程读到的还是 7s）。admin.commit
#      调的是 config.Save，而 /config 是可写的 bind mount —— 这把应急预算活了下来，
#      代价是文件里的注释被重序列化抹平（事实 #19），而且上一个演练 PUT 的值会
#      留给下一个。所以每个演练开头自己 PUT 一次 settings，不依赖上一个留下的
#      状态 —— 这也是 --only 能单独跑一个演练的前提，而理由不是「热更新会丢」。
#   6. **指标是进程内累积、重启归零**，所以断言用差值而不是绝对值：演练前记一次，
#      演练后记一次，比的是增量。
#
# 还有一条事实决定了配置怎么准备：telemetry 两个 exporter 都得开成 stdout。
#   - traces 不开，channels.go:traceIDOf 刻意返回 ""（noop provider 的全零渲染是
#     纯噪音），审计里就没有 trace_id，而 model_call 那行**不带 user_id**，演练无法
#     把它关联回是哪条消息触发的；
#   - metrics 不开，metrics.go:Setup 只在真的建出 provider 时才 SetMeterProvider，
#     于是全局 meter 保持 noop，所有 trpcservice.* 计数器什么都不产生（实测：off 的
#     栈上容器日志里 trpcservice.messages 出现 0 次）。
# 平台没有 pull 式的 /metrics 端点（web.go 只挂 5 条路由），指标只能 push，而
# stdout 是唯一不需要 collector 镜像的 push 目标 —— 本机拉不到 Docker Hub（事实 #1）。
#
# goroutine 泄漏（D1 的最后一项）在部署栈上没有 pprof 可看，但**不是**只能靠内存
# 曲线猜：fake-model 的 timeout 模式 select 在 r.Context().Done() 上，被取消就打
# "caller gave up"，挂满 30s 自然结束才打 "nobody cancelled"。数这两行就是因果证据
# —— 实测 20 条并发超时：gave up 20 / nobody cancelled 0，放弃时刻精确落在预算上。
#
# 变量名紧邻中文时必须写成 ${VAR}（bash 3.2 把多字节首字节当标识符字符，见 e2e.sh）。
# 自查用的正则得是「\$名字 后紧跟 [^\x00-\x7f]」，不能用 \w 也不能用 [\x80-\xff]：
# 前者在 Unicode 模式下把「，」也当词字符，于是那个坑的形状被整体匹成一个合法
# 变量名；后者在 Python str 模式里只覆盖 U+0080–U+00FF，而中文标点在 U+FF0C。
# 两种写法都扫不出东西 —— 本脚本第一次真跑就是被一个漏网的变量名停在起栈前。
# 扫描器还得跳过注释行：注释里的变量不会展开，而上面这段说明本身就是坑的形状，
# 不跳过就会自己报自己 —— 记录陷阱的文字被陷阱探测器当成陷阱，是个假阳性。
#
# 用法：
#   scripts/fault_drill.sh                 # 全部 7 个演练
#   scripts/fault_drill.sh --only D4,D5    # 只跑指定的
#   scripts/fault_drill.sh --strict        # SKIP 也算失败
#   scripts/fault_drill.sh --keep          # 跑完不拆栈（供人工检查）
set -u
cd "$(dirname "$0")/.."

ONLY=""; STRICT=0; KEEP=0
while [ $# -gt 0 ]; do
  case "$1" in
    --only)   ONLY="${2:-}"; shift 2 ;;
    --strict) STRICT=1; shift ;;
    --keep)   KEEP=1; shift ;;
    -h|--help) sed -n '2,50p' "$0"; exit 0 ;;
    *) echo "未知参数：$1（用 --only / --strict / --keep）"; exit 2 ;;
  esac
done

SMOKE=.smoke/drill
CFGDIR=$SMOKE/config
BASE=http://localhost:8080
MODEL=http://localhost:9009
TENANT=demo
BYSTANDER=demo-alt
APP=tas-app
MODELC=tas-model
REDISC=tas-redis
RUN=$$

rm -rf "$SMOKE"; mkdir -p "$CFGDIR"

pass=0; fail=0; skip=0; selftest=0

check() { # check <描述> <期望> <实测> [备注] —— 备注无论成败都打
  # 第四位是为了让日志能直接当证据归档：只打 yes 的话，「耗时落在预算附近」
  # 到底落在哪儿、「session 键还在」到底剩几个，都得重跑一遍才知道。
  if [ "$2" = "$3" ]; then pass=$((pass+1)); [ "$selftest" = 0 ] && printf 'PASS  %-48s %s%s\n' "$1" "$3" "${4:+  [$4]}"
  else fail=$((fail+1)); [ "$selftest" = 0 ] && printf 'FAIL  %-48s want=[%s] got=[%s]%s\n' "$1" "$2" "$3" "${4:+  [$4]}"; fi
}
has()   { if grep -q "$3" "$2"; then check "$1" "yes" "yes"; else check "$1" "yes" "no（$2 里没有 $3）"; fi; }
hasnt() { if grep -q "$3" "$2"; then check "$1" "no" "yes（$2 里出现了 $3）"; else check "$1" "no" "no"; fi; }
within() { # within <描述> <值> <下限> <上限> —— 秒，区间 [lo,hi)
  check "$1" "yes" "$(python3 -c "print('yes' if $3<=$2<$4 else 'no')")" "实测 ${2}s，区间 [$3,$4)"
}
num_gt() { if [ -n "${2:-}" ] && [ "$2" -gt "$3" ] 2>/dev/null; then check "$1" "yes" "yes" "实测 ${2} > $3"
  else check "$1" "yes" "no（got='${2:-}' want>'$3'）"; fi; }
skipn() { skip=$((skip + 1)); printf 'SKIP  %-48s %s\n' "$1" "$2"; }
want()  { # want D1 —— --only 的过滤器
  [ -z "$ONLY" ] && return 0
  case ",$ONLY," in *",$1,"*) return 0 ;; esac
  return 1
}

# --- helper 自检在下面，所有 helper 定义完之后（bash 要先解析函数才能调，
#     而 acount / wait_done / metric 都是断言helper，必须一并进自检）---

for t in curl python3 docker; do
  command -v "$t" >/dev/null 2>&1 || { echo "缺 ${t}，跑不了。"; exit 1; }
done
docker compose version >/dev/null 2>&1 || { echo "docker compose 不可用。"; exit 1; }

code() { curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$1"; }
calls() { curl -s "$MODEL/__mode" | python3 -c 'import sys,json;print(json.load(sys.stdin)["requests"])'; }
lastmsgs() { curl -s "$MODEL/__mode" | python3 -c 'import sys,json;print(json.load(sys.stdin)["last_messages"])'; }

# setmode <json> —— 注入并回读校验（e2e.sh 的纪律：静默失败就出在这里）。
setmode() {
  local got w real
  got=$(curl -s -X POST "$MODEL/__mode" -d "$1")
  w=$(python3 -c "import json,sys;print(json.loads(sys.argv[1])['mode'])" "$1")
  real=$(printf '%s' "$got" | python3 -c 'import sys,json;print(json.load(sys.stdin)["mode"])' 2>/dev/null)
  check "注入模式 ${w}（已回读校验）" "$w" "${real:-<读不回来>}"
  [ "$real" = "$w" ]
}

# settings <message_timeout> <max_concurrency_per_tenant> —— PUT 并回读校验。
# 每个演练自己调一次，不依赖上一个留下的状态：PUT 会落盘（纪律 5），所以上一个
# 演练调出来的值会活到下一个；--only 单独跑一个演练时更得自己把状态钉死。
settings() {
  local body real
  cat > "$SMOKE/settings.json" <<JSON
{"message_timeout":"$1","max_concurrency_per_tenant":$2,"max_llm_calls":8}
JSON
  curl -s -o "$SMOKE/settings.out" -w '%{http_code}' -X PUT "$BASE/admin/settings" \
    --data-binary @"$SMOKE/settings.json" > "$SMOKE/settings.code"
  check "PUT settings（timeout=$1 quota=$2）" "200" "$(cat "$SMOKE/settings.code")"
  real=$(curl -s "$BASE/admin/settings")
  has "settings 回读含 timeout=$1" <(printf '%s' "$real") "\"message_timeout\":\"$1\""
  has "settings 回读含 quota=$2" <(printf '%s' "$real") "\"max_concurrency_per_tenant\":$2"
}

# send <user> <text> [tenant] -> 打印耗时；SSE 落在 $SMOKE/sse_<user>.log
# 序号走文件而不是 shell 变量：调用点写的是 el=$(send ...)，命令替换是子 shell，
# SEQ=$((SEQ+1)) 只改子 shell 的副本，父 shell 永远停在 0（最小复现：连调三次
# $(f)，每次里面都是 SEQ=1）。于是每一次 send 造出的 msg_id 一模一样，而 dedup
# 表的键就是 msg_id（channels.go:313），TTL 窗口内第二句被当成重复投递静默吞掉 ——
# 202、无 inbound 行、无回复，看上去完全像「宕机期间消息挂死了」。D4 连败两轮都是
# 这个原因；D6 逃过一劫只因为它两轮之间 kill 了进程，而 dedup 表是进程内存的。
SEQF=$SMOKE/.seq
echo 0 > "$SEQF"
send() {
  local u=$1 txt=$2 t=${3:-$TENANT} c t0 t1 mid f n
  local log="$SMOKE/sse_$u.log"
  n=$(cat "$SEQF" 2>/dev/null); n=$(( ${n:-0} + 1 )); echo "$n" > "$SEQF"
  mid="drill-$RUN-$u-$n"
  f="$SMOKE/req_$mid.json"
  # 先清空 SSE 日志，再判撞号。顺序反了会留下一条假绿：M1 变异（把 msg_id 退回
  # 不唯一）实测发现，守卫在清空之前 return 9，于是上一轮的 Hello 还躺在日志里，
  # 「恢复后用户重新拿到正常回复」靠陈旧证据蒙对了。这就是纪律 4（审计只看本次
  # 新增的行）在 SSE 日志上的同一个陷阱：断言的文件里不得有上一轮的东西。
  : > "$log"
  # 撞号必须响亮失败，不能静默复用同一个文件。上面那条文件计数是为了不撞号，
  # 这条是它的兜底：上一版「修」过一次（RUN+user 改成 RUN+user+SEQ），但修复本身
  # 是 no-op，没人发现，因为失败形状一模一样（还是 89s + 审计 0 行）。当时只要
  # ls req_* 就会看到每个用户只有一个文件，那就是铁证 —— 所以这里直接把它变成错误。
  [ -e "$f" ] && { echo "msg_id 撞号：${mid}（dedup 会把这条吞掉）" >&2; return 9; }
  curl -sN --max-time 60 "$BASE/webchat/stream?tenant=$t&user=$u" > "$log" 2>&1 &
  c=$!
  sleep 0.4   # 等 SSE 注册完，否则回复发出去时还没有订阅者
  cat > "$f" <<JSON
{"user":"$u","msg_id":"$mid","text":"$txt"}
JSON
  t0=$(python3 -c 'import time;print(time.time())')
  curl -s -o "$SMOKE/cb_$u.out" -w '%{http_code}' --max-time 30 -X POST "$BASE/callback/webchat/$t" \
    --data-binary @"$f" > "$SMOKE/cb_$u.code"
  # 轮询上限用 bash 内建的 SECONDS，不用「次数 × sleep」：实测 600 次 × sleep 0.1
  # 的墙钟是 90s 而不是 60s（D4 那两条 no(89.76) 就是这么来的），因为每轮还要 fork
  # 一次 grep。拿这个上限去跟超时预算比就会被它骗，而它看起来又像个准确的 60s。
  SECONDS=0
  while [ "$SECONDS" -lt 60 ]; do
    grep -q '"done":true' "$log" 2>/dev/null && break
    sleep 0.1
  done
  t1=$(python3 -c 'import time;print(time.time())')
  kill "$c" 2>/dev/null; wait "$c" 2>/dev/null   # 显式收尸，否则噪音混进断言输出
  python3 -c "print(f'{$t1-$t0:.2f}')"
}

# 审计（纪律 4）：scratch 里没有 shell，只能 docker cp。
AUDIT=$SMOKE/audit.jsonl
NEW=$SMOKE/audit.new.jsonl
audit_dump() { docker cp "$APP:/data/audit.jsonl" "$AUDIT" >/dev/null 2>&1; }
AUDIT_BASE=0
audit_mark() { audit_dump && AUDIT_BASE=$(wc -l < "$AUDIT" | tr -d ' ') || AUDIT_BASE=0; }
audit_new() { audit_dump || return 1; tail -n "+$((AUDIT_BASE + 1))" "$AUDIT" > "$NEW"; }

# 审计行的字段抽取：afield <行号(从1起)> <字段> —— 从 $NEW 里读。
afield() {
  python3 -c '
import json,sys
rows=[json.loads(l) for l in open(sys.argv[1],encoding="utf-8") if l.strip()]
i=int(sys.argv[2])-1
if i>=len(rows): print("<无此行>"); sys.exit(0)
v=rows[i].get(sys.argv[3])
print("" if v is None else v)' "$NEW" "$1" "$2"
}
acount() { # acount <字段=值> [<字段=值> ...] —— 数 $NEW 里同时满足全部条件的行数
  # 收可变个条件而不是「一个字段一个值」：D7 要数的是 event=reply 且 decision=block
  # 这种交叉条件（限流那条话术到底有没有被投递），两个独立的计数相乘是数不出来的。
  # 空条件必须报错：all([]) 在 python 里是 True，少传一个参数会静默地变成「数全部行」，
  # 而那个数字看上去完全像个合理的结果。
  if [ $# -eq 0 ]; then echo noargs; return 2; fi
  python3 -c '
import json,sys
rows=[json.loads(l) for l in open(sys.argv[1],encoding="utf-8") if l.strip()]
want=[a.split("=",1) for a in sys.argv[2:]]
print(sum(1 for r in rows if all(str(r.get(k))==v for k,v in want)))' "$NEW" "$@"
}

# ntrace —— $NEW 里不同 trace_id 的个数（只数非空的）。D1/D4 都用它证明一条消息的
# 所有审计行挂在同一条链上 —— 失败路径最容易断链，因为出错的地方往往在 span 外面。
ntrace() {
  python3 -c '
import json,sys
rows=[json.loads(l) for l in open(sys.argv[1],encoding="utf-8") if l.strip()]
print(len({r.get("trace_id") for r in rows if r.get("trace_id")}))' "$NEW"
}

# metric <指标名> <属性过滤(逗号分隔 k=v，可空)> -> 最近一次能解析的 dump 里的值。
metric() {
  # 20000 而不是 800：实测一条消息给 app 日志加 134 行（trace 的 stdout exporter
  # 是多行美化 JSON，一个 span 几十行），窗口太窄会把指标 dump 挤出去。
  docker logs --tail 20000 "$APP" 2>&1 | python3 -c '
import json,sys
name=sys.argv[1]
want={}
for kv in sys.argv[2].split(","):
    if kv:
        k,v=kv.split("=",1); want[k]=v
best=None; cand=0; bad=0
for line in sys.stdin:
    if "\"ScopeMetrics\"" not in line: continue
    cand+=1
    try: d=json.loads(line)
    except Exception: bad+=1; continue
    best=d
# 拿不到 dump 时把诊断数字一并吐出来，而不是光一个 NODUMP：候选 0 是「exporter
# 没开、周期 reader 还没到第一次导出、或窗口太窄」，候选>0 但全坏是「日志被截断」。
# 两者都不该被静默当成 0 —— 那会让「指标增了 1」变成 0-0=0，断言朝错误的方向失败。
if best is None: print("NODUMP:%d:%d"%(cand,bad)); sys.exit(0)
tot=0; found=False
for sm in best.get("ScopeMetrics") or []:
    for m in sm.get("Metrics") or []:
        if m.get("Name")!=name: continue
        for dp in (m.get("Data") or {}).get("DataPoints") or []:
            a={x["Key"]:x["Value"]["Value"] for x in dp.get("Attributes") or []}
            if all(a.get(k)==v for k,v in want.items()):
                tot+=dp.get("Value") or 0; found=True
print(tot if found else 0)' "$1" "$2"
}

# wait_metric —— 周期 reader 的第一次导出发生在 interval（5s）**之后**，所以刚起来的
# 进程上取基线会什么都取不到。等它出现；等不到就让调用方记 SKIP 而不是 FAIL，
# 因为「没拿到指标」和「指标是 0」是两件事，混在一起就会得出「平台没记指标」的假结论。
wait_metric() {
  local i v
  for i in $(seq 1 30); do
    v=$(metric trpcservice.messages "")
    # esac 和 done 必须都在：case 写在一行上时很容易只留一个结束词，而 bash 报的
    # 是下面那个函数的 "}"（它替未闭合的 for 找到了文件末尾），错位两行。
    case "$v" in NODUMP*) sleep 0.5 ;; *) return 0 ;; esac
  done
  echo "$v"; return 1
}

# delta <后> <前> —— 两个都必须是纯数字，否则原样吐出来。
# 不用 python 算差：把 NODUMP 插进 "print(${m1}-${m0})" 会变成 NameError，报出来的是
# 一堆 traceback 加上 got=[]，而真正要知道的「哪一头没取到、候选行有几条」全丢了。
delta() {
  case "$1" in ''|*[!0-9]*) echo "bad_after($1)"; return ;; esac
  case "$2" in ''|*[!0-9]*) echo "bad_before($2)"; return ;; esac
  echo $(( $1 - $2 ))
}

# gave_up / nobody —— D1 的 goroutine 因果证据，数假模型日志。
gave_up()  { docker logs "$MODELC" 2>&1 | grep -c "caller gave up"; }
nobody()   { docker logs "$MODELC" 2>&1 | grep -c "nobody cancelled"; }

wait_ready() { # wait_ready <期望码> <最多几秒>
  local want=$1 n=${2:-20} i c
  for i in $(seq 1 $((n * 2))); do
    c=$(curl -s -o "$SMOKE/rz.json" -w '%{http_code}' --max-time 3 "$BASE/readyz" 2>/dev/null)
    [ "$c" = "$want" ] && { echo "$i"; return 0; }
    sleep 0.5
  done
  echo 0; return 1
}

# --- D4–D7 要的额外观测点 ---

# banner 数启动横幅。main.go:49 在载配置之前就打了它，所以 D5 的失败进程
# 也有横幅（而 listening 一行都没）—— 两者的差就是 fail-fast 的形状。
banner()   { docker logs "$APP" 2>&1 | grep -c "multi-tenant node-based agent platform"; }
listening() { docker logs "$APP" 2>&1 | grep -c '"msg":"listening"'; }
appstate() { docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$APP" 2>/dev/null; }
started()  { docker inspect -f '{{.State.StartedAt}}' "$APP" 2>/dev/null; }
rediskeys() { docker exec "$REDISC" redis-cli --scan --pattern 'tas:*' 2>/dev/null | wc -l | tr -d ' '; }

# wait_done <log> <至少几个终态> <最多几秒> —— D7 要同时等多条回复落在同一条流上。
wait_done() {
  local log=$1 n=$2 s=${3:-30} got
  # 上限用 SECONDS，理由同 send()：「次数 × sleep」会把每轮 grep 的 fork 开销当成 0，
  # 于是名义上的 40s 实际能跑到 60s+。
  SECONDS=0
  while [ "$SECONDS" -lt "$s" ]; do
    # 不能写 $(grep -c ... || echo 0)：grep 数到 0 时已经打了 "0" 且退出码 1，
    # || 后面再补一个 0 就变成两行，[ -ge ] 拿到 "0 0" 直接报整数表达式错。
    got=$(grep -c '"done":true' "$log" 2>/dev/null)
    [ "${got:-0}" -ge "$n" ] && return 0
    sleep 0.2
  done
  return 1
}

# stream <user> <tenant> —— 后台开一条 SSE，pid 放进 $SPID。
# 不用命令替换取 pid：$(...) 会在子 shell 里 fork curl，子 shell 一退 curl 就过继给
# init，后面 kill/wait 的父子关系绕了一层，而 wait 对非子进程是报错的。
stream() {
  local u=$1 t=$2
  local log="$SMOKE/sse_$u.log"
  : > "$log"
  curl -sN --max-time 120 "$BASE/webchat/stream?tenant=$t&user=$u" > "$log" 2>&1 &
  SPID=$!
  sleep 0.4   # 等 SSE 注册完，否则回复发出去时还没有订阅者
}
stopstream() { kill "$1" 2>/dev/null; wait "$1" 2>/dev/null; }

# sendraw <user> <tenant> <msg_id> <text> —— 只投递不等回复，打印 HTTP 码。
# send() 那套「发一条等一条」在 D7 里是错的形状：洪峰隔离要的就是三条同时在飞。
sendraw() {
  local u=$1 t=$2 mid=$3 txt=$4
  local f="$SMOKE/req_$mid.json"
  cat > "$f" <<JSON
{"user":"$u","msg_id":"$mid","text":"$txt"}
JSON
  curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "$BASE/callback/webchat/$t" \
    --data-binary @"$f"
}

# --- helper 自检：逐例签名，不是数总数（纪律 2）---
# 位置在这里而不是脚本开头，是因为 bash 要先解析函数定义才能调；而改了签名
# 的 acount 和新增的 wait_done 恰恰是最容易静默退化的两个（acount 少传一个条件
# 就变成数全部行，wait_done 超时不报错就变成对半截日志做 grep）。
selftest=1
FIX=$SMOKE/_selftest.txt
FIXD=$SMOKE/_selftest_done.log
printf 'needle\n' > "$FIX" || { echo "自检 fixture 写不出来：$FIX"; exit 1; }
printf 'data: {"done":true}\ndata: {"done":true}\n' > "$FIXD"
printf '{"event":"reply","decision":"block"}\n{"event":"reply","decision":"ok"}\n{"event":"inbound"}\n' > "$NEW"
sig=""
st() { local p0=$pass f0=$fail; "$@"
       if [ "$pass" -gt "$p0" ]; then sig="${sig}P"; elif [ "$fail" -gt "$f0" ]; then sig="${sig}F"
       else sig="${sig}?"; fi; }
st has     "_st" "$FIX" 'needle'
st hasnt   "_st" "$FIX" 'needle'
st has     "_st" "$FIX" 'absent'
st hasnt   "_st" "$FIX" 'absent'
st check   "_st" 'a' 'a'
st check   "_st" 'a' 'b'
st within  "_st" 1.5 1.0 2.0
st within  "_st" 2.5 1.0 2.0
st num_gt  "_st" 5 3
st num_gt  "_st" 1 3
st check   "_st" 1 "$(acount event=reply decision=block)"
st check   "_st" 2 "$(acount event=reply)"
st check   "_st" 9 "$(acount event=reply)"          # 故意写错，证明它能红
st check   "_st" 0 "$(acount event=nope)"
st check   "_st" noargs "$(acount 2>&1)"            # 空条件必须报错而不是数全部
printf '{"event":"inbound","trace_id":"a"}\n{"event":"reply","trace_id":"a"}\n' > "$NEW"
st check   "_st" 1 "$(ntrace)"
printf '{"event":"inbound","trace_id":"a"}\n{"event":"reply","trace_id":"b"}\n{"event":"x"}\n' > "$NEW"
st check   "_st" 2 "$(ntrace)"                      # 空 trace_id 不算一条链
st check   "_st" ok "$(wait_done "$FIXD" 2 2 && echo ok || echo no)"
st check   "_st" no "$(wait_done "$FIXD" 3 1 && echo ok || echo no)"
st check   "_st" 2 "$(delta 5 3)"
st check   "_st" "bad_after(NODUMP:0:0)" "$(delta 'NODUMP:0:0' 3)"
rm -f "$FIX" "$FIXD" "$NEW"
selftest=0; pass=0; fail=0
WANT_SIG=PFFPPFPFPFPPFPPPPPPPP
if [ "$sig" = "$WANT_SIG" ]; then
  printf 'PASS  %-48s %s\n' "断言 helper 自检（逐例签名）" "$sig"
else
  printf 'FAIL  %-48s want=[%s] got=[%s]\n' "断言 helper 自检" "$WANT_SIG" "$sig"
  echo "断言工具不可信，后面的结果全部作废。"; exit 1
fi

# --- 起栈：配置从仓库那份拷来，追加 telemetry（理由见脚本头）---
cp deploy/compose/config/config.yaml "$CFGDIR/config.yaml" || { echo "拷不到仓库配置"; exit 1; }
cat >> "$CFGDIR/config.yaml" <<'YAML'

# 由 scripts/fault_drill.sh 追加：演练要 trace_id 关联审计行、要指标增量做断言，
# 两者都只能 push 到 stdout（平台没有 /metrics 端点，且本机拉不到 collector 镜像）。
telemetry:
  traces:
    exporter: stdout
  metrics:
    exporter: stdout
    interval: 5s
YAML

echo "--- 起栈（CONFIG_DIR=${CFGDIR}，一次性副本，避免 PUT 毁掉仓库配置的注释）---"
CONFIG_DIR="$CFGDIR" docker compose up -d --force-recreate --build > "$SMOKE/up.log" 2>&1
uprc=$?
check "docker compose up（含重建镜像）" "0" "$uprc"
if [ "$uprc" != 0 ]; then tail -15 "$SMOKE/up.log"; echo "栈起不来，不再往下跑。"; exit 1; fi

# 就绪门禁（纪律 1）
tries=$(wait_ready 200 30)
num_gt "平台 /readyz 在 30s 内就绪" "${tries:-0}" 0
check "假模型 /healthz 在听" "200" "$(code "$MODEL/healthz")"
check "/healthz 体" '{"status":"ok"}' "$(curl -s --max-time 5 "$BASE/healthz")"
if [ "$fail" != 0 ]; then echo "门禁没过，不再往下跑。"; exit 1; fi

# 拆栈挂在 trap 上：中途失败也不会把 redis 留在停止状态、把 app 留在被 kill 状态。
cleanup() {
  [ "$KEEP" = 1 ] && { echo "--- --keep：栈保留，配置在 $CFGDIR ---"; return 0; }
  docker start "$REDISC" >/dev/null 2>&1
  CONFIG_DIR="$CFGDIR" docker compose down > "$SMOKE/down.log" 2>&1
}
trap cleanup EXIT

echo
echo "=== D1 模型超时：预算到点截断，用户拿到话术，上游被取消而非被弃 ==="
if want D1; then
  settings 3s 0
  setmode '{"mode":"timeout","delay":30,"reset":true}'
  audit_mark
  # 先等指标 dump 出现再取基线：周期 reader 的第一次导出在 interval 之后，而
  # D1 是栈起来后第一个演练，基线很容易抢在第一次导出之前 —— 那不是「平台没记
  # 指标」，只是还没到点，两者必须分开（一个 FAIL、一个 SKIP）。
  if why=$(wait_metric); then
    HM=1; m0=$(metric trpcservice.messages "tenant=$TENANT,result=error")
  else
    HM=0; skipn "D1 指标断言" "等不到指标 dump：$why"
  fi
  g0=$(gave_up); n0=$(nobody)
  el=$(send d1-$RUN "hang please")
  within "耗时落在预算附近（3s，不是上游的 30s）" "$el" 3.0 6.0
  has "用户收到超时话术" "$SMOKE/sse_d1-$RUN.log" '本次回复超时'
  hasnt "用户没收到内部错误串" "$SMOKE/sse_d1-$RUN.log" 'context deadline exceeded'
  check "终态只有一个 done 事件" "1" "$(grep -c '"done":true' "$SMOKE/sse_d1-$RUN.log")"
  check "上游只被打了一次（超时不重试）" "1" "$(calls)"
  if audit_new; then
    check "审计 error_type=timeout" "timeout" "$(afield 2 error_type)"
    check "审计 decision=error" "error" "$(afield 2 decision)"
    num_gt "审计 latency_ms 已记（不是 0）" "$(afield 2 latency_ms)" 0
    check "审计 detail 保留原始错误" "context deadline exceeded" "$(afield 2 detail)"
    check "失败路径也留了 reply 行" "reply" "$(afield 3 event)"
    check "三行同属一个 trace_id" "1" "$(ntrace)"
  else
    skipn "D1 审计断言" "取不到审计文件"
  fi
  if [ "$HM" = 1 ]; then
    sleep 6   # 指标 5s 一 dump，等它落地
    m1=$(metric trpcservice.messages "tenant=$TENANT,result=error")
    check "指标 messages{result=error} 增 1" "1" "$(delta "$m1" "$m0")"
  fi
  # goroutine 的因果证据：上游侧看见取消，而不是挂满 30s 自然结束。
  g1=$(gave_up); n1=$(nobody)
  check "上游报告「调用方放弃了」+1" "1" "$((g1 - g0))"
  check "上游没有「没人取消，挂满了」" "0" "$((n1 - n0))"
else
  skipn "D1" "--only 未选中"
fi

echo
echo "=== D2 上游 5xx / 429：错误分类为 agent，detail 进审计不进用户 ==="
if want D2; then
  # 预算必须是 30s，因为下面期望的是 error_type=agent。事实 #17②：openai-go 尊重
  # Retry-After，所以 429 的归类取决于「退避时长 vs message_timeout」—— 预算 2s 时
  # 退避里就到期了，归成 timeout（§4.5 实测）；预算 30s 时重试跑完，如实归成 agent
  # （本演练实测）。两边都量过，所以这里不是碰巧，是把预算当成一个已知变量钉住。
  settings 30s 0
  for mode in error500 limit429; do
    setmode "{\"mode\":\"$mode\",\"reset\":true}"
    audit_mark
    el=$(send "d2-$mode-$RUN" "please fail")
    has "用户收到失败话术（${mode}）" "$SMOKE/sse_d2-$mode-$RUN.log" '服务暂时不可用'
    # 修复前这里是 "agent error: POST ... 500 Internal Server Error ..."，把内部
    # 端点和上游响应体一起发给了任何能访问 /callback 的人。
    hasnt "话术不含内部端点（${mode}）" "$SMOKE/sse_d2-$mode-$RUN.log" 'fake-model:9009'
    hasnt "话术不含 error 前缀（${mode}）" "$SMOKE/sse_d2-$mode-$RUN.log" 'agent error'
    check "openai-go 重试 2 次 → 上游共 3 次（${mode}）" "3" "$(calls)"
    if audit_new; then
      check "审计 error_type=agent（${mode}）" "agent" "$(afield 2 error_type)"
      num_gt "审计 latency_ms 已记（${mode}）" "$(afield 2 latency_ms)" 0
      case "$mode" in
        error500)  has "审计 detail 保留 500 原文" "$NEW" '500 Internal Server Error' ;;
        limit429)  has "审计 detail 保留 429 原文" "$NEW" '429 Too Many Requests' ;;
      esac
      check "失败路径也留了 reply 行（${mode}）" "reply" "$(afield 3 event)"
    else
      skipn "D2 审计断言（${mode}）" "取不到审计文件"
    fi
    # 429 比 500 慢：Retry-After 让重试之间有等待。跨轮实测 500 落在 1.39–1.59s、
    # 429 落在 4.15–4.25s，所以下面用的是区间而不是某一次的样本值；区间被量出来过，
    # 不是拍的（拍出来的上界会在第四遍以 2.65s 那种方式把人手打的脸）。
    case "$mode" in
      error500) within "500 的耗时（3 次调用无退避）" "$el" 1.0 3.5 ;;
      limit429) within "429 的耗时（含 Retry-After 退避）" "$el" 3.0 7.0 ;;
    esac
  done
  setmode '{"mode":"ok","reset":true}'
else
  skipn "D2" "--only 未选中"
fi

echo
echo "=== D3 幂等：同一 msg_id 投递两次只执行一次，第二次静默 ACK ==="
if want D3; then
  settings 30s 0
  setmode '{"mode":"ok","reset":true}'
  audit_mark
  u="d3-$RUN"
  : > "$SMOKE/sse_$u.log"
  curl -sN --max-time 30 "$BASE/webchat/stream?tenant=$TENANT&user=$u" > "$SMOKE/sse_$u.log" 2>&1 &
  c=$!
  sleep 0.4
  # 两次投递用**完全相同**的 payload：msg_id 相同才会命中 dedup。
  cat > "$SMOKE/req_d3.json" <<JSON
{"user":"$u","msg_id":"drill-d3-fixed-$RUN","text":"say hi once"}
JSON
  k1=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/callback/webchat/$TENANT" --data-binary @"$SMOKE/req_d3.json")
  sleep 0.3
  k2=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/callback/webchat/$TENANT" --data-binary @"$SMOKE/req_d3.json")
  for _ in $(seq 1 200); do grep -q '"done":true' "$SMOKE/sse_$u.log" 2>/dev/null && break; sleep 0.1; done
  sleep 0.4
  kill "$c" 2>/dev/null; wait "$c" 2>/dev/null
  check "第一次投递 202" "202" "$k1"
  check "第二次投递也 202（静默 ACK，不是 4xx）" "202" "$k2"
  check "上游只被打了 1 次" "1" "$(calls)"
  check "用户只收到 1 个终态" "1" "$(grep -c '"done":true' "$SMOKE/sse_$u.log")"
  if audit_new; then
    check "审计只有 3 行（inbound/model_call/reply）" "3" "$(wc -l < "$NEW" | tr -d ' ')"
    check "只有 1 行 inbound（第二次连 trace 都不开）" "1" "$(acount event=inbound)"
  else
    skipn "D3 审计断言" "取不到审计文件"
  fi
else
  skipn "D3" "--only 未选中"
fi

echo
echo "=== D4 Redis 运行中宕机：readyz 转 503、liveness 不动、消息拿错误回复而非挂死、起来后自愈 ==="
if want D4; then
  settings 30s 0
  setmode '{"mode":"ok","reset":true}'
  u="d4-$RUN"
  el=$(send "$u" "first turn before the outage")
  base_msgs=$(lastmsgs)
  check "宕机前上游看到 2 条消息（system+user）" "2" "$base_msgs"
  check "宕机前 /readyz 200" "200" "$(code "$BASE/readyz")"
  num_gt "宕机前 session 键已落 Redis" "$(rediskeys)" 0

  # stop 而不是 pause：stop 会把容器从 Docker 的内嵌 DNS 里摘掉，于是 dial 立刻
  # 拿到 no such host。pause 量到的是另一种故障（连接挂着不断），本演练要的是
  # 「后端没了」，而 readyz 的 3s 外层预算正是为「后端慢」那种准备的。
  # 上界从 web.go 读，不写死（同 check_deploy.sh 的做法）：它就是 handler 给一次
  # 就绪评估兜的底 readyTimeout。我原来钉的是 2.0s —— 来自手工量到的一次 0.153s，
  # 第四遍真跑量到 2.65s 就红了。补量五个样本：0.066/0.090/0.131/0.136/1.089s，
  # reason 串五次逐字相同（no such host），所以方差全在 Docker 内嵌 DNS 的解析
  # 耗时上，与平台行为无关。curl 的 --max-time 也得跟着它走：写死 5s 的话，谁把
  # readyTimeout 提到 10s，这里量到的就是 curl 自己截断的 000 而不是 handler 的 503。
  rdy=$(sed -nE 's/^const readyTimeout *= *([0-9]+) *\* *time\.Second.*$/\1/p' trpcservice/web/web.go)
  num_gt "web.go 里读得出 readyTimeout（读不出来=常量改名或换了写法）" "${rdy:-0}" 0
  rdy=${rdy:-3}
  docker stop "$REDISC" > "$SMOKE/d4-stop.log" 2>&1
  t0=$(python3 -c 'import time;print(time.time())')
  rz=$(curl -s -o "$SMOKE/d4-rz.json" -w '%{http_code}' --max-time $((rdy + 2)) "$BASE/readyz")
  t1=$(python3 -c 'import time;print(time.time())')
  check "Redis 一停 /readyz 就转 503" "503" "$rz"
  within "readyz 在 handler 兜底预算（${rdy}s）之内返回" "$(python3 -c "print(f'{$t1-$t0:.2f}')")" 0.0 "$rdy"
  # 比「小于某个秒数」更强的证据：503 里带的是依赖自己的错误串，说明它在兜底
  # 超时触发之前就返回了。真挂满预算的话 reason 会是 context deadline exceeded。
  hasnt "而且是依赖自己报的错，不是兜底超时（证明没挂满预算）" "$SMOKE/d4-rz.json" 'context deadline'
  has "503 体点名了没就绪的原因" "$SMOKE/d4-rz.json" 'session backend'
  has "503 体 status=unavailable" "$SMOKE/d4-rz.json" 'unavailable'
  check "/healthz 仍然 200（liveness 不跟依赖走）" "200" "$(code "$BASE/healthz")"
  check "app 容器没有被编排器判死" "true" \
    "$(docker inspect -f '{{if eq .State.Status "running"}}true{{else}}false{{end}}' "$APP")"

  audit_mark
  el=$(send "$u" "a turn during the outage")
  within "宕机期间的消息很快拿到终态，没有挂死" "$el" 0.0 10.0
  has "用户收到失败话术" "$SMOKE/sse_$u.log" '服务暂时不可用'
  hasnt "用户没看到内部拓扑" "$SMOKE/sse_$u.log" 'lookup redis'
  if audit_new; then
    # 用扫描而不是行号：这一路的行序取决于 runner 在哪一步炸，而 D1/D2 那种
    # 「模型自己失败」的行序是量过的。runner 错误是会话后端没了，形状不同。
    check "审计里出现了 error_type=runner（会话后端失败，不是模型失败）" "1" "$(acount error_type=runner)"
    check "失败路径也留了 reply 行" "1" "$(acount event=reply)"
    # dep9c 那条修复的回归防线：Send 失败曾经被记成 decision=error→ok，而失败路径
    # 根本不留 reply 行。这里两件事一起钉：reply 行存在，而且它如实记的是 error。
    check "reply 行的 decision=error（失败没被记成 ok）" "1" "$(acount event=reply decision=error)"
    check "三行同属一个 trace_id（会话失败也没断链）" "1" "$(ntrace)"
    # detail 的措辞跟 /readyz 不同源，这是量出来的：readyz 走 storage.Ping，报的是
    # "session backend read: ..."；消息路径走的是 redis session 实现自己，报的是
    # "check session exists: check session exists pipeline: dial tcp: lookup redis on
    # 127.0.0.11:53: no such host"。我原来把 readyz 的措辞套到审计上，那是没量过的
    # 猜测 —— 而它失败得看上去像「平台丢了原始错误」，很容易把人引向错的方向。
    has "审计 detail 点名了失败在会话读取这一步" "$NEW" 'check session exists'
    has "而且保留了原始 dial 错误（不是被包装成通用串）" "$NEW" 'dial tcp'
    hasnt "同一个内部串没泄到用户话术里（detail 进审计不进用户）" "$SMOKE/sse_$u.log" 'dial tcp'
  else
    skipn "D4 审计断言" "取不到审计文件"
  fi

  started0=$(started)
  docker start "$REDISC" > "$SMOKE/d4-start.log" 2>&1
  t0=$(python3 -c 'import time;print(time.time())')
  tries=$(wait_ready 200 20)
  t1=$(python3 -c 'import time;print(time.time())')
  num_gt "Redis 起来后 /readyz 自己回到 200" "${tries:-0}" 0
  within "自愈耗时（秒级）" "$(python3 -c "print(f'{$t1-$t0:.2f}')")" 0.0 12.0
  check "整个过程 app 没有被重启" "$started0" "$(started)"
  num_gt "session 键还在（stop/start 不丢数据）" "$(rediskeys)" 0

  el=$(send "$u" "a turn after the recovery")
  after_msgs=$(lastmsgs)
  # 只断言「涨过宕机前」而不断言具体数字：宕机期间那条失败的消息有没有把 user
  # 那一轮写进会话，取决于 runner 在哪一步炸，两种结果都算恢复成功。失忆才是失败。
  num_gt "恢复后上游看到的历史比宕机前多（会话没丢）" "$after_msgs" $((base_msgs + 1))
  has "恢复后用户重新拿到正常回复" "$SMOKE/sse_$u.log" 'Hello'
else
  skipn "D4" "--only 未选中"
fi

echo
echo "=== D5 Redis 启动期宕机：fail-fast 退出，端口从未绑定，不进半死状态 ==="
if want D5; then
  # 不能用「停掉 redis 再起 app」：compose 的 depends_on: service_healthy 会在 app
  # 之前就把这个故障挡下来，量到的是 compose 的门禁而不是进程自己的行为。所以让
  # redis 好好活着、只把 URL 指到一个没人听的端口 —— env 钩子正是为此留的。
  uprc=0
  STORAGE_SESSION_REDIS_URL=redis://redis:6399 CONFIG_DIR="$CFGDIR" \
    docker compose up -d --no-deps --force-recreate app > "$SMOKE/d5-up.log" 2>&1 || uprc=$?
  # 这条本身就是演练的一个发现：up -d 是 detached 的，容器起来就立刻退出它也报 0。
  # 编排器能不能发现故障，取决于它读不读容器状态 —— 所以下面读的是 State。
  check "docker compose up -d 对立刻退出的容器仍报 0（陷阱，不是 bug）" "0" "$uprc"
  st=""
  for _ in $(seq 1 60); do
    st=$(appstate)
    case "$st" in exited*) break ;; esac
    sleep 0.25
  done
  check "进程自己退出且退出码是 1（不是被杀）" "exited 1" "$st"
  docker logs "$APP" > "$SMOKE/d5-app.log" 2>&1
  has "退出原因点名了 session 后端" "$SMOKE/d5-app.log" 'init session storage'
  has "而且是 probe 失败，不是配置解析失败" "$SMOKE/d5-app.log" 'probe'
  check "从没打过 listening（端口未绑定）" "0" "$(grep -c '"msg":"listening"' "$SMOKE/d5-app.log")"
  check "启动横幅打过了（说明进程真跑起来了，不是没被拉起）" "1" \
    "$(grep -c 'multi-tenant node-based agent platform' "$SMOKE/d5-app.log")"
  check "健康探针连不上：不是半死状态" "000" \
    "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$BASE/healthz")"

  CONFIG_DIR="$CFGDIR" docker compose up -d --force-recreate app > "$SMOKE/d5-restore.log" 2>&1
  num_gt "把 URL 改回来就能起（栈没被这个演练弄坏）" "$(wait_ready 200 40)" 0
else
  skipn "D5" "--only 未选中"
fi

echo
echo "=== D6 节点宕机后会话恢复：新进程从 Redis 重建对话，热更新落盘所以跨重启 ==="
if want D6; then
  settings 30s 0
  setmode '{"mode":"ok","reset":true}'
  u="d6-$RUN"
  el=$(send "$u" "remember the word banana")
  check "第一轮上游看到 2 条消息" "2" "$(lastmsgs)"
  check "第一轮只打了上游 1 次（reset 之后计数从 0 起）" "1" "$(calls)"
  b0=$(banner)
  check "演练开始时 app 日志里只有一个进程的横幅" "1" "$b0"
  # 「假模型没被一起重启」必须在 kill 之前记 StartedAt。原来这里断言的是 calls==1，
  # 那是空断言：演练开头 reset:true 已经把 requests 归零，容器有没有被重建这个 1
  # 都成立 —— 它证明的是「本轮打过一次上游」，不是「依赖容器没被动过」。而后者是
  # 承重的：如果 kill/start app 顺带重建了假模型，下面 lastmsgs=4 的因果就被污染。
  # 实测 docker compose up -d --force-recreate app 不动 redis/fake-model，这里钉住。
  mstart=$(docker inspect -f '{{.State.StartedAt}}' "$MODELC" 2>/dev/null)

  docker kill "$APP" > "$SMOKE/d6-kill.log" 2>&1
  st=""
  for _ in $(seq 1 40); do st=$(appstate); case "$st" in exited*) break ;; esac; sleep 0.25; done
  check "docker kill 的退出码是 137（SIGKILL，不是优雅退出）" "exited 137" "$st"

  t0=$(python3 -c 'import time;print(time.time())')
  docker start "$APP" > "$SMOKE/d6-start.log" 2>&1
  tries=$(wait_ready 200 40)
  t1=$(python3 -c 'import time;print(time.time())')
  num_gt "同一容器起回来后 /readyz 恢复 200" "${tries:-0}" 0
  within "恢复耗时（秒级，不是分钟级）" "$(python3 -c "print(f'{$t1-$t0:.2f}')")" 0.0 30.0
  check "横幅变成 2 次：确实是新进程，不是原来那个还活着" "$((b0 + 1))" "$(banner)"
  check "假模型容器没被动过（StartedAt 逐字相同）" "$mstart" \
    "$(docker inspect -f '{{.State.StartedAt}}' "$MODELC" 2>/dev/null)"

  # 模型日志也要切片（纪律 4 的同源陷阱）：前面每个演练都留下过 "request #N"，
  # 直接 grep 'request #2' 会命中上一个演练的那一行，然后把它的 messages= 当成本轮的。
  MODEL_BASE=$(docker logs "$MODELC" 2>&1 | wc -l | tr -d ' ')
  el=$(send "$u" "what word did I ask you to remember")
  MNEW=$SMOKE/model.new.log
  docker logs "$MODELC" 2>&1 | tail -n "+$((MODEL_BASE + 1))" > "$MNEW"
  # 决定性的一条。假模型的回复是脚本化的固定文本，不回显用户说了什么，所以
  # 「新进程答得上来」没法从回复正文里读出来 —— 只能从上游看到了几条消息来判断：
  # 失忆的新进程只会发 2 条（system+当前这句），带上了宕机前那一轮才是 4 条。
  check "新进程把宕机前那一轮一起发给了上游" "4" "$(lastmsgs)"
  has "上游日志里那一行的 messages=4" "$MNEW" 'messages=4'
  has "而且是本轮这句新问题（不是重放旧请求）" "$MNEW" 'what word did I ask'
  check "上游一共只被打过 2 次（每轮一次）" "2" "$(calls)"
  has "用户在新进程上拿到了正常回复" "$SMOKE/sse_$u.log" 'Hello'
  # 纪律 5 的证据，两条一起才密封：进程被 SIGKILL 过，内存一定没了，所以重启后还
  # 读到 30s 只能是从文件来的；文件里那一行则排除了「其实是 env 或默认值凑巧对上」。
  # 这条原来断言的是反的（timeout 回到 2m0s），因为我把「热更新不落盘」当成了事实 ——
  # 实测 admin.commit 第一步就是 config.Save(s.path, next)，而 /config 是可写的 bind
  # mount，所以 PUT 同时改了内存和文件，应急预算活得过节点重启。
  check "热更新跨了重启：新进程读到的还是 PUT 进去的 30s" "yes" \
    "$(curl -s "$BASE/admin/settings" | grep -q '"message_timeout":"30s"' && echo yes || echo no)"
  has "而且它确实落进了挂载的配置文件（不是只在内存里）" "$CFGDIR/config.yaml" 'message_timeout: 30s'
  # 落盘是有代价的，代价本身也要钉住：Save 重写整个文件，注释被抹平（事实 #19）。
  num_gt "仓库那份配置本来是有注释的（对照，否则下面那条是空断言）" \
    "$(grep -c '^#' deploy/compose/config/config.yaml)" 0
  hasnt "副本里的注释被 Save 抹平了" "$CFGDIR/config.yaml" '^#'
  # 差点被我当成数据丢失的一条：PUT 里带了 max_llm_calls=8，文件里却没有这一行。
  # agentToYAML 刻意省略等于默认值的字段（DefaultMaxLLMCalls=8，为了 diff 干净），
  # 重新 Load 会填回同一个默认值，所以无损。不是 bug，但「文件里看不到=没生效」
  # 这个直觉是错的 —— 反过来，看到 max_llm_calls 出现就意味着它不是默认值。
  hasnt "等于默认值的 max_llm_calls 被 Save 省略（无损，但反直觉）" "$CFGDIR/config.yaml" 'max_llm_calls'
else
  skipn "D6" "--only 未选中"
fi

echo
echo "=== D7 单租户洪峰隔离：超额的那条被拒，旁观租户完全不受影响 ==="
if want D7; then
  settings 30s 1
  # delay 是 chunk 之间的间隔（不是整请求的延时），4 个 chunk × 2s ≈ 8s，
  # 足够让配额在 A 的第二、三条到达时还被占着。
  setmode '{"mode":"slow","delay":2,"reset":true}'
  audit_mark
  # 基线要等第一次 dump 出现（理由同 D1）。D7 是最后一个演练，整栈跑下来通常不会
  # 抢跑；但 --only D7 单独跑时它就是第一个，三个基线会全变成 NODUMP，而原来用
  # python 算差的 "print(${th1:-0}-${th0:-0})" 会把 NODUMP 当变量名报 NameError，
  # 输出里只剩一堆 traceback 加 got=[]，看不出是哪一头没取到、候选行有几条。
  HM7=1
  if why=$(wait_metric); then
    th0=$(metric trpcservice.messages "tenant=$TENANT,result=throttled")
    oka0=$(metric trpcservice.messages "tenant=$TENANT,result=ok")
    okb0=$(metric trpcservice.messages "tenant=$BYSTANDER,result=ok")
  else
    HM7=0; skipn "D7 指标断言" "等不到指标 dump：$why"
  fi
  ua="d7a-$RUN"; ub="d7b-$RUN"
  stream "$ua" "$TENANT";    pa=$SPID
  stream "$ub" "$BYSTANDER"; pb=$SPID

  # A 的第一条先走一步，把 quota=1 占住；随后两条同时撞上它。
  ka1=$(sendraw "$ua" "$TENANT" "drill-d7-a1-$RUN" "slow one")
  sleep 0.5
  sendraw "$ua" "$TENANT" "drill-d7-a2-$RUN" "second of the burst" > "$SMOKE/a2.code" &
  p2=$!
  sendraw "$ua" "$TENANT" "drill-d7-a3-$RUN" "third of the burst" > "$SMOKE/a3.code" &
  p3=$!
  kb1=$(sendraw "$ub" "$BYSTANDER" "drill-d7-b1-$RUN" "bystander message")
  wait "$p2" "$p3" 2>/dev/null   # 后台赋的值在子 shell 里，所以码落在文件上
  wait_done "$SMOKE/sse_$ua.log" 3 40
  wait_done "$SMOKE/sse_$ub.log" 1 40
  sleep 0.5
  stopstream "$pa"; stopstream "$pb"

  check "A 的三条都被受理（202，限流是在异步分发里判的）" "202,202,202" \
    "$ka1,$(cat "$SMOKE/a2.code"),$(cat "$SMOKE/a3.code")"
  check "旁观租户 B 也被受理（202）" "202" "$kb1"
  check "A 收到 2 条限流话术" "2" "$(grep -c '当前咨询量较大' "$SMOKE/sse_$ua.log")"
  check "A 收到 3 个终态（1 条正常 + 2 条限流）" "3" "$(grep -c '"done":true' "$SMOKE/sse_$ua.log")"
  has "A 被放行的那条拿到了正常回复" "$SMOKE/sse_$ua.log" 'Hello'
  # 核心命题。前面几条都只是「限流生效了」，隔离是单向的才算数：A 的洪峰不许
  # 溢出到 B，否则一个租户的流量就是在花掉别人的额度。
  check "B 只收到 1 个终态" "1" "$(grep -c '"done":true' "$SMOKE/sse_$ub.log")"
  has "B 拿到完整正常回复" "$SMOKE/sse_$ub.log" 'Hello'
  check "B 一次都没被限流" "0" "$(grep -c '当前咨询量较大' "$SMOKE/sse_$ub.log")"
  check "被拒的 2 条一次上游都没打（requests = A1 + B1）" "2" "$(calls)"

  if audit_new; then
    check "审计 4 行 inbound（限流发生在 inbound 之后）" "4" "$(acount event=inbound)"
    check "审计 2 行 throttled" "2" "$(acount event=throttled)"
    check "throttled 行的 stage=admission" "2" "$(acount event=throttled stage=admission)"
    check "throttled 行的 rule=concurrency" "2" "$(acount event=throttled rule=concurrency)"
    has "throttled 行的 detail 带着当时的配额" "$NEW" 'max_concurrency_per_tenant=1'
    check "审计 2 行 model_call（被拒的没走到模型）" "2" "$(acount event=model_call)"
    check "审计 4 行 reply（每条消息都有终态记录）" "4" "$(acount event=reply)"
    check "其中 2 行是 decision=block（限流话术的投递也被记了）" "2" "$(acount event=reply decision=block)"
    check "A 与 B 的行都在（不是把 B 的算成了 A 的）" "1" \
      "$(acount event=inbound user_id=$ub)"
  else
    skipn "D7 审计断言" "取不到审计文件"
  fi

  if [ "$HM7" = 1 ]; then
    sleep 6   # 指标 5s 一 dump，等它落地
    th1=$(metric trpcservice.messages "tenant=$TENANT,result=throttled")
    oka1=$(metric trpcservice.messages "tenant=$TENANT,result=ok")
    okb1=$(metric trpcservice.messages "tenant=$BYSTANDER,result=ok")
    check "指标 demo/throttled 增 2" "2" "$(delta "$th1" "$th0")"
    check "指标 demo/ok 增 1" "1" "$(delta "$oka1" "$oka0")"
    check "指标 demo-alt/ok 增 1（旁观租户在指标里也看得见）" "1" "$(delta "$okb1" "$okb0")"
  fi
  setmode '{"mode":"ok","reset":true}'
else
  skipn "D7" "--only 未选中"
fi

printf '\n=== %d PASS / %d FAIL / %d SKIP ===\n' "$pass" "$fail" "$skip"
if [ "$fail" != 0 ]; then
  echo "DRILL FAIL"
elif [ "$skip" != 0 ] && [ "$STRICT" = 1 ]; then
  echo "DRILL FAIL（--strict：有 $skip 条性质没拿到证据，SKIP 算失败）"; fail=1
elif [ "$skip" != 0 ]; then
  echo "DRILL PASS（但有 $skip 条没拿到证据，见上面的 SKIP 行；要完整证据用 --strict）"
else
  echo "DRILL PASS"
fi
[ "$fail" = 0 ]
