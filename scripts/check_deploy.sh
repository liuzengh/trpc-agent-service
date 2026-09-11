#!/usr/bin/env bash
# Deployment artifact gate (docs/spec-deployment-fault-drill.md §2.4).
#
#   scripts/check_deploy.sh            check everything that is checkable offline
#   scripts/check_deploy.sh --strict   a SKIP becomes a failure (use this in CI)
#
# WHY THIS SCRIPT EXISTS
# There is no cluster here — no kind, minikube, k3d or helm (fact #3) — so the
# Kubernetes manifests can never be "tested" in the usual sense. That is not a
# reason to leave them unchecked. Almost everything that actually goes wrong in a
# manifest set is structural, and structural properties are decidable offline:
# does the Deployment reference a ConfigMap that the kustomization really
# generates, does the Service selector intersect the Pod labels, does the Ingress
# backend name a port the Service exposes, are the three places that pin the
# replica count still in agreement. None of that needs an API server.
#
# Two things also turned out to be checkable that were not expected to be:
#
#   `kubectl kustomize <dir>` is purely local. It parses every resource and
#       refuses to build if one is malformed. `kubectl apply --dry-run=client
#       --validate=false` is NOT local — it still performs API discovery to map
#       kind to resource, so without a cluster it fails on correct files and
#       proves nothing (fact #3, corrected here).
#   `docker compose config --format json` needs the CLI but not the daemon.
#       Verified with DOCKER_HOST pointed at a dead port: still exit 0. So the
#       compose assertions run on a machine where Docker cannot start anything.
#
# What this script deliberately does NOT do is parse YAML itself. The channel is
# kubectl -> ruby (YAML.load_stream -> JSON) -> jq. Ruby is doing real work here
# and is also a hazard: Psych reads an unquoted scalar that starts with `:` as a
# Symbol, and a Symbol serialises to JSON without the colon. Section C therefore
# scans the rendered manifests for exactly that shape (fact #21), because the
# alternative is a gate that silently checks the wrong value.
#
# The config FILES are not asserted here. trpcservice/config/deploy_test.go feeds
# them to the real loader — parse, build, Validate, env overrides, the exact path
# cmd/trpc-service runs at boot — which is both more precise and faster than
# spawning a process and reading its exit code. Section D just runs those tests,
# and counts them, so that renaming one cannot turn this gate green by matching
# nothing.
#
# Two disciplines carried over from the dep7 run (§4.5), both earned the hard way:
#
#   3. The assertion helpers self-test first, case by case, and a broken helper
#      aborts the run. Twice in one session a red/green result came from the tool
#      rather than the product: an inverted branch in `hasnt`, and an aggregate
#      count that reported PASS for a self-test which had itself failed.
#   4. Every command substitution goes into a variable by assignment, never
#      straight into a function argument. bash 3.2 mangles nested quoting when a
#      `"$(...)"` is a positional argument behind other quoted arguments
#      (fact #18); the assignment form is byte-exact. Long jq programs are read
#      from heredocs for the same reason — no quoting to get wrong.
#
# bash 3.2 compatible: no associative arrays, no ;;& , no ${var,,}.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SMOKE=".smoke/check_deploy"
mkdir -p "$SMOKE"
trap 'rm -rf "$SMOKE"' EXIT

strict=0
case "${1:-}" in
  --strict) strict=1 ;;
  "") ;;
  *) echo "usage: $0 [--strict]" >&2; exit 2 ;;
esac

pass=0
fail=0
skip=0

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

check() { # check <描述> <期望> <实测>
  if [ "$2" = "$3" ]; then
    pass=$((pass + 1))
    [ "$selftest" = 0 ] && printf 'PASS  %-52s %s\n' "$1" "$3"
  else
    fail=$((fail + 1))
    [ "$selftest" = 0 ] && printf 'FAIL  %-52s want=[%s] got=[%s]\n' "$1" "$2" "$3"
  fi
}

has() { # has <描述> <文件> <ERE>  —— 断言正则**命中**
  if grep -qE -- "$3" "$2"; then check "$1" "yes" "yes"
  else check "$1" "yes" "no（$2 里没有 /$3/）"; fi
}

hasnt() { # hasnt <描述> <文件> <ERE>  —— 断言正则**不命中**
  if grep -qE -- "$3" "$2"; then
    check "$1" "no" "yes（命中 $(grep -nE -- "$3" "$2" | head -2 | tr '\n' ';')）"
  else check "$1" "no" "no"; fi
}

num_gt() { # num_gt <描述> <实测> <下界>  —— 断言 实测 > 下界（整数）
  if [ -n "${2:-}" ] && [ "$2" -gt "$3" ] 2>/dev/null; then check "$1" "yes" "yes"
  else check "$1" "yes" "no（got='${2:-}' want>'$3'）"; fi
}

jqv() { # jqv <json 文件> <jq 程序>  —— 输出结果，或一条能看懂的诊断
  local out rc
  out=$(jq -r "$2" "$1" 2>&1); rc=$?
  if [ "$rc" -ne 0 ]; then
    printf 'jq-error: %s' "$(printf '%s' "$out" | head -1)"
  elif [ -z "$out" ]; then
    printf '<empty>'
  else
    printf '%s' "$out"
  fi
}

jqcheck() { # jqcheck <描述> <期望> <json 文件> <jq 程序>
  local got
  got=$(jqv "$3" "$4")
  check "$1" "$2" "$got"
}

skipn() { # skipn <原因> <条数>  —— 明确 skip，不是静默通过
  skip=$((skip + $2))
  printf 'SKIP  %-52s %s\n' "（$2 条）" "$1"
}

have() { command -v "$1" >/dev/null 2>&1; }

# The two shapes of a YAML plain scalar that starts with `:` — a list item and a
# mapping value. Ruby's Psych reads both as a Symbol and JSON drops the colon;
# Go reads both as the string they look like (fact #21). The lookalikes that must
# NOT match, and are covered by the self-test below: a URL with a port in it
# (`redis://redis:6379`) and a value that is actually quoted (`":8080"`).
COLON_SCALAR_LIST='^[[:space:]]*-[[:space:]]+:[^[:space:]]'
COLON_SCALAR_MAP='^[[:space:]]*[^[:space:]#][^:]*:[[:space:]]+:[^[:space:]]'

# ---------------------------------------------------------------------------
# helper self-test: per-case signature, not a tally
# ---------------------------------------------------------------------------
# A tally is what let a broken self-test report PASS in the dep7 run: the fixture
# could not be written, both greps failed, and "wrong" plus "wrong" added up to
# the expected total. Comparing a signature string makes each case individually
# visible, and the fixture is asserted to exist before any case runs.

selftest=1
FIX="$SMOKE/_selftest.yaml"
printf 'needle\nok: redis://redis:6379\n' > "$FIX" || {
  echo "自检 fixture 写不出来：$FIX"; exit 1; }
[ -s "$FIX" ] || { echo "自检 fixture 为空：$FIX"; exit 1; }

sig=""
st() { # st <调用…>  —— 跑一例，把 P/F/? 追加进签名
  local p0=$pass f0=$fail
  "$@"
  if [ "$pass" -gt "$p0" ]; then sig="${sig}P"
  elif [ "$fail" -gt "$f0" ]; then sig="${sig}F"
  else sig="${sig}?"; fi
}
st check   "_st" 'a' 'a'            # P
st check   "_st" 'a' 'b'            # F
st has     "_st" "$FIX" 'needle'    # P
st hasnt   "_st" "$FIX" 'needle'    # F
st has     "_st" "$FIX" 'absent'    # F
st hasnt   "_st" "$FIX" 'absent'    # P
st num_gt  "_st" 5 3                # P
st num_gt  "_st" 3 5                # F
st num_gt  "_st" '' 3               # F
# The `:`-leading-scalar scan of C2, against both the bad shapes and two lookalikes
# that must NOT fire: a URL with a port in it, and a value that is quoted.
printf -- '- :8080\nkey: :9090\nok1: redis://redis:6379\nok2: ":8080"\n' > "$SMOKE/_hz.yaml"
st has     "_st" "$SMOKE/_hz.yaml" "$COLON_SCALAR_LIST"   # P  (- :8080)
st has     "_st" "$SMOKE/_hz.yaml" "$COLON_SCALAR_MAP"    # P  (key: :9090)
st hasnt   "_st" "$FIX" "$COLON_SCALAR_LIST"              # P  (no list items at all)
st hasnt   "_st" "$FIX" "$COLON_SCALAR_MAP"               # P  (ok1/ok2 must not match)
rm -f "$FIX" "$SMOKE/_hz.yaml"

selftest=0
pass=0
fail=0
if [ "$sig" = "PFPFFPPFFPPPP" ]; then
  printf 'PASS  %-52s %s\n' "断言 helper 自检（逐例签名）" "$sig"
else
  printf 'FAIL  %-52s want=[PFPFFPPFFPPPP] got=[%s]\n' "断言 helper 自检" "$sig"
  echo "断言工具不可信，后面的结果全部作废。"
  exit 1
fi

# ---------------------------------------------------------------------------
# Section A — build context hygiene. No tools required.
# ---------------------------------------------------------------------------
echo
echo "── A. 构建上下文与镜像（.dockerignore / Dockerfile）"

# The two entries that are correctness requirements rather than context-size
# savings. config.yaml is gitignored precisely because it holds real keys, and
# `COPY . .` would bake it into a builder layer that anyone with the image can
# read back out. .git can hold the same secrets in history.
has ".dockerignore 排除 config.yaml" .dockerignore '^config\.yaml$'
has ".dockerignore 排除 .git" .dockerignore '^\.git$'
# vendor/ must stay reachable: with a vendor directory Go builds via -mod=vendor,
# which is the only fully offline escape hatch (fact #2).
hasnt ".dockerignore 不排除 vendor/" .dockerignore '^vendor/?$'

# `# syntax=` makes the builder fetch the Dockerfile frontend image from Docker
# Hub, which is unreachable here (fact #1) — the build would die before the
# first FROM.
hasnt "Dockerfile 没有 # syntax= 指令" Dockerfile '^#[[:space:]]*syntax='
has "Dockerfile 终态 FROM scratch" Dockerfile '^FROM scratch$'
has "Dockerfile 静态编译 CGO_ENABLED=0" Dockerfile 'CGO_ENABLED=0'
has "Dockerfile 降权 USER 65534:65534" Dockerfile '^USER 65534:65534$'
has "Dockerfile 构建期造好 /data" Dockerfile 'mkdir -p /out/data'
# audit.New opens its file with O_CREATE but never mkdirs the parent, and scratch
# has no shell to do it at runtime.
has "Dockerfile 把 /data 交给运行时 UID" Dockerfile 'chown=65534:65534 /out/data /data'
has "Dockerfile 带 TLS 根证书" Dockerfile 'ca-certificates\.crt'
has "Dockerfile 两个二进制都进镜像" Dockerfile '^COPY --from=builder /out/fake-model /fake-model$'

# --timeout has to sit above the binary's own probe budget (healthcheckTimeout
# = 8s in cmd/trpc-service/main.go), or Docker reports a bare timeout and hides
# the 503 reasons the endpoint exists to give.
hc_timeout=$(sed -n 's/.*--timeout=\([0-9]*\)s.*/\1/p' Dockerfile | head -1)
num_gt "Dockerfile HEALTHCHECK --timeout > 二进制内部 8s" "$hc_timeout" 8

# ---------------------------------------------------------------------------
# Section B — compose, via the normalized JSON. Needs the docker CLI only.
# ---------------------------------------------------------------------------
echo
echo "── B. compose（docker compose config --format json，不需要 daemon）"

COMPOSE_DEF="$SMOKE/compose.default.json"
COMPOSE_OBS="$SMOKE/compose.observability.json"
if have docker && have jq; then
  # env -u: the normalized output interpolates CONFIG_DIR / APP_PORT / MODEL_PORT
  # / IMAGE, so a value exported in the caller's shell would otherwise decide
  # what this gate asserts. Same reason deploy_test.go pins every env hook empty.
  ok=1
  env -u CONFIG_DIR -u APP_PORT -u MODEL_PORT -u IMAGE \
    docker compose config --format json > "$COMPOSE_DEF" 2>"$SMOKE/compose.err" || ok=0
  env -u CONFIG_DIR -u APP_PORT -u MODEL_PORT -u IMAGE \
    docker compose --profile observability config --format json \
    > "$COMPOSE_OBS" 2>>"$SMOKE/compose.err" || ok=0
  if [ "$ok" = 0 ]; then
    printf 'FAIL  %-52s %s\n' "compose 规范化" "$(head -2 "$SMOKE/compose.err")"
    fail=$((fail + 1))
  else
    # The collector MUST stay out of the default stack: its image has to come
    # from Docker Hub (fact #1), so a `docker compose up` that included it would
    # fail to pull and take the whole stack down with it.
    jqcheck "默认栈 = app + fake-model + redis（不含 collector）" \
      "app,fake-model,redis" "$COMPOSE_DEF" '.services | keys | join(",")'
    jqcheck "--profile observability 才带出 collector" \
      "app,fake-model,otel-collector,redis" "$COMPOSE_OBS" '.services | keys | join(",")'
    jqcheck "collector 挂在 observability profile 上" "observability" \
      "$COMPOSE_OBS" '.services["otel-collector"].profiles | join(",")'

    # One image, two binaries: the fake model is the same build with a different
    # entrypoint, which is why the demo stack needs no python image (fact #11).
    jqcheck "app 与 fake-model 同一个镜像" "ok" "$COMPOSE_DEF" '
      if .services.app.image == .services["fake-model"].image then "ok"
      else "app=\(.services.app.image) model=\(.services["fake-model"].image)" end'

    # Measured bug, not a theoretical one: overriding `entrypoint` does NOT
    # override the image HEALTHCHECK, so this container inherited a probe for
    # port 8080 where nothing listens and settled into `unhealthy` after five
    # `connect: refused`. An unhealthy container misleads every dashboard and
    # breaks any future depends_on: service_healthy pointed at it.
    jqcheck "fake-model 显式关掉继承来的 HEALTHCHECK" "true" \
      "$COMPOSE_DEF" '.services["fake-model"].healthcheck.disable'

    # The app probes the session backend at boot and exits if it is down (D5), so
    # "started" would race the probe.
    jqcheck "app 等 redis 健康而不是仅启动" "service_healthy" \
      "$COMPOSE_DEF" '.services.app.depends_on.redis.condition'

    # fact #19, measured three ways:
    #   ./config.yaml:/config/config.yaml  -> 400 permission denied
    #   ./config:/config                   -> 200, persisted, no tmp left
    #   ./config:/config:ro                -> 400 read-only file system
    # config.Save writes <path>.tmp and renames it into place, and Docker creates
    # /config owned by root, so anything but a writable directory turns every
    # live admin write into error_type=commit.
    jqcheck "app 的 /config 是可写目录挂载（fact #19）" "ok" "$COMPOSE_DEF" '
      [.services.app.volumes[] | select(.target=="/config")] as $m
      | if ($m|length) != 1 then "有 \($m|length) 个 /config 挂载"
        elif $m[0].type != "bind" then "类型是 \($m[0].type)，不是 bind"
        elif $m[0].read_only == true then "只读：admin PUT 会全部 400"
        else "ok" end'
    jqcheck "app 的 /data 是命名卷（审计跨起停留存）" "ok" "$COMPOSE_DEF" '
      [.services.app.volumes[] | select(.target=="/data") | .type]
      | if . == ["volume"] then "ok" else "\(.)" end'
    jqcheck "collector 的配置只读挂载" "ok" "$COMPOSE_OBS" '
      [.services["otel-collector"].volumes[] | select(.read_only==true) | .target]
      | if . == ["/etc/otelcol/config.yaml"] then "ok" else "只读挂载=\(.)" end'

    # The host already runs redis on 6379 (fact #9); publishing a second one
    # there is a coin flip the drills should not depend on.
    jqcheck "redis 不对外发布端口" "ok" "$COMPOSE_DEF" '
      if .services.redis.ports == null then "ok" else "\(.services.redis.ports)" end'
    # A loopback bind inside a container is invisible to everyone else on the
    # network — this is the fake-model equivalent of the fact #21 hazard.
    jqcheck "fake-model 绑 0.0.0.0 而不是 127.0.0.1" "ok" "$COMPOSE_DEF" '
      (.services["fake-model"].command | join(" ")) as $c
      | if ($c | contains("0.0.0.0:9009")) then "ok" else $c end'

    # D4 stops redis mid-flight and D5 asserts the app exits when it is missing.
    # A restart policy on either side would paper over the thing being measured.
    jqcheck "全部服务 restart=no（D4/D5 要能真停）" "no" \
      "$COMPOSE_DEF" '[.services[] | .restart] | unique | join(",")'

    # Compose stays at one replica because fault drills address tas-app by a
    # fixed name. Redis coordination makes K8s, not this local drill stack,
    # the scaling target.
    jqcheck "Compose 演练栈固定 1 个 app" "1" \
      "$COMPOSE_DEF" '.services.app.deploy.replicas'
    # srv.Shutdown gets 5s and main returns without waiting for in-flight
    # dispatch goroutines (fact #8), so a longer grace buys nothing.
    jqcheck "stop_grace_period = 10s（> Shutdown 的 5s，不多给）" "10s" \
      "$COMPOSE_DEF" '.services.app.stop_grace_period'
  fi
else
  skipn "需要 docker CLI 与 jq（compose 规范化不需要 daemon）" 14
fi

# ---------------------------------------------------------------------------
# Section C — Kubernetes manifests, via kustomize. Needs kubectl + ruby + jq.
# ---------------------------------------------------------------------------
echo
echo "── C. k8s 清单（kubectl kustomize → ruby YAML→JSON → jq）"

K8S_YAML="$SMOKE/k8s.yaml"
K8S_JSON="$SMOKE/k8s.json"
if have kubectl && have ruby && have jq; then
  ok=1
  kubectl kustomize deploy/k8s/ > "$K8S_YAML" 2>"$SMOKE/k8s.err" || ok=0
  if [ "$ok" = 1 ]; then
    ruby -ryaml -rjson -e \
      'puts JSON.pretty_generate(YAML.load_stream(File.read(ARGV[0])))' \
      "$K8S_YAML" > "$K8S_JSON" 2>>"$SMOKE/k8s.err" || ok=0
  fi
  if [ "$ok" = 0 ]; then
    printf 'FAIL  %-52s %s\n' "kubectl kustomize 离线渲染" "$(head -3 "$SMOKE/k8s.err")"
    fail=$((fail + 1))
  else
    printf 'PASS  %-52s %s\n' "kubectl kustomize 离线渲染" "exit 0"
    pass=$((pass + 1))

    jqcheck "渲染出 7 类对象（去重）" \
      "ConfigMap,Deployment,HorizontalPodAutoscaler,Ingress,PodDisruptionBudget,Secret,Service" \
      "$K8S_JSON" '[.[] | .kind] | unique | join(",")'
    # The suffix is the content hash. If it is missing, the generator did not run
    # and editing the config no longer rolls the Deployment.
    jqcheck "ConfigMap 名带内容哈希（改配置自动 rollout）" "ok" "$K8S_JSON" '
      [.[] | select(.kind=="ConfigMap") | .metadata.name]
      | if length==1 and (.[0]|startswith("trpc-agent-config-"))
             and (.[0]|length) > 18 then "ok" else "\(.)" end'

    # The hazard that made this gate necessary: kustomize re-emits ":8080"
    # WITHOUT the quotes the source had, Psych reads that as a Symbol, and a
    # Symbol becomes "8080" in JSON — the colon silently disappears. Go's parser
    # reads the same bytes as ":8080". One file, two meanings (fact #21).
    hasnt "渲染结果里没有以 ':' 开头的裸标量（列表项）" "$K8S_YAML" "$COLON_SCALAR_LIST"
    hasnt "渲染结果里没有以 ':' 开头的裸标量（映射值）" "$K8S_YAML" "$COLON_SCALAR_MAP"

    # --- cross-references: the class of bug kustomize cannot catch -------------
    jqcheck "Deployment 引用的 ConfigMap 真的存在" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="ConfigMap") | .metadata.name]) as $cm
      | ([.[] | select(.kind=="Deployment") | .spec.template.spec.volumes[]?
          | select(.configMap) | .configMap.name]) as $ref
      | if ($ref|length)==0 then "根本没有 configMap 卷"
        elif (($ref - $cm)|length)==0 then "ok"
        else "悬空引用：\(($ref - $cm)|join(","))" end'
    jqcheck "envFrom 引用的 Secret 真的存在" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="Secret") | .metadata.name]) as $s
      | ([.[] | select(.kind=="Deployment") | .spec.template.spec.containers[].envFrom[]?
          | select(.secretRef) | .secretRef.name]) as $ref
      | if ($ref|length)==0 then "根本没有 secretRef"
        elif (($ref - $s)|length)==0 then "ok"
        else "悬空引用：\(($ref - $s)|join(","))" end'

    # --- the reliable roles: the split is only real if each Deployment says so
    jqcheck "4 个 Deployment：gateway + worker + delivery + jobs" \
      "trpc-agent-delivery,trpc-agent-jobs,trpc-agent-service,trpc-agent-worker" "$K8S_JSON" '
      [.[] | select(.kind=="Deployment") | .metadata.name] | sort | join(",")'
    jqcheck "三个可靠角色各自声明了正确的 -role" "ok" "$K8S_JSON" '
      [.[] | select(.kind=="Deployment") | {n:.metadata.name, a:(.spec.template.spec.containers[0].args|join(" "))}] as $all
      | ([$all[] | select(.n=="trpc-agent-worker" and (.a|startswith("-role worker")))] | length) as $w
      | ([$all[] | select(.n=="trpc-agent-delivery" and (.a|startswith("-role delivery")))] | length) as $d
      | ([$all[] | select(.n=="trpc-agent-jobs" and (.a|startswith("-role jobs")))] | length) as $j
      | if $w==1 and $d==1 and $j==1 then "ok" else "worker=\($w) delivery=\($d) jobs=\($j)" end'
    jqcheck "worker 副本数 ≥2（fencing 需要第二个进程见证接管）" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-worker") | .spec.replicas] | .[0]) as $r
      | if ($r // 0) >= 2 then "ok" else "replicas=\($r)" end'
    jqcheck "可靠角色的 WORKER_ID 取 Pod 名（每副本唯一）" "ok" "$K8S_JSON" '
      [.[] | select(.kind=="Deployment")
         | select(.metadata.name=="trpc-agent-worker" or .metadata.name=="trpc-agent-delivery" or .metadata.name=="trpc-agent-jobs")] as $roles
      | ([$roles[] | .spec.template.spec.containers[0].env[]?
          | select(.name=="WORKER_ID" and .valueFrom.fieldRef.fieldPath=="metadata.name")] | length) as $ok
      | if $ok==($roles|length) then "ok" else "\($ok)/\($roles|length)" end'
    jqcheck "可靠角色引用 trpc-agent-reliable Secret" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="Deployment") | .spec.template.spec.containers[0].env[]?
          | select(.valueFrom.secretKeyRef.name=="trpc-agent-reliable")] | length) as $refs
      | if $refs>=3 then "ok" else "只有 \($refs) 个环境变量引用它" end'
    # The gateway Deployment's selector is the bare label `app:
    # trpc-agent-service`, and a Deployment's selector also decides which Pods
    # its ReplicaSet owns. A reliable Pod carrying that label would be adopted
    # and then deleted during the gateway's next rollout — so the label set is
    # an invariant, not a style.
    jqcheck "可靠角色 Pod 不带裸 app 标签（不被 gateway ReplicaSet/Service 认领）" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="Deployment" and (.metadata.name|startswith("trpc-agent-")))
          | select(.metadata.name!="trpc-agent-service")
          | .spec.template.metadata.labels.app] | map(select(.!=null)) | length) as $bad
      | if $bad==0 then "ok" else "\($bad) 个可靠 Pod 带 app 标签" end'
    jqcheck "Service selector 命中 Pod 标签，targetPort 命中容器端口名" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="Service")] | .[0]) as $svc
      | ([.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service")] | .[0]) as $dep
      | ($dep.spec.template.metadata.labels) as $pl
      | ($svc.spec.selector | to_entries | map(select($pl[.key] != .value) | .key)) as $bad
      | ($dep.spec.template.spec.containers[0].ports | map(.name)) as $pn
      | ([$svc.spec.ports[] | select((.targetPort|type)=="string") | .targetPort
          | select(. as $t | ($pn|index($t))|not)]) as $miss
      | if ($bad|length)==0 and ($miss|length)==0 then "ok"
        else "selector 不匹配=\($bad|join(",")) 端口名不存在=\($miss|join(","))" end'
    jqcheck "HPA 指向这个 Deployment" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="HorizontalPodAutoscaler") | .spec.scaleTargetRef] | .[0]) as $t
      | ([.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service") | .metadata.name] | .[0]) as $n
      | if $t.kind=="Deployment" and $t.name==$n then "ok" else "\($t) vs Deployment/\($n)" end'
    jqcheck "PDB selector 命中 Pod 标签" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="PodDisruptionBudget") | .spec.selector.matchLabels] | .[0]) as $sel
      | ([.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service")] | .[0].spec.template.metadata.labels) as $pl
      | ($sel | to_entries | map(select($pl[.key] != .value) | .key)) as $bad
      | if ($bad|length)==0 then "ok" else "不匹配：\($bad|join(","))" end'
    jqcheck "Ingress backend 指向 Service 的真实端口" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="Service")] | .[0]) as $svc
      | ([$svc.spec.ports[] | {n:.name, p:.port}]) as $sp
      | ([.[] | select(.kind=="Ingress") | .spec.rules[]?.http.paths[]?.backend.service
          | {n:.name, p:(.port.name // (.port.number|tostring))}]) as $b
      | if ($b|length)==0 then "Ingress 没有 backend"
        else ([$b[] | select(. as $x
                 | ($svc.metadata.name==$x.n)
                   and ([$sp[] | select(.n==$x.p)]|length>0) | not)]) as $bad
        | if ($bad|length)==0 then "ok" else "悬空 backend：\($bad)" end end'

    # Redis-backed SETNX and leases make replicas safe; assert the deployment
    # starts above one, rolls safely, and the HPA can add capacity.
    jqcheck "Redis 协调后允许滚动扩容（Deployment + HPA）" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service") | .spec] | .[0]) as $d
      | ([.[] | select(.kind=="HorizontalPodAutoscaler") | .spec] | .[0]) as $h
      | if $d.replicas>=2 and $d.strategy.type=="RollingUpdate"
           and $h.minReplicas>=2 and $h.maxReplicas>$h.minReplicas then "ok"
        else "replicas=\($d.replicas) strategy=\($d.strategy.type) hpa=\($h.minReplicas)-\($h.maxReplicas)" end'
    jqcheck "terminationGracePeriodSeconds = 10（Shutdown 5s，不等在飞消息）" "10" \
      "$K8S_JSON" '[.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service") | .spec.terminationGracePeriodSeconds] | .[0]'
    # Both composites below guard against null BEFORE joining. In jq `null.foo`
    # is null rather than an error, and `join(" ")` renders null as the empty
    # string — so a manifest with the whole probe block missing would produce
    # "   " and read as a near-miss on the values instead of a missing field.
    # That is the same shape as the self-test that once tallied its way to PASS.
    jqcheck "探针：liveness /healthz，readiness+startup /readyz，5s×2 摘流量" \
      "/healthz /readyz /readyz 4 5 2" "$K8S_JSON" '
      ([.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service") | .spec.template.spec.containers[0]] | .[0]) as $c
      | if ($c.livenessProbe==null) or ($c.readinessProbe==null) or ($c.startupProbe==null)
        then "缺探针：只有 \([$c | keys[] | select(endswith("Probe"))] | join(","))"
        else [$c.livenessProbe.httpGet.path, $c.readinessProbe.httpGet.path,
              $c.startupProbe.httpGet.path, ($c.readinessProbe.timeoutSeconds|tostring),
              ($c.readinessProbe.periodSeconds|tostring),
              ($c.readinessProbe.failureThreshold|tostring)] | join(" ") end'
    # The bound these two inequalities compare against is web.readyTimeout — the
    # handler's own backstop around one whole readiness evaluation — and NOT
    # storage's 2s pingTimeout, which sits inside it. So the number is read out
    # of web.go instead of typed here: a hardcoded 3 keeps passing after someone
    # raises readyTimeout to 10s, and from that moment every probe timeout in
    # the manifest is too small while the gate still reports green. A probe
    # timeout that merely ties the handler budget turns the event into
    # "Client.Timeout" (wedged process) exactly when it should read
    # "statuscode: 503" (dependency down), and telling those apart is all the
    # event has: kubelet composes it from the status code alone and discards the
    # response body (k8s issue 99425). Both probes hit /readyz, so both are held
    # to it.
    rdy=$(sed -nE 's/^const readyTimeout *= *([0-9]+) *\* *time\.Second.*$/\1/p' \
      trpcservice/web/web.go)
    num_gt "web.go 里读得出 readyTimeout（秒；读不出来=常量改名或换了写法）" "$rdy" 0
    rto=$(jqv "$K8S_JSON" '
      [.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service")
       | .spec.template.spec.containers[0].readinessProbe.timeoutSeconds] | .[0]')
    num_gt "readinessProbe.timeoutSeconds > web.readyTimeout 的 ${rdy}s" "$rto" "$rdy"
    sto=$(jqv "$K8S_JSON" '
      [.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service")
       | .spec.template.spec.containers[0].startupProbe.timeoutSeconds] | .[0]')
    num_gt "startupProbe.timeoutSeconds > web.readyTimeout 的 ${rdy}s" "$sto" "$rdy"
    # …and below its own period. The worker is a serial loop (doProbe, then wait
    # for the next tick), so a longer timeout never overlaps probes — it drops
    # the tick and stretches the interval to max(period, timeout), which is what
    # would falsify the "~10s to leave the Service" the manifest promises.
    # Readiness only: startup deliberately breaks this (2s period, 4s timeout),
    # because stretching its interval only lengthens a boot budget nobody waits
    # on, while a 2s period keeps a ready Pod noticed promptly.
    jqcheck "readiness 的 timeout < period（摘流量 ≈ 2×5s 的前提）" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service") | .spec.template.spec.containers[0].readinessProbe]
       | .[0]) as $r
      | if $r.timeoutSeconds < $r.periodSeconds then "ok"
        else "timeout=\($r.timeoutSeconds) period=\($r.periodSeconds)" end'

    jqcheck "非 root + 只读根文件系统 + drop ALL" "true 65534 65534 true false ALL" "$K8S_JSON" '
      ([.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service") | .spec.template.spec] | .[0]) as $ps
      | ($ps.securityContext) as $pod
      | ($ps.containers[0].securityContext) as $cs
      | if ($pod.runAsNonRoot==null) or ($pod.runAsUser==null) or ($pod.fsGroup==null)
           or ($cs.readOnlyRootFilesystem==null) or ($cs.allowPrivilegeEscalation==null)
           or ($cs.capabilities.drop==null)
        then "缺字段：pod=\($pod) container=\($cs)"
        else [($pod.runAsNonRoot|tostring), ($pod.runAsUser|tostring), ($pod.fsGroup|tostring),
              ($cs.readOnlyRootFilesystem|tostring), ($cs.allowPrivilegeEscalation|tostring),
              ($cs.capabilities.drop|join(""))] | join(" ") end'

    # The initContainer is what makes live admin writes possible at all: a
    # ConfigMap volume is read-only, and config.Save needs to write <path>.tmp
    # next to the file it replaces (fact #19).
    jqcheck "initContainer 只读 ConfigMap → 可写 emptyDir → app 挂同一卷" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service") | .spec.template.spec] | .[0]) as $ps
      | ($ps.initContainers[0]) as $ic
      | ($ps.volumes | map({(.name): (if .configMap then "configMap"
                                      elif .emptyDir then "emptyDir"
                                      else "other" end)}) | add) as $kind
      | ($ic.volumeMounts | map(select(.mountPath=="/config-src")) | .[0]) as $src
      | ($ic.volumeMounts | map(select(.mountPath=="/config")) | .[0]) as $dst
      | ($ps.containers[0].volumeMounts | map(select(.mountPath=="/config")) | .[0]) as $appm
      | if $src.readOnly==true and $kind[$src.name]=="configMap"
           and $kind[$dst.name]=="emptyDir" and $dst.readOnly!=true
           and $appm.name==$dst.name and $appm.readOnly!=true then "ok"
        else "src=\($src) dst=\($dst) app=\($appm) kinds=\($kind)" end'
    # The three ends of the config path have to line up: the key inside the
    # ConfigMap, what the initContainer copies, and what -config points at.
    jqcheck "ConfigMap 键 = initContainer 拷贝 = -config 指向" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="ConfigMap") | .data | keys] | .[0] | join(",")) as $keys
      | ([.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service") | .spec.template.spec.containers[0].args]
         | .[0] | join(" ")) as $args
      | ([.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service") | .spec.template.spec.initContainers[0].command]
         | .[0] | join(" ")) as $cmd
      | if $keys=="config.yaml" and ($args|endswith("-config /config/config.yaml"))
           and ($cmd|contains("/config-src/config.yaml"))
           and ($cmd|contains("/config/config.yaml")) then "ok"
        else "keys=\($keys) args=\($args) init=\($cmd)" end'
    # The Go runtime sizes its thread pool from the NODE's cpu count and this repo
    # does not vendor automaxprocs, so GOMAXPROCS has to track limits.cpu or the
    # process spends its life being CFS-throttled — which reads exactly like a
    # model latency regression in the metrics.
    jqcheck "GOMAXPROCS 与 limits.cpu 同步" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service") | .spec.template.spec.containers[0]] | .[0]) as $c
      | ($c.env | map({(.name): .value}) | add) as $e
      | if $e.GOMAXPROCS == $c.resources.limits.cpu then "ok"
        else "GOMAXPROCS=\($e.GOMAXPROCS) limits.cpu=\($c.resources.limits.cpu)" end'
    jqcheck "GOMEMLIMIT 低于 limits.memory（留栈与运行时开销）" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service") | .spec.template.spec.containers[0]] | .[0]) as $c
      | ($c.env | map({(.name): .value}) | add) as $e
      | (($e.GOMEMLIMIT // "") | capture("^(?<n>[0-9]+)MiB$") | .n | tonumber) as $gl
      | (($c.resources.limits.memory // "") | capture("^(?<n>[0-9]+)Mi$") | .n | tonumber) as $ml
      | if $gl > 0 and $gl < $ml then "ok"
        else "GOMEMLIMIT=\($e.GOMEMLIMIT) limits.memory=\($c.resources.limits.memory)" end'
    # audit.file is absent from the ConfigMap, so there must be no /data mount:
    # with readOnlyRootFilesystem a path there is a boot failure, not a warning.
    jqcheck "不设 audit.file 也就不挂 /data（只读根下会起不来）" "ok" "$K8S_JSON" '
      ([.[] | select(.kind=="Deployment" and .metadata.name=="trpc-agent-service") | .spec.template.spec.containers[0].volumeMounts]
       | .[0] | map(.mountPath)) as $m
      | ([.[] | select(.kind=="ConfigMap") | .data["config.yaml"]] | .[0]) as $cfg
      | if ($m|index("/data")) and ($cfg|contains("audit:")|not) then "挂了 /data 但配置里没有 audit"
        elif ($m|index("/data")|not) and ($cfg|contains("file: /data")) then "配置要写 /data 但没挂载"
        else "ok" end'
  fi
else
  skipn "需要 kubectl + ruby + jq（本机 python3 没有 pyyaml，yq 也没装）" 30
fi

# ---------------------------------------------------------------------------
# Section D — the config files, through the real loader.
# ---------------------------------------------------------------------------
echo
echo "── D. 部署配置过真校验器（go test，计数以防改名后匹配到 0 个）"

if have go; then
  DEPLOY_TESTS='^(TestComposeConfigs|TestComposeDemoTenant|TestKubernetesConfig|TestDeployConfigs|TestObservabilityConfigDiffers)'
  out=$(GOCACHE=/private/tmp/trpc-agent-go-cache go test ./trpcservice/config/ -run "$DEPLOY_TESTS" -count=1 -v 2>&1)
  n=$(printf '%s\n' "$out" | grep -cE '^--- PASS' || true)
  check "7 个部署配置测试全过" "7" "$n"
  [ "$n" = 7 ] || printf '%s\n' "$out" | grep -E '^(--- FAIL|    )' | head -10

  # fact #20: audit.New("") used to return a nil logger, so omitting audit.file
  # dropped the entire trail while the docs promised "log-only". The K8s config
  # depends on the fixed behaviour, so its regression test belongs in this gate.
  out=$(GOCACHE=/private/tmp/trpc-agent-go-cache go test ./trpcservice/audit/ -run '^TestEmptyPathIsLogOnlyNotSilent$' -count=1 -v 2>&1)
  n=$(printf '%s\n' "$out" | grep -cE '^--- PASS' || true)
  check "空 audit.file 是 log-only 而不是静默丢弃" "1" "$n"
  [ "$n" = 1 ] || printf '%s\n' "$out" | grep -E '^(--- FAIL|    )' | head -10
else
  skipn "需要 go" 2
fi

# ---------------------------------------------------------------------------
# Section E — the built image, if there is one. Optional: needs the daemon.
# ---------------------------------------------------------------------------
echo
echo "── E. 已构建镜像的实物检查（可选，需要 daemon）"

IMAGE="${IMAGE:-trpc-agent-service:local}"
if have docker && docker image inspect "$IMAGE" >/dev/null 2>&1; then
  cid=$(docker create "$IMAGE" 2>/dev/null)
  if [ -n "$cid" ]; then
    docker export "$cid" 2>/dev/null | tar -tf - > "$SMOKE/image.tar.list" 2>/dev/null
    docker rm "$cid" >/dev/null 2>&1
    # Section A checks that .dockerignore *says* config.yaml. This checks that the
    # secret is actually not in the artifact — the difference between reading the
    # lock and trying the door.
    hasnt "镜像里没有 config.yaml" "$SMOKE/image.tar.list" '(^|/)config\.yaml$'
    hasnt "镜像里没有 .git" "$SMOKE/image.tar.list" '(^|/)\.git/'
    has "镜像里有 /data 目录" "$SMOKE/image.tar.list" '^data/$'
    has "镜像里有 /trpc-service" "$SMOKE/image.tar.list" '^trpc-service$'
    has "镜像里有 /fake-model" "$SMOKE/image.tar.list" '^fake-model$'
    has "镜像里有 TLS 根证书" "$SMOKE/image.tar.list" 'ca-certificates\.crt$'
  else
    skipn "docker create 失败" 6
  fi
else
  skipn "本地没有镜像 ${IMAGE}（docker build -t $IMAGE . 之后再跑）" 6
fi

# ---------------------------------------------------------------------------
echo
if [ "$fail" -gt 0 ]; then
  printf 'DEPLOY CHECK FAIL  %d passed / %d failed / %d skipped\n' "$pass" "$fail" "$skip"
  exit 1
fi
if [ "$skip" -gt 0 ]; then
  printf 'DEPLOY CHECK PASS WITH SKIPS  %d passed / 0 failed / %d skipped\n' "$pass" "$skip"
  printf '被跳过的部分没有被检查。CI 里用 --strict 把 skip 当失败。\n'
  [ "$strict" = 1 ] && exit 1
  exit 0
fi
printf 'DEPLOY CHECK PASS  %d passed / 0 failed / 0 skipped\n' "$pass"
