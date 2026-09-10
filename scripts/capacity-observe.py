#!/usr/bin/env python3
"""Collect disposable Capacity infrastructure metrics around one load command."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
UNAVAILABLE = "unavailable"
SERVICE_NAMES = ("gateway", "worker-1", "worker-2")
STREAM_COMMANDS = {
    "xadd",
    "xack",
    "xautoclaim",
    "xclaim",
    "xpending",
    "xread",
    "xreadgroup",
    "xrange",
    "xrevrange",
}
SESSION_LOCK_COMMANDS = {
    "del",
    "eval",
    "evalsha",
    "exists",
    "expire",
    "get",
    "hdel",
    "hexists",
    "hget",
    "hgetall",
    "hset",
    "pexpire",
    "set",
}


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def run_command(command: list[str]) -> tuple[int, str]:
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return 127, ""
    return result.returncode, result.stdout.strip()


def compose_command(files: list[str]) -> list[str]:
    command = ["docker", "compose", "--env-file", ".env.example"]
    for compose_file in files:
        command.extend(("-f", compose_file))
    return command


def compose_exec(compose: list[str], service: str, *command: str) -> tuple[int, str]:
    return run_command([*compose, "exec", "-T", service, *command])


def parse_number(value: object) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value


def parse_info(value: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in value.splitlines():
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, raw = line.split(":", 1)
        result[key] = raw
    return result


def parse_commandstats(value: str) -> dict[str, int] | None:
    result: dict[str, int] = {}
    for line in value.splitlines():
        if not line.startswith("cmdstat_") or ":" not in line:
            continue
        command, fields = line.split(":", 1)
        calls = next(
            (field.split("=", 1)[1] for field in fields.split(",") if field.startswith("calls=")),
            None,
        )
        if calls is None:
            continue
        try:
            result[command[len("cmdstat_") :]] = int(calls)
        except ValueError:
            continue
    return result


def redis_queue_snapshot(
    compose: list[str], stream: str, group: str, record_error
) -> dict[str, object]:
    snapshot: dict[str, object] = {
        "pending": UNAVAILABLE,
        "lag": UNAVAILABLE,
        "consumers": UNAVAILABLE,
    }

    code, groups = compose_exec(compose, "redis", "redis-cli", "--json", "XINFO", "GROUPS", stream)
    if code != 0:
        record_error("redis.XINFO.GROUPS")
        return snapshot
    try:
        group_rows = json.loads(groups)
    except json.JSONDecodeError:
        record_error("redis.XINFO.GROUPS.json")
        return snapshot
    selected = next((row for row in group_rows if row.get("name") == group), None)
    if selected is None:
        record_error("redis.consumer_group")
        return snapshot
    for field in ("pending", "lag", "consumers"):
        value = parse_number(selected.get(field))
        if value is not None and value >= 0:
            snapshot[field] = int(value)
        else:
            record_error(f"redis.consumer_group.{field}")
    return snapshot


def redis_snapshot(
    compose: list[str], stream: str, group: str, record_error
) -> dict[str, object]:
    snapshot: dict[str, object] = {
        "total_commands_processed": UNAVAILABLE,
        "command_calls": UNAVAILABLE,
    }

    code, stats = compose_exec(compose, "redis", "redis-cli", "--raw", "INFO", "stats")
    if code == 0:
        raw_total = parse_info(stats).get("total_commands_processed")
        try:
            snapshot["total_commands_processed"] = int(raw_total) if raw_total is not None else UNAVAILABLE
        except ValueError:
            record_error("redis.total_commands_processed")
    else:
        record_error("redis.INFO.stats")

    code, commandstats = compose_exec(compose, "redis", "redis-cli", "--raw", "INFO", "commandstats")
    parsed_commandstats = parse_commandstats(commandstats) if code == 0 else None
    if parsed_commandstats is not None:
        snapshot["command_calls"] = parsed_commandstats
    else:
        record_error("redis.INFO.commandstats")

    snapshot.update(redis_queue_snapshot(compose, stream, group, record_error))
    return snapshot


def postgres_snapshot(compose: list[str], record_error) -> dict[str, object]:
    fields = ("transactions", "commits", "rollbacks", "inserts", "updates", "deletes")
    unavailable = {field: UNAVAILABLE for field in fields}
    query = (
        "SELECT json_build_object("
        "'transactions', xact_commit + xact_rollback,"
        "'commits', xact_commit,"
        "'rollbacks', xact_rollback,"
        "'inserts', tup_inserted,"
        "'updates', tup_updated,"
        "'deletes', tup_deleted"
        ")::text FROM pg_stat_database WHERE datname = current_database();"
    )
    code, output = compose_exec(
        compose,
        "postgres",
        "psql",
        "-U",
        "trpc",
        "-d",
        "trpc_agent_service",
        "-Atqc",
        query,
    )
    if code != 0:
        record_error("postgres.pg_stat_database")
        return unavailable
    try:
        raw = json.loads(next(line for line in output.splitlines() if line.strip()))
    except (StopIteration, json.JSONDecodeError):
        record_error("postgres.pg_stat_database.json")
        return unavailable
    result: dict[str, object] = {}
    for field in fields:
        value = parse_number(raw.get(field))
        if value is None:
            record_error(f"postgres.{field}")
            result[field] = UNAVAILABLE
        else:
            result[field] = int(value)
    return result


def parse_memory_bytes(value: str) -> int | None:
    match = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]*)", value)
    if match is None:
        return None
    amount = float(match.group(1))
    unit = match.group(2).lower()
    multipliers = {
        "": 1,
        "b": 1,
        "kb": 1000,
        "kib": 1024,
        "mb": 1000**2,
        "mib": 1024**2,
        "gb": 1000**3,
        "gib": 1024**3,
        "tb": 1000**4,
        "tib": 1024**4,
    }
    multiplier = multipliers.get(unit)
    return round(amount * multiplier) if multiplier is not None else None


def docker_stats_snapshot(compose: list[str], record_error) -> dict[str, dict[str, object]]:
    empty = {"cpu_percent": UNAVAILABLE, "memory_bytes": UNAVAILABLE}
    result = {service: dict(empty) for service in SERVICE_NAMES}
    code, output = run_command([*compose, "ps", "-q", *SERVICE_NAMES])
    container_ids = output.splitlines() if code == 0 else []
    if len(container_ids) != len(SERVICE_NAMES):
        for service in SERVICE_NAMES:
            record_error(f"docker.container.{service}")
        return result

    code, inspected = run_command(
        [
            "docker",
            "inspect",
            "--format",
            '{{.Id}} {{index .Config.Labels "com.docker.compose.service"}}',
            *container_ids,
        ]
    )
    service_by_id: dict[str, str] = {}
    if code == 0:
        for line in inspected.splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[1] in SERVICE_NAMES:
                service_by_id[parts[0]] = parts[1]
    if not service_by_id:
        service_by_id = dict(zip(container_ids, SERVICE_NAMES))

    code, stats_output = run_command(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *container_ids]
    )
    if code != 0:
        for service in SERVICE_NAMES:
            record_error(f"docker.stats.{service}")
        return result
    for line in stats_output.splitlines():
        try:
            stats = json.loads(line)
            short_id = str(stats.get("ID", ""))
            service = next(
                (name for full_id, name in service_by_id.items() if full_id.startswith(short_id)),
                None,
            )
            if service is None:
                service = next(
                    (name for name in SERVICE_NAMES if str(stats.get("Name", "")).endswith(f"-{name}-1")),
                    None,
                )
            if service is None:
                continue
            cpu_percent = float(str(stats["CPUPerc"]).rstrip("%"))
            memory_bytes = parse_memory_bytes(str(stats["MemUsage"]).split("/", 1)[0])
            if memory_bytes is None:
                raise ValueError("memory value is not parseable")
            result[service] = {"cpu_percent": cpu_percent, "memory_bytes": memory_bytes}
        except (KeyError, ValueError, json.JSONDecodeError):
            continue
    for service in SERVICE_NAMES:
        if result[service]["cpu_percent"] == UNAVAILABLE:
            record_error(f"docker.stats.{service}.json")
    return result


PROMETHEUS_QUERIES = {
    "token_rate": "sum(rate(trpc_agent_service_tenant_model_token_usage_total[1m]))",
    "cost_rate": "sum(rate(trpc_agent_service_tenant_estimated_cost_total[1m]))",
    "model_latency_p95_seconds": "histogram_quantile(0.95, sum by (le) (rate(trpc_agent_service_model_latency_bucket[1m])))",
    "tool_latency_p95_seconds": "histogram_quantile(0.95, sum by (le) (rate(trpc_agent_service_tool_latency_bucket[1m])))",
    "callback_failure_ratio": "sum(rate(trpc_agent_service_im_callback_count_total{result=\"failure\"}[1m])) / clamp_min(sum(rate(trpc_agent_service_im_callback_count_total[1m])), 1)",
    "reply_failure_ratio": "sum(rate(trpc_agent_service_im_reply_failure_total[1m])) / clamp_min(sum(rate(trpc_agent_service_im_reply_success_total[1m])) + sum(rate(trpc_agent_service_im_reply_failure_total[1m])), 1)",
    "queue_lag_p95_seconds": "histogram_quantile(0.95, sum by (le) (rate(trpc_agent_service_queue_lag_bucket[1m])))",
    "worker_utilization": "max(trpc_agent_service_worker_utilization)",
}


def prometheus_snapshot(base_url: str, record_error) -> dict[str, object]:
    result: dict[str, object] = {name: UNAVAILABLE for name in PROMETHEUS_QUERIES}
    if not base_url:
        record_error("prometheus.url")
        return result
    for name, query in PROMETHEUS_QUERIES.items():
        try:
            url = f"{base_url.rstrip('/')}/api/v1/query?{urlencode({'query': query})}"
            request = Request(url, headers={"Accept": "application/json"})
            with urlopen(request, timeout=3) as response:
                payload = json.load(response)
            rows = payload.get("data", {}).get("result", [])
            if not rows:
                record_error(f"prometheus.{name}.empty")
                continue
            value = float(rows[0]["value"][1])
            if value != value or value in (float("inf"), float("-inf")):
                raise ValueError("non-finite value")
            result[name] = value
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            record_error(f"prometheus.{name}")
    return result


def numeric_values(rows: list[dict[str, object]], key: str) -> list[float]:
    values = []
    for row in rows:
        value = parse_number(row.get(key))
        if value is not None:
            values.append(float(value))
    return values


def peak(rows: list[dict[str, object]], key: str) -> float | int | str:
    values = numeric_values(rows, key)
    if not values:
        return UNAVAILABLE
    value = max(values)
    return int(value) if value.is_integer() else value


def counter_delta(before: object, after: object) -> int | str:
    start = parse_number(before)
    end = parse_number(after)
    if start is None or end is None or end < start:
        return UNAVAILABLE
    return int(end - start)


def rate(value: object, seconds: float) -> float | str:
    number = parse_number(value)
    if number is None or seconds <= 0:
        return UNAVAILABLE
    return float(number) / seconds


class Collector:
    def __init__(self, compose: list[str], stream: str, group: str, metrics_dir: Path, prometheus_url: str):
        self.compose = compose
        self.stream = stream
        self.group = group
        self.metrics_dir = metrics_dir
        self.prometheus_url = prometheus_url
        self.samples: list[dict[str, object]] = []
        self.errors: set[str] = set()
        self.lock = threading.Lock()
        self.stop = threading.Event()

    def error(self, component: str) -> None:
        self.errors.add(component)

    def capture(self, phase: str) -> dict[str, object]:
        sample = {
            "phase": phase,
            "timestamp": timestamp(),
            "timestamp_epoch": time.time(),
            "runtime": docker_stats_snapshot(self.compose, self.error),
        }
        redis = redis_queue_snapshot(self.compose, self.stream, self.group, self.error)
        sample["redis_queue"] = {
            "pending": redis["pending"],
            "lag": redis["lag"],
            "consumers": redis["consumers"],
        }
        sample["business_metrics"] = prometheus_snapshot(self.prometheus_url, self.error)
        with self.lock:
            self.samples.append(sample)
        with (self.metrics_dir / "runtime-samples.ndjson").open("a", encoding="utf-8") as file:
            file.write(json.dumps(sample, separators=(",", ":")) + "\n")
        return sample

    def loop(self, interval: float) -> None:
        while not self.stop.is_set():
            self.capture("during")
            self.stop.wait(interval)


def aggregate_runtime(
    samples: list[dict[str, object]], final_sample: dict[str, object], sample_interval: float
) -> dict[str, object]:
    during_samples = [sample for sample in samples if sample["phase"] == "during"]
    observed_interval = UNAVAILABLE
    if len(during_samples) > 1:
        observed_interval = (
            during_samples[-1]["timestamp_epoch"] - during_samples[0]["timestamp_epoch"]
        ) / (len(during_samples) - 1)
    result: dict[str, object] = {
        "sample_count": len(during_samples),
        "configured_sample_interval_seconds": sample_interval,
        "observed_average_interval_seconds": observed_interval,
    }
    for service in SERVICE_NAMES:
        rows = [sample["runtime"][service] for sample in during_samples]
        result[service] = {
            "cpu_peak_percent": peak(rows, "cpu_percent"),
            "memory_peak_bytes": peak(rows, "memory_bytes"),
            "final": final_sample["runtime"][service],
        }
    worker_rows = [result["worker-1"], result["worker-2"]]
    result["worker_cpu_peak_percent"] = peak(worker_rows, "cpu_peak_percent")
    result["worker_memory_peak_bytes"] = peak(worker_rows, "memory_peak_bytes")
    return result


def aggregate_redis(
    samples: list[dict[str, object]], baseline: dict[str, object], final: dict[str, object], seconds: float
) -> dict[str, object]:
    queue_rows = [sample["redis_queue"] for sample in samples]
    command_delta: dict[str, int | str] | str
    before_commands = baseline.get("command_calls")
    after_commands = final.get("command_calls")
    if isinstance(before_commands, dict) and isinstance(after_commands, dict):
        command_delta = {
            key: counter_delta(before_commands.get(key, 0), after_commands.get(key, 0))
            for key in sorted(set(before_commands) | set(after_commands))
        }
    else:
        command_delta = UNAVAILABLE
    stream_delta = (
        {key: value for key, value in command_delta.items() if key in STREAM_COMMANDS}
        if isinstance(command_delta, dict)
        else UNAVAILABLE
    )
    session_delta = (
        {key: value for key, value in command_delta.items() if key in SESSION_LOCK_COMMANDS}
        if isinstance(command_delta, dict)
        else UNAVAILABLE
    )
    return {
        "pending_peak": peak(queue_rows, "pending"),
        "lag_peak": peak(queue_rows, "lag"),
        "final_pending": final.get("pending", UNAVAILABLE),
        "final_lag": final.get("lag", UNAVAILABLE),
        "final_consumers": final.get("consumers", UNAVAILABLE),
        "total_commands_delta": counter_delta(
            baseline.get("total_commands_processed"), final.get("total_commands_processed")
        ),
        "average_commands_per_second": rate(
            counter_delta(baseline.get("total_commands_processed"), final.get("total_commands_processed")),
            seconds,
        ),
        "command_calls_delta": command_delta,
        "stream_command_calls_delta": stream_delta,
        "session_lock_command_calls_delta": session_delta,
    }


def aggregate_postgres(
    baseline: dict[str, object], final: dict[str, object], seconds: float
) -> dict[str, object]:
    deltas = {
        field: counter_delta(baseline.get(field), final.get(field))
        for field in ("transactions", "commits", "rollbacks", "inserts", "updates", "deletes")
    }
    writes = [deltas[field] for field in ("inserts", "updates", "deletes")]
    write_delta: int | str = sum(writes) if all(isinstance(value, int) for value in writes) else UNAVAILABLE
    return {
        "transactions_delta": deltas["transactions"],
        "commits_delta": deltas["commits"],
        "rollbacks_delta": deltas["rollbacks"],
        "inserts_delta": deltas["inserts"],
        "updates_delta": deltas["updates"],
        "deletes_delta": deltas["deletes"],
        "write_delta": write_delta,
        "average_transactions_per_second": rate(deltas["transactions"], seconds),
        "average_write_operations_per_second": rate(write_delta, seconds),
    }


def aggregate_business_metrics(
    samples: list[dict[str, object]], baseline: dict[str, object], final: dict[str, object]
) -> dict[str, object]:
    during = [sample.get("business_metrics", {}) for sample in samples if sample["phase"] != "final"]
    peak_values: dict[str, object] = {}
    for name in PROMETHEUS_QUERIES:
        values = [parse_number(row.get(name)) for row in during]
        numeric = [float(value) for value in values if value is not None]
        peak_values[name] = max(numeric) if numeric else UNAVAILABLE
    return {"baseline": baseline, "final": final, "peak": peak_values}


def read_json(path: Path) -> dict[str, object] | None:
    try:
        with path.open(encoding="utf-8") as file:
            value = json.load(file)
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def write_json(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.write("\n")
    temporary.replace(path)


def git_sha() -> str:
    value = os.environ.get("GITHUB_SHA", "").strip()
    if value:
        return value
    code, output = run_command(["git", "rev-parse", "HEAD"])
    return output if code == 0 and output else UNAVAILABLE


def run(args: argparse.Namespace) -> int:
    report_path = Path(args.report)
    if not report_path.is_absolute():
        report_path = ROOT / report_path
    report_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_dir = report_path.parent / "capacity-metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    compose = compose_command(args.compose_files or ["compose.yaml", "compose.deployment-e2e.yaml"])
    stream = os.environ.get("TRPC_AGENT_SERVICE_REDIS_STREAM", "trpc-agent-service:dispatch")
    group = os.environ.get("TRPC_AGENT_SERVICE_REDIS_GROUP", "workers")
    prometheus_url = args.prometheus_url or os.environ.get(
        "TRPC_AGENT_SERVICE_PROMETHEUS_URL", "http://127.0.0.1:19090"
    )
    collector = Collector(compose, stream, group, metrics_dir, prometheus_url)

    started_at = timestamp()
    started = time.time()
    baseline_redis = redis_snapshot(compose, stream, group, collector.error)
    baseline_postgres = postgres_snapshot(compose, collector.error)
    baseline_business = prometheus_snapshot(prometheus_url, collector.error)
    write_json(
        metrics_dir / "baseline.json",
        {"timestamp": started_at, "redis": baseline_redis, "postgres": baseline_postgres, "business": baseline_business},
    )

    command = list(args.command)
    while command and command[0] == "--":
        command.pop(0)
    if not command:
        print("capacity observer: load command is required", file=sys.stderr)
        return 2

    report_path.unlink(missing_ok=True)
    sampler = threading.Thread(target=collector.loop, args=(args.sample_interval,), daemon=True)
    sampler.start()
    try:
        with report_path.open("w", encoding="utf-8") as report_file:
            process = subprocess.Popen(command, cwd=ROOT, stdout=report_file)
            load_status = process.wait()
    except OSError:
        load_status = 127
        print("capacity observer: load command could not start", file=sys.stderr)
    finally:
        collector.stop.set()
        sampler.join(timeout=max(5.0, args.sample_interval + 5.0))

    final_sample = collector.capture("final")
    finished_at = timestamp()
    elapsed = max(time.time() - started, 0.001)
    final_redis = redis_snapshot(compose, stream, group, collector.error)
    final_postgres = postgres_snapshot(compose, collector.error)
    final_business = prometheus_snapshot(prometheus_url, collector.error)
    write_json(
        metrics_dir / "final.json",
        {"timestamp": finished_at, "redis": final_redis, "postgres": final_postgres, "business": final_business},
    )

    with collector.lock:
        samples = list(collector.samples)
    report = read_json(report_path) or {}
    http_report_valid = bool(report)
    report["runtime_metrics"] = aggregate_runtime(samples, final_sample, args.sample_interval)
    report["redis_metrics"] = aggregate_redis(
        [sample for sample in samples if sample["phase"] != "final"],
        baseline_redis,
        final_redis,
        elapsed,
    )
    report["postgres_metrics"] = aggregate_postgres(baseline_postgres, final_postgres, elapsed)
    report["business_metrics"] = aggregate_business_metrics(samples, baseline_business, final_business)
    report["environment"] = {
        "git_sha": git_sha(),
        "github_run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "test_started_at": started_at,
        "test_finished_at": finished_at,
        "test_date": started_at[:10],
        "measurement_duration_seconds": elapsed,
        "concurrency": report.get("concurrency", UNAVAILABLE),
        "requests": report.get("configured_requests", UNAVAILABLE),
        "duration_seconds": report.get("duration_seconds", UNAVAILABLE),
        "input_concurrency": os.environ.get(
            "CAPACITY_CONCURRENCY", report.get("concurrency", UNAVAILABLE)
        ),
        "input_requests": os.environ.get(
            "CAPACITY_REQUESTS", report.get("configured_requests", UNAVAILABLE)
        ),
        "input_duration": os.environ.get("CAPACITY_DURATION", UNAVAILABLE),
        "input_max_error_rate": os.environ.get(
            "CAPACITY_MAX_ERROR_RATE", report.get("max_error_rate", UNAVAILABLE)
        ),
        "gateway_count": 1,
        "worker_count": 2,
        "workload_type": args.workload_type,
        "model": args.model_type,
        "redis_stream": stream,
        "redis_group": group,
    }
    report["infrastructure_metrics_status"] = "partial" if collector.errors else "complete"
    report["business_metrics_status"] = (
        "complete" if any(parse_number(value) is not None for value in final_business.values()) else UNAVAILABLE
    )
    if not http_report_valid:
        report["http_metrics_status"] = UNAVAILABLE
        load_status = load_status or 1
    write_json(report_path, report)
    if collector.errors:
        print(
            "capacity observer: unavailable metrics: " + ", ".join(sorted(collector.errors)),
            file=sys.stderr,
        )
    return load_status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True)
    parser.add_argument("--sample-interval", type=float, default=2.0)
    parser.add_argument("--compose-file", action="append", dest="compose_files")
    parser.add_argument("--workload-type", default="http_chat_completions")
    parser.add_argument("--model-type", default="deterministic")
    parser.add_argument("--prometheus-url", default="")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.sample_interval <= 0:
        parser.error("--sample-interval must be positive")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
