#!/usr/bin/env python3
"""Record completed CI gates and verify their identity before packaging."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone


JOBS = {
    "go": [
        ("secure-toolchain", ["bash", "./scripts/require_secure_go.sh"]),
        ("module-integrity", ["go", "mod", "verify"]),
        ("format", ["python", "scripts/ci_evidence.py", "check-format"]),
        ("evidence-contracts", ["python", "-B", "-m", "unittest", "discover", "-s", "scripts", "-p", "test_ci_evidence.py"]),
        ("build", ["go", "build", "-buildvcs=false", "./cmd/..."]),
        ("vet", ["go", "vet", "./..."]),
        ("unit", ["go", "test", "-json", "-count=1", "./..."]),
        ("race", ["go", "test", "-json", "-race", "-count=1", "./..."]),
        ("integration", ["go", "test", "-json", "-race", "-tags=integration", "-count=1", "./test/integration", "./cmd/queue-bench"]),
        ("static", ["bash", "./scripts/static_verify.sh"]),
        ("compose-config", ["docker", "compose", "-f", "deploy/docker-compose.yml", "config", "--quiet"]),
        ("compose-build", ["docker", "compose", "-f", "deploy/docker-compose.yml", "build"]),
        ("migration-image", ["docker", "compose", "-f", "deploy/docker-compose.yml", "--profile", "operations", "build", "data-migrate"]),
        ("telegram-poller-image", ["docker", "build", "-f", "deploy/Dockerfile.telegram-poller", "."]),
        ("wecom-bot-image", ["docker", "build", "-f", "deploy/Dockerfile.wecom-bot", "."]),
        ("prometheus-rules", ["docker", "run", "--rm", "--entrypoint", "/bin/promtool", "-v", "{repo}/deploy:/work:ro", "prom/prometheus:v2.54.1", "check", "rules", "/work/prometheus-rules.yml"]),
    ],
    "go-1-25-compatibility": [
        ("module-integrity", ["go", "mod", "verify"]),
        ("build", ["go", "build", "-buildvcs=false", "./..."]),
        ("unit", ["go", "test", "-json", "-count=1", "./..."]),
    ],
    "vulnerability-scan": [
        ("secure-toolchain", ["bash", "./scripts/require_secure_go.sh"]),
        ("vulnerability", ["go", "run", "golang.org/x/vuln/cmd/govulncheck@v1.7.0", "./..."]),
    ],
}
GO_VERSIONS = {job: "go1.25.14" if job == "go-1-25-compatibility" else "go1.26.7" for job in JOBS}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def ci_identity(env):
    identity = {key: env.get(variable, "") for key, variable in {
        "repository": "GITHUB_REPOSITORY", "source_sha": "GITHUB_SHA",
        "run_id": "GITHUB_RUN_ID", "run_attempt": "GITHUB_RUN_ATTEMPT",
    }.items()}
    require(re.fullmatch(r"[0-9a-f]{40}", identity["source_sha"]), "invalid CI source SHA")
    require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", identity["repository"]), "invalid CI repository")
    for key in ("run_id", "run_attempt"):
        require(re.fullmatch(r"[1-9][0-9]*", identity[key]), "invalid CI run identity")
    return identity


def verify_checkout(repo, identity):
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    require(actual == identity["source_sha"], "checkout SHA does not match CI source SHA")
    # Evidence is written outside the checkout. Tracked edits, staged edits and
    # extra untracked source all invalidate the clean commit identity.
    status = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=all"], cwd=repo, text=True)
    require(not status.strip(), "checkout changed while recording or packaging evidence")


def record_job(repo, output, job, identity):
    verify_checkout(repo, identity)
    require(not output.exists() or not any(output.iterdir()), "evidence output must be empty")
    output.mkdir(parents=True, exist_ok=True)
    toolchain = subprocess.check_output(["go", "version"], cwd=repo, text=True).strip()
    require(toolchain.split()[2] == GO_VERSIONS[job], "unexpected Go toolchain for CI job")
    manifest = {
        "schema_version": 1, "job": job, "identity": identity,
        "toolchain": toolchain, "started_utc": now(), "gates": [],
    }
    path = output / (job + ".json")
    write_json(path, manifest)
    for gate, command in JOBS[job]:
        log_path = output / (job + "-" + gate + ".log")
        entry = {"gate": gate, "command": command, "started_utc": now(), "log": log_path.name}
        print("::group::" + job + "/" + gate, flush=True)
        actual = [part.replace("{repo}", str(repo)) for part in command]
        if actual[0] == "python":
            actual[0] = sys.executable
        with log_path.open("wb") as stream:
            try:
                result = subprocess.Popen(actual, cwd=repo, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                for data in iter(result.stdout.readline, b""):
                    stream.write(data)
                    sys.stdout.buffer.write(data)
                    sys.stdout.buffer.flush()
                code = result.wait()
            except OSError:
                stream.write(b"CI gate executable could not be started\n")
                code = 127
        print("::endgroup::", flush=True)
        entry.update({"ended_utc": now(), "exit_code": code, "log_sha256": sha256(log_path)})
        manifest["gates"].append(entry)
        manifest["ended_utc"] = now()
        write_json(path, manifest)
        if code != 0:
            return code
    verify_checkout(repo, identity)
    return 0


def check_interval(start, end):
    try:
        first, last = datetime.fromisoformat(start), datetime.fromisoformat(end)
        require(first.tzinfo is not None and last.tzinfo is not None and first <= last, "invalid evidence timestamps")
    except (TypeError, ValueError):
        raise ValueError("invalid evidence timestamps") from None


def validate_test_log(path, allow_skips):
    """A zero exit code with an empty or incomplete go test log is insufficient."""
    packages, completed, skipped, passed = set(), set(), [], 0
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue  # Compiler diagnostics may accompany the test JSON stream.
        if not isinstance(event, dict) or not event.get("Package"):
            continue
        action = event.get("Action")
        require(action != "fail", "test evidence contains a failed test")
        if event.get("Test") and action == "skip":
            skipped.append(event["Package"] + "/" + event["Test"])
        if event.get("Test") and action == "pass":
            passed += 1
        if action == "start":
            packages.add(event["Package"])
        if action in ("pass", "skip") and not event.get("Test"):
            completed.add(event["Package"])
    require(packages and packages == completed and passed, "test evidence has no complete test results")
    require(allow_skips or not skipped, "required integration tests were skipped")
    return {"packages": len(completed), "passed_test_results": passed, "skipped_tests": skipped}


def require_unique_filenames(paths):
    """Reject same-name files before flattening evidence into the package."""
    names = [path.name for path in paths]
    require(len(names) == len(set(names)), "CI evidence contains duplicate filenames")


def verify_evidence(inputs, output, identity):
    require(inputs.is_dir(), "CI evidence directory is missing")
    files = [path for path in inputs.rglob("*") if path.is_file()]
    require(not any(path.is_symlink() for path in inputs.rglob("*")), "evidence cannot contain symbolic links")
    require_unique_filenames(files)
    manifests, selected, jobs, test_results = {}, set(), [], {}
    for job in JOBS:
        found = [path for path in files if path.name == job + ".json"]
        require(len(found) == 1, "missing or duplicated CI job evidence: " + job)
        path = found[0]
        value = json.loads(path.read_text(encoding="utf-8"))
        require(value.get("schema_version") == 1 and value.get("job") == job, "invalid CI evidence schema or job")
        producer = value.get("identity", {})
        require(set(producer) == set(identity), "invalid CI evidence identity")
        require(all(producer.get(key) == identity[key] for key in identity if key != "run_attempt"), "CI evidence source or run identity mismatch")
        attempt = producer.get("run_attempt", "")
        # GitHub's "rerun failed jobs" legitimately reuses successful jobs from
        # an earlier attempt of this same run. Preserve their producer attempt;
        # never relabel it as a fresh execution, or accept a different run/SHA.
        require(isinstance(attempt, str) and re.fullmatch(r"[1-9][0-9]*", attempt) and int(attempt) <= int(identity["run_attempt"]), "invalid CI evidence producer attempt")
        require(re.fullmatch(r"go version " + re.escape(GO_VERSIONS[job]) + r" [a-z0-9]+/[a-z0-9]+", value.get("toolchain", "")), "CI evidence toolchain mismatch")
        check_interval(value.get("started_utc"), value.get("ended_utc"))
        gates = value.get("gates", [])
        require(len(gates) == len(JOBS[job]), "missing or extra CI gates: " + job)
        selected.add(path)
        for gate, (expected_name, expected_command) in zip(gates, JOBS[job]):
            require(gate.get("gate") == expected_name and gate.get("command") == expected_command, "CI gate name or command mismatch")
            require(type(gate.get("exit_code")) is int and gate["exit_code"] == 0, "CI gate did not succeed")
            check_interval(gate.get("started_utc"), gate.get("ended_utc"))
            expected_log = job + "-" + expected_name + ".log"
            require(gate.get("log") == expected_log, "invalid CI evidence log path")
            log = path.parent / expected_log
            require(log.is_file() and sha256(log) == gate.get("log_sha256"), "CI evidence log is missing or changed")
            if expected_name in ("unit", "race", "integration"):
                test_results[job + "/" + expected_name] = validate_test_log(log, expected_name != "integration")
            selected.add(log)
        manifests[job] = value
        jobs.append(job)
    require(selected == set(files), "unexpected files in CI evidence artifact")
    require(not output.exists() or not any(output.iterdir()), "verified output must be empty")
    output.mkdir(parents=True, exist_ok=True)
    for path in sorted(selected):
        shutil.copyfile(path, output / path.name)
    write_json(output / "verification-manifest.json", {
        "schema_version": 1, "identity": identity, "scope": "completed_prerequisite_jobs",
        "jobs": jobs, "verified_utc": now(),
        "test_results": test_results,
        "files": {path.name: sha256(path) for path in sorted(selected)},
    })
    with (output / "current-validation.log").open("w", encoding="utf-8", newline="\n") as summary:
        summary.write("CI prerequisite verification\n" + json.dumps(identity, sort_keys=True) + "\n")
        for job in jobs:
            value = manifests[job]
            summary.write("\njob=" + job + " producer_attempt=" + value["identity"]["run_attempt"] + " toolchain=" + value["toolchain"] + "\n")
            for gate in value["gates"]:
                summary.write(json.dumps(gate, sort_keys=True) + "\n")
    return len(selected) + 2


def check_format(repo):
    files = sorted(str(path.relative_to(repo)) for directory in ("cmd", "pkg", "migrations", "test") for path in (repo / directory).rglob("*.go"))
    require(files, "no Go source found")
    result = subprocess.run(["gofmt", "-l"] + files, cwd=repo, capture_output=True, text=True)
    require(result.returncode == 0 and not result.stdout.strip(), "Go source formatting check failed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    run = sub.add_parser("run")
    run.add_argument("--job", choices=JOBS, required=True)
    run.add_argument("--output", type=Path, required=True)
    verify = sub.add_parser("verify")
    verify.add_argument("--input", type=Path, required=True)
    verify.add_argument("--output", type=Path, required=True)
    sub.add_parser("check-format")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    if args.action == "check-format":
        check_format(repo)
        return 0
    identity = ci_identity(os.environ)
    verify_checkout(repo, identity)
    if args.action == "run":
        return record_job(repo, args.output.resolve(), args.job, identity)
    count = verify_evidence(args.input.resolve(), args.output.resolve(), identity)
    verify_checkout(repo, identity)
    print("Verified CI evidence files:", count)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, KeyError, TypeError, OSError, subprocess.CalledProcessError) as error:
        print("CI evidence rejected:", str(error), file=sys.stderr)
        sys.exit(1)
