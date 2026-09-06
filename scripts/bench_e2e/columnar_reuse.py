#!/usr/bin/env python3
"""Opt-in, answer-checked raw/Parquet/persisted-ClickHouse reuse experiment."""
from __future__ import annotations

import argparse
import datetime
import heapq
import json
import math
import os
import platform
import resource
import shutil
import statistics
import sys
import time
from pathlib import Path

import explore

VERSION = 1
EMPTY = {"columns": [], "ordered": False}
EMPTY_EXPECTED = explore.digest_rows(EMPTY, [])
ROUNDTRIP = {"columns": [("source_json", "str")], "ordered": False}
SEMANTICS = {"columns": [("source_json", "str"), ("v", "int"), ("nested_v", "int"), ("payload", "str"),
                         ("mixed_present", "int"), ("mixed_raw", "str")], "ordered": False}
SCHEMA = "source_json String, v Int32, nested_v Int32, payload String, mixed_present UInt8, mixed_raw String"


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key is outside the accelerated experiment contract")
        result[key] = value
    return result


def _check_json_domain(value):
    if type(value) is int and not -(2**63) <= value < 2**64:
        raise ValueError("opaque JSON integer exceeds ClickHouse JSON parser range; raw fallback alone cannot validate accelerated columns")
    if type(value) is float and not math.isfinite(value):
        raise ValueError("nonfinite JSON number is outside the representation contract")
    if isinstance(value, dict):
        for child in value.values():
            _check_json_domain(child)
    elif isinstance(value, list):
        for child in value:
            _check_json_domain(child)


def inspect_source(path):
    """Independent bounded oracle; selected fields deliberately require exact i32."""
    count = total = nested_total = present = nulls = 0
    top = []
    maximum = None
    minimum = None
    roundtrip = explore.Digest(ROUNDTRIP)
    semantics = explore.Digest(SEMANTICS)
    with Path(path).open(encoding="utf-8") as source:
        while True:
            line = source.readline(explore.MAX_LINE + 1)
            if not line:
                break
            if len(line) > explore.MAX_LINE:
                raise ValueError("source row exceeds experiment bound")
            row = json.loads(line, object_pairs_hook=_unique_object)
            if not isinstance(row, dict):
                raise ValueError("source rows must be JSON objects")
            _check_json_domain(row)
            v = row.get("v")
            nested = row.get("nested")
            metrics = nested.get("metrics") if isinstance(nested, dict) else None
            nv = metrics.get("v") if isinstance(metrics, dict) else None
            if any(type(value) is not int or not -(2**31) <= value < 2**31 for value in [v, nv]):
                raise ValueError("accelerated v/nested.metrics.v contract requires present exact i32 integers")
            if not isinstance(row.get("payload"), str):
                raise ValueError("accelerated payload contract requires a present string")
            count += 1
            total += v
            nested_total += nv
            minimum = v if minimum is None else min(minimum, v)
            maximum = v if maximum is None else max(maximum, v)
            heapq.heappush(top, (v, row["payload"]))
            if len(top) > 10:
                heapq.heappop(top)
            raw = line.strip()
            roundtrip.add([raw])
            mixed_present = int("mixed" in row)
            mixed_raw = json.dumps(row["mixed"], ensure_ascii=False, separators=(",", ":"), allow_nan=False) if mixed_present else ""
            semantics.add([raw, v, nv, row["payload"], mixed_present, mixed_raw])
            present += mixed_present
            nulls += int(mixed_present and row["mixed"] is None)
    if not count:
        raise ValueError("empty corpus is outside this experiment")
    threshold = maximum - max(1, (maximum - minimum + 1) // 100) + 1
    definitions = {
        "count": {"columns": [("n", "int")], "ordered": False,
                  "logq": "select count(*) as n from it", "sql": "SELECT count() AS n FROM {source}"},
        "narrow": {"columns": [("n", "int"), ("total", "float")], "ordered": False,
                   "logq": "select count(*) as n, sum(v) as total from it", "sql": "SELECT count() AS n, sum(v) AS total FROM {source}"},
        "nested": {"columns": [("n", "int"), ("total", "float")], "ordered": False,
                   "logq": "select count(*) as n, sum(nested.metrics.v) as total from it", "sql": "SELECT count() AS n, sum(nested_v) AS total FROM {source}"},
        "wide": {"columns": [("payload", "str"), ("v", "int")], "ordered": True,
                 "logq": f"select payload, v from it where v >= {threshold} order by v desc, payload desc limit 10",
                 "sql": f"SELECT payload, v FROM {{source}} WHERE v >= {threshold} ORDER BY v DESC, payload DESC LIMIT 10"},
    }
    answers = {"count": [(count,)], "narrow": [(count, total)], "nested": [(count, nested_total)],
               "wide": [(payload, value) for value, payload in sorted(top, reverse=True) if value >= threshold]}
    for key, definition in definitions.items():
        definition["expected"] = explore.digest_rows(definition, answers[key])
    return {"rows": count, "mixed_present_rows": present, "mixed_null_rows": nulls,
            "roundtrip": roundtrip.snapshot(), "semantics": semantics.snapshot(),
            "queries": definitions, "selective_threshold": threshold}


def sql_literal(value):
    value = str(value)
    if any(char in value for char in "\0\n\r"):
        raise ValueError("control characters in SQL literal")
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def ch_command(binary, query, threads, database_path=None):
    args = [str(binary), "local", "--multiquery", "--query", query,
            "--output_format_json_quote_64bit_integers", "0", "--use_query_cache", "0"]
    if threads:
        args += ["--max_threads", str(threads), "--max_parsing_threads", str(threads)]
    if database_path is not None:
        args += ["--path", str(database_path)]
    return args


def check_identity(saved, current):
    if saved != current:
        raise ValueError("prepared manifest identity changed; use a new prepared directory")


def amortized(preparation, query_samples):
    return {"query_count": len(query_samples),
            "query_wall_seconds": sum(sample["wall_seconds"] for sample in query_samples),
            "total_wall_seconds": preparation["wall_seconds"] + sum(sample["wall_seconds"] for sample in query_samples),
            "total_cpu_seconds": sum(preparation[key] for key in ["user_cpu_seconds", "system_cpu_seconds"]) +
                sum(sample[key] for sample in query_samples for key in ["user_cpu_seconds", "system_cpu_seconds"])}


def repeated_answer(definition, expected, repetitions):
    """Repeat a one-row unordered oracle without retaining N result rows."""
    if repetitions <= 0 or definition["ordered"] or expected["rows"] != 1:
        raise ValueError("session reuse requires a positive count of single-row unordered answers")
    return {"rows": repetitions,
            **{key: f"{int(expected[key], 16) * repetitions % (1 << 256):064x}"
               for key in ["sum_sha256_mod_2_256", "sum_squared_sha256_mod_2_256"]}}


def session_command(argv, repetitions):
    if repetitions <= 0 or "--multiquery" not in argv or argv.count("--query") != 1:
        raise ValueError("session reuse requires one structured ClickHouse multiquery command")
    result = list(argv)
    index = result.index("--query") + 1
    result[index] = ";\n".join([result[index]] * repetitions)
    return result


def projection():
    return ("json AS source_json, JSONExtract(json, 'v', 'Int32') AS v, "
            "JSONExtract(json, 'nested', 'metrics', 'v', 'Int32') AS nested_v, "
            "JSONExtractString(json, 'payload') AS payload, "
            "toUInt8(JSONHas(json, 'mixed')) AS mixed_present, JSONExtractRaw(json, 'mixed') AS mixed_raw")


def raw_source(path):
    return f"(SELECT {projection()} FROM file({sql_literal(path)}, 'JSONAsString', 'json String'))"


def native_raw_source(path):
    schema = "v Int32, payload String, nested Tuple(metrics Tuple(v Int32), unused String)"
    return f"(SELECT v, payload, nested.metrics.v AS nested_v FROM file({sql_literal(path)}, 'JSONEachRow', {sql_literal(schema)}))"


def parquet_source(path):
    return f"file({sql_literal(path)}, 'Parquet')"


def execute(argv, definition=EMPTY, expected=EMPTY_EXPECTED, timeout=300):
    return explore.run_once(argv, definition, expected, timeout=timeout)


def verify_representation(ch, source, expected, threads, timeout, database_path=None):
    columns = ", ".join(name for name, _ in SEMANTICS["columns"])
    execute(ch_command(ch, f"SELECT {columns} FROM {source} FORMAT JSONEachRow", threads, database_path),
            SEMANTICS, expected["semantics"], timeout)


def semantic_fixture(root, ch, threads, timeout):
    """Small real-format gate before any representative conversion/timing."""
    path = root / "semantic-source.jsonl"
    rows = [{"v": 0, "nested": {"metrics": {"v": 0}}, "payload": "absent"}]
    for index, value in enumerate([None, False, "1", 1, 1.25, {"a": [1, None]}, 9007199254740993, -(2**63), 2**64 - 1], 1):
        rows.append({"v": index, "nested": {"metrics": {"v": index}}, "payload": "雪 \\\"", "mixed": value})
    path.write_text("".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows), encoding="utf-8")
    info = inspect_source(path)
    parquet = root / "semantic.parquet"
    query = f"INSERT INTO FUNCTION file({sql_literal(parquet)}, 'Parquet') SELECT * FROM {raw_source(path)} SETTINGS output_format_parquet_compression_method='zstd'"
    execute(ch_command(ch, query, threads), timeout=timeout)
    columns = ", ".join(name for name, _ in SEMANTICS["columns"])
    check = f"SELECT {columns} FROM {parquet_source(parquet)} FORMAT JSONEachRow"
    execute(ch_command(ch, check, threads), SEMANTICS, info["semantics"], timeout)
    return {"status": "passed", "rows": len(rows), "source_sha256": explore.sha256(path),
            "parquet_sha256": explore.sha256(parquet), "oracle": info["semantics"]}


def artifact_snapshot(root):
    """Immutable table parts/schema only; process logs/status are deliberately excluded."""
    paths = [root / "data.parquet"]
    for name in ["store", "metadata"]:
        directory = root / "clickhouse" / name
        if directory.exists():
            paths += [path for path in directory.rglob("*") if path.is_file()]
    result = []
    for path in sorted(paths):
        if not path.resolve().is_relative_to(root.resolve()):
            raise ValueError("prepared artifact escaped its owned directory")
        result.append({"path": str(path.relative_to(root)), "bytes": path.stat().st_size, "sha256": explore.sha256(path)})
    return result


def verify_artifacts(root, expected):
    if artifact_snapshot(root) != expected:
        raise ValueError("prepared data changed; refusing invalidated representation")


def prepare(root, identity, ch, source, info, threads, timeout):
    manifest_path = root / "manifest.json"
    if root.exists():
        if not manifest_path.is_file():
            raise ValueError("refusing foreign or incomplete prepared directory")
        saved = json.loads(manifest_path.read_text())
        check_identity(saved["identity"], identity)
        if saved.get("status") != "complete":
            raise ValueError("prepared directory has no successful validation")
        verify_artifacts(root, saved["artifacts"])
        return saved
    root.mkdir(parents=True)
    parquet = root / "data.parquet"
    database = root / "clickhouse"
    query = f"INSERT INTO FUNCTION file({sql_literal(parquet)}, 'Parquet') SELECT * FROM {raw_source(source)} SETTINGS output_format_parquet_compression_method='zstd'"
    parquet_argv = ch_command(ch, query, threads)
    parquet_sample = execute(parquet_argv, timeout=timeout)
    # Fully qualified Atomic tables survive reopening clickhouse-local --path.
    columns = ", ".join(column + " CODEC(ZSTD)" for column in SCHEMA.split(", "))
    query = (f"CREATE DATABASE reuse ENGINE=Atomic; CREATE TABLE reuse.events ({columns}) ENGINE=MergeTree ORDER BY tuple(); "
             f"INSERT INTO reuse.events SELECT * FROM {raw_source(source)}")
    merge_argv = ch_command(ch, query, threads, database)
    merge_sample = execute(merge_argv, timeout=timeout)
    validation_started = time.perf_counter()
    verify_representation(ch, parquet_source(parquet), info, threads, timeout)
    verify_representation(ch, "reuse.events", info, threads, timeout, database)
    saved = {"status": "complete", "identity": identity, "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
             "preparation": {"parquet": {"argv": parquet_argv, "sample": parquet_sample},
                             "persisted": {"argv": merge_argv, "sample": merge_sample}},
             "representation_validation_wall_seconds": time.perf_counter() - validation_started,
             "artifacts": artifact_snapshot(root)}
    explore.write_json(manifest_path, saved)
    return saved


def _binary(path):
    path = Path(path).resolve()
    if not path.is_file() or not os.access(path, os.X_OK):
        raise ValueError("binary must be executable: " + str(path))
    return {"path": str(path), "sha256": explore.sha256(path),
            "version": explore.information([str(path), "--version"])}


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--data-dir", type=Path, required=True)
    result.add_argument("--file", default="width-2048.jsonl")
    result.add_argument("--prepared-dir", type=Path, required=True)
    result.add_argument("--results-dir", type=Path, required=True)
    result.add_argument("--clickhouse", type=Path, required=True)
    result.add_argument("--logq", type=Path, required=True)
    result.add_argument("--threads", nargs="+", type=int, default=[1])
    result.add_argument("--repetitions", nargs="+", type=int, default=[1, 10, 100])
    result.add_argument("--runs", type=int, default=3)
    result.add_argument("--warmup", type=int, default=1)
    result.add_argument("--cases", nargs="+", choices=["count", "narrow", "nested", "wide"], default=["count", "narrow", "wide"])
    result.add_argument("--engines", nargs="+", choices=["logq_raw", "clickhouse_raw", "clickhouse_envelope", "parquet", "persisted"],
                        default=["logq_raw", "clickhouse_raw", "parquet", "persisted"])
    result.add_argument("--timeout", type=float, default=300)
    result.add_argument("--validate-only", action="store_true")
    result.add_argument("--skip-rss", action="store_true")
    result.add_argument("--session-reuse", action="store_true",
                        help="also measure repeated count/narrow queries in one ClickHouse local process; no logq session equivalent")
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    if args.runs <= 0 or args.warmup < 0 or args.timeout <= 0 or min(args.repetitions) <= 0 or min(args.threads) < 0:
        raise ValueError("positive runs/repetitions/timeout and nonnegative warmup/threads required")
    if args.session_reuse and any(case not in ["count", "narrow"] for case in args.cases):
        raise ValueError("--session-reuse currently requires --cases count and/or narrow")
    source_root = args.data_dir.resolve()
    source = (source_root / args.file).resolve()
    if not source.is_relative_to(source_root) or any(char in str(source) for char in ",*?[]{}"):
        raise ValueError("source must be one unambiguous manifest-owned file")
    manifest_path = source_root / "manifest.json"
    hash_started = time.perf_counter()
    manifest = json.loads(manifest_path.read_text())
    entry = next((item for item in manifest["files"] if source_root / item["path"] == source), None)
    if entry is None or explore.sha256(source) != entry["sha256"]:
        raise ValueError("source missing from manifest or corpus hash changed")
    manifest_hash = explore.sha256(manifest_path)
    hash_wall = time.perf_counter() - hash_started
    binaries = {"logq": _binary(args.logq), "clickhouse": _binary(args.clickhouse)}
    result_dir = args.results_dir.resolve()
    prepared_dir = args.prepared_dir.resolve()
    if result_dir == prepared_dir or result_dir.is_relative_to(prepared_dir) or prepared_dir.is_relative_to(result_dir):
        raise ValueError("results and prepared directories must be separate")
    result_dir.mkdir(parents=True, exist_ok=False)
    snapshots = {}
    for script in [Path(__file__).resolve(), Path(explore.__file__).resolve()]:
        shutil.copyfile(script, result_dir / script.name)
        snapshots[str(script)] = explore.sha256(script)
    metadata = {
        "status": "running", "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "experiment": "explicit-schema ClickHouse JSONEachRow versus standard Parquet versus persisted MergeTree; optional JSONAsString/JSONExtract envelope control is separately labeled; logq raw is a separate engine control, not a native logq cache",
        "cache": "warm: independent source oracle and verification precede timings; physical read bytes/cold cache not measured",
        "query_cache": "ClickHouse use_query_cache=0; each invocation is a fresh local process, not a running server",
        "prepared_contract": "source_json retains every accepted original JSON object; selected v/nested.metrics.v must be present i32 and payload a string; mixed values retained as raw JSON plus presence bit; duplicate keys, nonfinite numbers, and integers outside [-2^63,2^64-1] anywhere in the object are rejected for this projection contract; mixed raw-token validation expects compact Python JSON spelling, so other whitespace or numeric spellings may fail closed",
        "numerics": "selected fields restricted to exact public logq i32; SUM oracle normalized to existing f32 output; opaque raw values retain larger integers",
        "timing": "blocking wait with process-group deadline; CLI startup/execution/format/write included; complete answer validation excluded; repetition totals sum actual successful child samples, exclude harness validation/process-launch gaps",
        "preparation_note": "one observed preparation per representation, validated afterwards; reusing a prepared manifest retains its original preparation cost; no durability/fsync claim",
        "memory_note": "RSS is a separate validated sample including mapped pages; prepared storage is sum of logical artifact sizes, not disk blocks",
        "persisted_layout": "Atomic database; MergeTree ORDER BY tuple(); ZSTD payload codecs; no sort-key/secondary index advantage or running-server reuse",
        "thread_policy": "engine settings only, not affinity; 0 leaves ClickHouse defaults and selects logq auto",
        "source": {"absolute_path": str(source), **entry}, "manifest_sha256": manifest_hash,
        "source_hash_preflight_wall_seconds": hash_wall,
        "immutability": "manifest/user-managed immutable source during matrix, hashed before and after; no per-query full source hash or native invalidation-overhead claim",
        "binaries": binaries, "source_snapshots": snapshots,
        "git_commit": explore.information(["git", "-C", str(explore.ROOT), "rev-parse", "HEAD"]),
        "git_status": explore.information(["git", "-C", str(explore.ROOT), "status", "--short"]),
        "platform": platform.platform(), "logical_cpus": os.cpu_count(),
        "threads": args.threads, "runs": args.runs, "repetitions": args.repetitions,
        "validate_only": args.validate_only, "prepared_dir": str(prepared_dir),
        "session_reuse": args.session_reuse,
        "session_note": "optional separate ClickHouse-only control: N queries in one fresh local --multiquery process, one startup included, query cache disabled, all N answers validated; no equivalent logq session and no running-server or cold-cache claim",
    }
    results = []
    verification = []
    session_results = []
    session_verification = []
    def save():
        explore.write_json(result_dir / "metadata.json", metadata)
        explore.write_json(result_dir / "results.json", results)
        explore.write_json(result_dir / "verification.json", verification)
        if args.session_reuse:
            explore.write_json(result_dir / "session-results.json", session_results)
            explore.write_json(result_dir / "session-verification.json", session_verification)
    save()
    try:
        contract_started = time.perf_counter()
        before = resource.getrusage(resource.RUSAGE_SELF)
        info = inspect_source(source)
        after = resource.getrusage(resource.RUSAGE_SELF)
        contract = {"wall_seconds": time.perf_counter() - contract_started,
                    "user_cpu_seconds": after.ru_utime - before.ru_utime, "system_cpu_seconds": after.ru_stime - before.ru_stime}
        metadata["contract_and_oracle_sample"] = contract
        metadata["contract_cost_note"] = "additional conservative total charges this full Python contract/oracle pass once to prepared representations; it includes oracle fingerprint/top10 work and is not an exact native-cache validation cost"
        explore.write_json(result_dir / "oracle.json", info)
        ch, logq = (binaries[key]["path"] for key in ["clickhouse", "logq"])
        # Semantic fixtures are tiny and separate from the representative source.
        semantic_started = time.perf_counter()
        metadata["semantic_fixture"] = semantic_fixture(result_dir, ch, args.threads[0], args.timeout)
        metadata["semantic_fixture_wall_seconds"] = time.perf_counter() - semantic_started
        identity = {"source_path": str(source), "source_sha256": entry["sha256"], "schema_version": VERSION,
                    "schema": SCHEMA, "projection": projection(), "clickhouse_sha256": binaries["clickhouse"]["sha256"],
                    "conversion_script_sha256": snapshots[str(Path(__file__).resolve())],
                    "measurement_helper_sha256": snapshots[str(Path(explore.__file__).resolve())],
                    "preparation_threads": args.threads[0]}
        prepared = prepare(prepared_dir, identity, ch, source, info, args.threads[0], args.timeout)
        explore.write_json(result_dir / "prepared-manifest.json", prepared)
        metadata["preparation"] = prepared["preparation"]
        metadata["storage_bytes"] = {
            "raw": source.stat().st_size,
            "parquet": sum(item["bytes"] for item in prepared["artifacts"] if item["path"] == "data.parquet"),
            "persisted": sum(item["bytes"] for item in prepared["artifacts"] if item["path"] != "data.parquet"),
        }
        sources = {"clickhouse_raw": native_raw_source(source), "clickhouse_envelope": raw_source(source),
                   "parquet": parquet_source(prepared_dir / "data.parquet"), "persisted": "reuse.events"}
        zero_prep = {"wall_seconds": 0, "user_cpu_seconds": 0, "system_cpu_seconds": 0}
        combinations = []
        session_combinations = []
        verification_started = time.perf_counter()
        for case in dict.fromkeys(args.cases):
            definition = info["queries"][case]
            for threads in dict.fromkeys(args.threads):
                commands = {"logq_raw": [logq, "query", "--output", "ndjson", "--threads", str(threads), "--table", f"it:jsonl={source}", definition["logq"]]}
                for engine, table in sources.items():
                    commands[engine] = ch_command(ch, definition["sql"].format(source=table) + " FORMAT JSONEachRow", threads,
                                                 prepared_dir / "clickhouse" if engine == "persisted" else None)
                    if engine == "clickhouse_raw":
                        commands[engine] += ["--input_format_skip_unknown_fields", "1", "--input_format_json_named_tuples_as_objects", "1",
                                             "--input_format_json_defaults_for_missing_elements_in_named_tuple", "1"]
                for engine, command in commands.items():
                    if engine not in args.engines:
                        continue
                    # Every selected engine/case is checked before any query timings.
                    execute(command, definition, definition["expected"], args.timeout)
                    row = {"case": case, "engine": engine, "threads": threads, "argv": command, "status": "verified"}
                    verification.append(row)
                    combinations.append((row, definition))
                    if args.session_reuse and engine != "logq_raw":
                        for repetitions in dict.fromkeys(args.repetitions):
                            command_n = session_command(command, repetitions)
                            expected_n = repeated_answer(definition, definition["expected"], repetitions)
                            execute(command_n, definition, expected_n, args.timeout)
                            session_row = {**row, "argv": command_n, "repetitions": repetitions,
                                           "expected": expected_n, "execution_mode": "one_clickhouse_process"}
                            session_verification.append(session_row)
                            session_combinations.append((session_row, definition))
        metadata["query_preflight_wall_seconds"] = time.perf_counter() - verification_started
        save()
        if not args.validate_only:
            for row, definition in combinations:
                for _ in range(args.warmup):
                    execute(row["argv"], definition, definition["expected"], args.timeout)
            for repetitions in dict.fromkeys(args.repetitions):
                for iteration in range(args.runs):
                    ordered = combinations if iteration % 2 == 0 else list(reversed(combinations))
                    for row, definition in ordered:
                        sample_row = {**row, "repetitions": repetitions, "iteration": iteration, "status": "running", "samples": []}
                        results.append(sample_row)
                        for _ in range(repetitions):
                            sample_row["samples"].append(execute(row["argv"], definition, definition["expected"], args.timeout))
                        preparation = prepared["preparation"].get(row["engine"], {}).get("sample", zero_prep)
                        sample_row.update(status="ok", **amortized(preparation, sample_row["samples"]))
                        charged_contract = contract if row["engine"] in ["parquet", "persisted"] else zero_prep
                        sample_row["total_with_contract_wall_seconds"] = sample_row["total_wall_seconds"] + charged_contract["wall_seconds"]
                        sample_row["total_with_contract_cpu_seconds"] = sample_row["total_cpu_seconds"] + charged_contract["user_cpu_seconds"] + charged_contract["system_cpu_seconds"]
                        save()
                        print(row["case"], row["engine"], f"t{row['threads']}", f"N={repetitions}", f"{sample_row['total_wall_seconds']:.4f}s including preparation", flush=True)
            for row, definition in session_combinations:
                for _ in range(args.warmup):
                    execute(row["argv"], definition, row["expected"], args.timeout)
            for iteration in range(args.runs):
                ordered = session_combinations if iteration % 2 == 0 else list(reversed(session_combinations))
                for row, definition in ordered:
                    sample_row = {**row, "iteration": iteration, "status": "running", "samples": []}
                    session_results.append(sample_row)
                    sample_row["samples"].append(execute(row["argv"], definition, row["expected"], args.timeout))
                    preparation = prepared["preparation"].get(row["engine"], {}).get("sample", zero_prep)
                    sample_row.update(status="ok", **amortized(preparation, sample_row["samples"]))
                    sample_row.update(query_count=row["repetitions"], process_count=1)
                    charged_contract = contract if row["engine"] in ["parquet", "persisted"] else zero_prep
                    sample_row["total_with_contract_wall_seconds"] = sample_row["total_wall_seconds"] + charged_contract["wall_seconds"]
                    sample_row["total_with_contract_cpu_seconds"] = sample_row["total_cpu_seconds"] + charged_contract["user_cpu_seconds"] + charged_contract["system_cpu_seconds"]
                    save()
                    print(row["case"], row["engine"], f"t{row['threads']}", f"N={row['repetitions']} one process",
                          f"{sample_row['total_wall_seconds']:.4f}s including preparation", flush=True)
            if not args.skip_rss:
                for row, definition in combinations:
                    row["rss_sample"] = explore.run_once(row["argv"], definition, definition["expected"], rss=True, timeout=args.timeout)
                for row, definition in session_combinations:
                    row["rss_sample"] = explore.run_once(row["argv"], definition, row["expected"], rss=True, timeout=args.timeout)
        # Hash after timing; a renamed/appended/replaced input must invalidate reuse.
        postcheck_started = time.perf_counter()
        if explore.sha256(source) != entry["sha256"] or explore.sha256(manifest_path) != manifest_hash:
            raise ValueError("source or manifest changed during experiment")
        for binary in binaries.values():
            if explore.sha256(binary["path"]) != binary["sha256"]:
                raise ValueError("binary changed during experiment")
        for path, digest in snapshots.items():
            if explore.sha256(path) != digest:
                raise ValueError("harness source changed during experiment")
        verify_artifacts(prepared_dir, prepared["artifacts"])
        metadata["provenance_postcheck_wall_seconds"] = time.perf_counter() - postcheck_started
        metadata["status"] = "complete"
    except Exception as error:
        metadata.update(status="failed", error=str(error))
        for row in results + session_results:
            row["status"] = "invalid_run"
        raise
    finally:
        metadata["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        save()
    summary = []
    for row in verification:
        for repetitions in dict.fromkeys(args.repetitions):
            observed = [item for item in results if item["case"] == row["case"] and item["engine"] == row["engine"] and item["threads"] == row["threads"] and item["repetitions"] == repetitions]
            if observed:
                values = [item["total_wall_seconds"] for item in observed]
                summary.append({"case": row["case"], "engine": row["engine"], "threads": row["threads"], "repetitions": repetitions,
                                "total_wall_mean": statistics.mean(values), "total_wall_sample_sd": statistics.stdev(values) if len(values) > 1 else None,
                                "total_cpu_mean": statistics.mean(item["total_cpu_seconds"] for item in observed),
                                "total_with_contract_wall_mean": statistics.mean(item["total_with_contract_wall_seconds"] for item in observed),
                                "total_with_contract_cpu_mean": statistics.mean(item["total_with_contract_cpu_seconds"] for item in observed)})
    explore.write_json(result_dir / "summary.json", summary)
    if args.session_reuse:
        session_summary = []
        for row in session_verification:
            observed = [item for item in session_results if all(item[key] == row[key] for key in ["case", "engine", "threads", "repetitions"])]
            if observed:
                values = [item["total_wall_seconds"] for item in observed]
                session_summary.append({key: row[key] for key in ["case", "engine", "threads", "repetitions", "execution_mode"]} |
                                       {"total_wall_mean": statistics.mean(values),
                                        "total_wall_sample_sd": statistics.stdev(values) if len(values) > 1 else None,
                                        "query_wall_mean": statistics.mean(item["query_wall_seconds"] for item in observed),
                                        "total_cpu_mean": statistics.mean(item["total_cpu_seconds"] for item in observed),
                                        "total_with_contract_wall_mean": statistics.mean(item["total_with_contract_wall_seconds"] for item in observed),
                                        "total_with_contract_cpu_mean": statistics.mean(item["total_with_contract_cpu_seconds"] for item in observed)})
        explore.write_json(result_dir / "session-summary.json", session_summary)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
