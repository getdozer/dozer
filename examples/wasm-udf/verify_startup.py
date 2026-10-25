#!/usr/bin/env python3
"""Assert that invalid UDF configuration fails before a loopback source listens.

Uses the actual built CLI, synthetic fixtures, temporary storage and no services.
Each subprocess is stopped and its complete output retained in the JSON result.
"""
import argparse
import json
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time


def stop(process):
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def main():
    parser = argparse.ArgumentParser()
    example = Path(__file__).resolve().parent
    parser.add_argument("--dozer", type=Path, default=example.parents[1] / "target/debug/dozer")
    parser.add_argument("--results", type=Path, required=True)
    args = parser.parse_args()
    binary = args.dozer.resolve(strict=True)
    template = (example / "dozer-config.yaml").read_text()
    prefix = template.split("udfs:\n", 1)[0]
    results = []
    with tempfile.TemporaryDirectory(prefix="dozer-wasm-startup-") as tmp:
        tmp = Path(tmp)
        malformed = tmp / "invalid.wasm"
        malformed.write_bytes(b"not a WASM module")
        module = "udfs/wasm/my_function"
        cases = [
            ("unused_missing_module", f"udfs:\n  wasm:\n    my_function: {module}\n    unused_function: {tmp}/missing.wasm\n", "SELECT a INTO computed FROM numbers;", "cannot initialize WASM UDF unused_function"),
            ("unused_malformed_module", f"udfs:\n  wasm:\n    unused_function: {malformed}\n", "SELECT a INTO computed FROM numbers;", "Invalid WASM module"),
            ("unused_missing_export", f"udfs:\n  wasm:\n    unused_function: {module}\n", "SELECT a INTO computed FROM numbers;", "WASM export unused_function"),
            ("no_sql_missing_module", f"udfs:\n  wasm:\n    unused_function: {tmp}/missing.wasm\n", None, "cannot initialize WASM UDF unused_function"),
            ("duplicate_list_name", f"udfs:\n  - name: my_function\n    type: wasm\n    config: {{path: {module}}}\n  - name: my_function\n    config: !Wasm {{path: {module}}}\n", "SELECT my_function(a,b) INTO computed FROM numbers;", "duplicate UDF name"),
            ("reserved_builtin_name", f"udfs:\n  wasm:\n    abs: {module}\n", "SELECT a INTO computed FROM numbers;", "conflicts with a built-in SQL function"),
        ]
        for name, udfs, sql, expected in cases:
            work = tmp / name
            work.mkdir()
            with socket.socket() as reserve:
                reserve.bind(("127.0.0.1", 0))
                port = reserve.getsockname()[1]
            config = prefix.replace("port: 18765", f"port: {port}").replace("home_dir: .dozer", "home_dir: " + json.dumps(str(work / "data")))
            if sql is None:
                config = config.replace("table_name: computed", "table_name: numbers")
            config += udfs + ("sql: " + sql + "\n" if sql else "")
            path = work / "dozer-config.yaml"
            path.write_text(config)
            log_path = work / "output.log"
            with log_path.open("w") as log:
                process = subprocess.Popen([str(binary), "--ignore-pipe", "-c", str(path), "run"], cwd=example, stdout=log, stderr=subprocess.STDOUT)
            listened = False
            timed_out = False
            try:
                deadline = time.monotonic() + 15
                while process.poll() is None:
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.05):
                            listened = True
                            break
                    except OSError:
                        pass
                    if time.monotonic() >= deadline:
                        timed_out = True
                        break
                    time.sleep(0.02)
                natural_exit = process.poll()
            finally:
                stop(process)
            output = log_path.read_text()
            passed = not listened and not timed_out and natural_exit not in (None, 0) and expected in output
            result = {"case": name, "passed": passed, "listener_observed": listened, "timed_out": timed_out, "exit_code_before_cleanup": natural_exit, "expected_error": expected, "output": output}
            results.append(result)
            print(f"{'PASS' if passed else 'FAIL'}: {name}", flush=True)
    args.results.write_text(json.dumps(results, indent=2) + "\n")
    return 0 if all(row["passed"] for row in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
