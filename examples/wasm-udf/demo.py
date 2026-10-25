#!/usr/bin/env python3
"""Exercise the real Dozer CLI, webhook source, SQL WASM call and dummy sink.

Uses only Python's standard library and an already-built local Dozer binary.
All listener/storage settings are private to this process. No external services.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dozer", type=Path, default=Path(__file__).resolve().parents[2] / "target/debug/dozer")
    parser.add_argument("--udf-format", choices=("mapping", "typed", "tagged"), default="mapping")
    args = parser.parse_args()
    binary = args.dozer.resolve(strict=True)
    example = Path(__file__).resolve().parent
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]

    with tempfile.TemporaryDirectory(prefix="dozer-wasm-demo-") as work:
        work = Path(work)
        config = example / "dozer-config.yaml"
        if args.udf_format != "mapping":
            source = config.read_text()
            mapping = "  wasm:\n    my_function: udfs/wasm/my_function"
            if source.count(mapping) != 1:
                raise RuntimeError("Expected exactly one WASM mapping in the example")
            config = work / "dozer-config.yaml"
            entry = ("  - type: wasm\n    name: my_function\n    config:\n      path: udfs/wasm/my_function"
                     if args.udf_format == "typed" else
                     "  - name: my_function\n    config: !Wasm\n      path: udfs/wasm/my_function")
            config.write_text(source.replace(mapping, entry))
        # inserted_at enables the existing dummy sink's record debug logging.
        sql = "SELECT my_function(a,b) AS sum, NOW() AS inserted_at INTO computed FROM numbers"
        command = [str(binary), "--ignore-pipe", "-c", str(config),
                   "--config-overrides", "/connections/0/config/Webhook/port=" + str(port),
                   "--config-overrides", "/home_dir=" + json.dumps(str(work / "data")),
                   "--config-overrides", "/sql=" + json.dumps(sql), "run"]
        env = dict(os.environ, RUST_LOG="info,dozer_cli::pipeline::dummy_sink=debug")
        log_path = work / "dozer.log"
        with log_path.open("w") as log:
            process = subprocess.Popen(command, cwd=example, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 30
            while True:
                if process.poll() is not None:
                    raise RuntimeError("Dozer exited during startup\n" + log_path.read_text())
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError("Webhook startup timed out\n" + log_path.read_text())
                    time.sleep(0.1)
            # This connector maps an omitted nullable field to SQL NULL; it does
            # not currently accept explicit JSON null for integer fields.
            payload = {"numbers": [{"a": 3, "b": 4}, {"a": 3, "b": 5}, {"b": 4}]}
            request = urllib.request.Request(f"http://127.0.0.1:{port}/numbers", data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}, method="POST")
            try:
                with urllib.request.urlopen(request, timeout=5) as response:
                    assert response.status == 200, response.status
            except urllib.error.HTTPError as error:
                raise RuntimeError(error.read().decode() + "\n" + log_path.read_text()) from error
            deadline = time.monotonic() + 10
            while True:
                text = log_path.read_text()
                if all(f"values: [{value}, Timestamp" in text for value in ("Int(7)", "Int(8)", "Null")):
                    break
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("Missing expected SQL output\n" + text)
                time.sleep(0.1)
            for line in text.splitlines():
                if "Received record:" in line:
                    print(line)
            print(f"PASS ({args.udf_format} configuration): local webhook -> Dozer SQL -> compiled AssemblyScript -> sink: 7, 8, NULL")
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
