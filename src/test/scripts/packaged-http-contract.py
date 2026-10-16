#!/usr/bin/env python3
"""打包后 HTTP/真实 PostgreSQL 契约：本地协议替身不代表真实 Flink/Operator 验收。

运行前执行 sbt assembly，并准备 Docker。脚本只创建、暂停和删除自己的临时容器，
启动两个回环 HTTP 进程，不访问用户的 Kubernetes、PostgreSQL 或 RustFS。
"""

import json
import os
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request
import urllib.error
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

repo = Path(__file__).resolve().parents[3]
jar = repo / "target/scala-3.3.5/zio-flink-operator-lab-assembly-0.1.0-SNAPSHOT.jar"
objects = {}
lock = threading.Lock()


class Kube(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path.endswith(
            (
                "flinkdeployments",
                "flinksessionjobs",
                "flinkstatesnapshots",
                "flinkoperationlocks",
            )
        ):
            self.send(200, {"metadata": {"resourceVersion": "1"}, "items": []})
        elif path in objects:
            self.send(200, objects[path])
        else:
            self.send(404, {"kind": "Status", "code": 404, "message": "not found"})

    def do_POST(self):
        data = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
        collection = self.path.split("?")[0]
        assert collection.endswith("/flinkoperationlocks"), (
            "only test lock creates are allowed"
        )
        path = collection + "/" + data["metadata"]["name"]
        with lock:
            if path in objects:
                self.send(
                    409, {"kind": "Status", "code": 409, "message": "already exists"}
                )
                return
            data["metadata"].update(uid=str(uuid.uuid4()), resourceVersion="1")
            objects[path] = data
        self.send(201, data)

    def do_DELETE(self):
        assert "/flinkoperationlocks/" in self.path
        objects.pop(self.path, None)
        self.send(200, {"kind": "Status", "status": "Success"})


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def call(base, path, body=None, method=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        base + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)


def main():
    api = None
    processes = []
    container_id = None
    try:
        if not jar.is_file():
            raise RuntimeError("run sbt assembly first")
        # 临时 PostgreSQL 仅映射回环地址；无宿主目录或持久卷。
        container_name = "zio-e2e-http-" + uuid.uuid4().hex[:12]
        container_id = subprocess.check_output(
            [
                "docker",
                "run",
                "--rm",
                "-d",
                "--name",
                container_name,
                "-e",
                "POSTGRES_PASSWORD=local-test-only",
                "-e",
                "POSTGRES_DB=zio_contract",
                "-p",
                "127.0.0.1::5432",
                "postgres:17-alpine",
            ],
            text=True,
        ).strip()

        api = ThreadingHTTPServer(("127.0.0.1", 0), Kube)
        threading.Thread(target=api.serve_forever, daemon=True).start()
        pg_port = (
            subprocess.check_output(
                ["docker", "port", container_id, "5432/tcp"], text=True
            )
            .strip()
            .rsplit(":", 1)[1]
        )
        for attempt in range(100):
            ready = subprocess.run(
                [
                    "docker",
                    "exec",
                    container_id,
                    "pg_isready",
                    "-U",
                    "postgres",
                    "-d",
                    "zio_contract",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("temporary PostgreSQL did not become ready")
        with tempfile.TemporaryDirectory(prefix="zio-runtime-") as directory:
            config = Path(directory) / "kubeconfig"
            config.write_text(
                json.dumps(
                    {
                        "apiVersion": "v1",
                        "kind": "Config",
                        "clusters": [
                            {
                                "name": "contract",
                                "cluster": {
                                    "server": f"http://127.0.0.1:{api.server_port}"
                                },
                            }
                        ],
                        "contexts": [
                            {
                                "name": "contract",
                                "context": {"cluster": "contract", "user": "contract"},
                            }
                        ],
                        "current-context": "contract",
                        "users": [{"name": "contract", "user": {}}],
                    }
                )
            )
            env = {
                key: os.environ[key]
                for key in ("PATH", "JAVA_HOME", "LANG")
                if key in os.environ
            }
            env.update(
                KUBECONFIG=str(config),
                FLINK_NAMESPACE="analytics",
                ZIO_FLINK_METADATA_STORE="postgres",
                ZIO_FLINK_METADATA_JDBC_URL=f"jdbc:postgresql://127.0.0.1:{pg_port}/zio_contract",
                ZIO_FLINK_METADATA_USER="postgres",
                ZIO_FLINK_METADATA_PASSWORD="local-test-only",
                ZIO_FLINK_OPERATION_STORE="postgres",
                ZIO_FLINK_STATE_BACKEND="kubernetes",
                POSTGRES_HOST="127.0.0.1",
                POSTGRES_PORT=pg_port,
                POSTGRES_DB="zio_contract",
                POSTGRES_USER="postgres",
                POSTGRES_PASSWORD="local-test-only",
                ZIO_FLINK_SERVER_HOST="127.0.0.1",
            )
            bases = []
            for i in range(2):
                p = port()
                base = f"http://127.0.0.1:{p}"
                bases.append(base)
                log = open(Path(directory) / f"replica-{i}.log", "w")
                proc = subprocess.Popen(
                    [
                        str(Path(env["JAVA_HOME"]) / "bin" / "java")
                        if env.get("JAVA_HOME")
                        else "java",
                        "-Xmx256m",
                        "-jar",
                        str(jar),
                    ],
                    env=dict(env, ZIO_FLINK_SERVER_PORT=str(p)),
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                processes.append(proc)
                for attempt in range(100):
                    if proc.poll() is not None:
                        raise RuntimeError(f"replica {i} exited; see log")
                    try:
                        if call(base, "/readyz")[0] == 200:
                            break
                    except (OSError, ValueError):
                        pass
                    time.sleep(0.15)
                else:
                    raise RuntimeError(f"replica {i} did not become ready")
            catalog = "runtime-" + uuid.uuid4().hex[:8]
            at = "2026-10-16T00:00:00Z"
            assert (
                call(
                    bases[0],
                    "/v1/catalogs",
                    {
                        "id": catalog,
                        "type": "postgres",
                        "endpoint": "jdbc:postgresql://test/db",
                        "database": "test",
                    },
                )[0]
                == 201
            )
            snapshot = {
                "catalogId": catalog,
                "schema": "public",
                "version": "v1",
                "observedAt": at,
                "tables": [
                    {
                        "catalogId": catalog,
                        "schema": "public",
                        "name": "orders",
                        "columns": [
                            {"name": "id", "dataType": "BIGINT", "nullable": False}
                        ],
                        "observedAt": at,
                    }
                ],
            }
            assert (
                call(bases[0], f"/v1/catalogs/{catalog}/snapshots", snapshot)[0] == 201
            )
            status, tables = call(
                bases[1], f"/v1/catalogs/{catalog}/tables?schema=public"
            )
            assert status == 200 and tables["items"][0]["name"] == "orders"
            assert call(bases[1], f"/v1/catalogs/{catalog}/schemas")[1]["items"] == [
                {"name": "public"}
            ]
            sink = "acceptance_sink_" + uuid.uuid4().hex[:8]
            sql = f"INSERT INTO {sink} SELECT * FROM input_one a, input_two b WHERE a.id=b.id"
            status, edges = call(
                bases[0], "/v1/lineage/sql", {"sql": sql, "jobId": catalog}
            )
            assert status == 201 and {e["source"]["name"] for e in edges["items"]} == {
                "input_one",
                "input_two",
            }, edges
            status, graph = call(bases[1], "/v1/lineage/graph?root=" + sink)
            assert status == 200 and len(graph["items"]) == 2
            assert (
                call(
                    bases[1],
                    "/v1/lineage/sql",
                    {
                        "sql": "INSERT INTO acceptance_sink SELECT * FROM input_one a, input_two b WHERE"
                    },
                )[0]
                == 400
            )
            request = "runtime-" + uuid.uuid4().hex
            status, one = call(
                bases[0],
                f"/v1/deployments/missing/resume?requestId={request}",
                method="POST",
            )
            assert status == 202, one
            status, two = call(
                bases[1],
                f"/v1/deployments/missing/resume?requestId={request}",
                method="POST",
            )
            assert status == 202 and two["operationId"] == one["operationId"], two
            for attempt in range(100):
                status, op = call(bases[1], "/v1/operations/" + one["operationId"])
                if op["state"] == "FAILED":
                    break
                time.sleep(0.1)
            else:
                raise RuntimeError("missing target was not terminalized")
            assert "does not exist" in json.dumps(op)
            print(
                "PASS packaged HTTP replicas: readiness; PostgreSQL catalog/schema write on A read on B; Flink AST comma lineage; malformed SQL400; same requestId same operation; missing target FAILED read across replicas"
            )
            subprocess.run(
                ["docker", "pause", container_id], check=True, stdout=subprocess.DEVNULL
            )
            try:
                started = time.monotonic()
                status, body = call(bases[0], "/readyz")
                elapsed = time.monotonic() - started
                assert status == 503, (status, body)
                assert elapsed < 4.5, (
                    f"readiness exceeded the 3s deadline: {elapsed:.2f}s"
                )
                assert call(bases[0], "/healthz")[0] == 200
                print(
                    f"PASS dependency outage: readyz=503 in {elapsed:.2f}s; healthz=200"
                )
            finally:
                subprocess.run(
                    ["docker", "unpause", container_id],
                    check=True,
                    stdout=subprocess.DEVNULL,
                )
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if api is not None:
            api.shutdown()
            api.server_close()
        if container_id is not None:
            subprocess.run(
                ["docker", "rm", "-f", container_id],
                check=True,
                stdout=subprocess.DEVNULL,
            )
        print("CLEANUP temporary PostgreSQL container and both HTTP processes removed")


if __name__ == "__main__":
    main()
