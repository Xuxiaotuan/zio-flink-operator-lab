# 状态监控

监控分为两类：Flink 作业 CR 的运行状态，以及 `FlinkStateSnapshot` 的 checkpoint/savepoint 请求状态。

状态数据流见 [数据流图](diagrams/dataflow.html)，组件关系见 [架构图](diagrams/architecture.html)。

## CLI

查询资源的完整 CR：

```sh
sbt "run status deployment --name orders --namespace flink-lineage-test"
sbt "run status session-job --name orders-job --namespace flink-lineage-test"
sbt "run status state-snapshot --name orders-savepoint --namespace flink-lineage-test"
```

持续观察任务和快照：

```sh
sbt "run watch deployment --name orders --namespace flink-lineage-test"
sbt "run watch session-job --name orders-job --namespace flink-lineage-test"
sbt "run watch state-snapshot --name orders-savepoint --namespace flink-lineage-test"
```

watch 输出是状态投影，不代表业务输出已经正确。需要同时检查 Job 状态、Pod、业务结果和快照路径。

## HTTP

服务端口提供规范化状态：

```sh
curl -fsS 'http://127.0.0.1:18080/v1/deployments/orders/status?namespace=flink-lineage-test'
curl -fsS 'http://127.0.0.1:18080/v1/deployments?namespace=flink-lineage-test'
curl -fsS 'http://127.0.0.1:18080/v1/snapshots/orders-savepoint?namespace=flink-lineage-test'
curl -fsS 'http://127.0.0.1:18080/v1/snapshots?namespace=flink-lineage-test'
curl -fsS 'http://127.0.0.1:18080/v1/state?namespace=flink-lineage-test'
curl -fsS 'http://127.0.0.1:18080/v1/state/deployment/orders?namespace=flink-lineage-test'
curl -fsS -X DELETE 'http://127.0.0.1:18080/v1/snapshots/orders-savepoint?namespace=flink-lineage-test'
```

`/v1/state` 返回当前配置的后端：Kubernetes 模式直接读取三类 Flink CR；PostgreSQL 模式返回共享表中的最新观测和 `history` 生命周期事件。服务启动后，每个副本都会按 `ZIO_FLINK_STATE_POLL_INTERVAL_SECONDS`（默认 15 秒）读取三类 CR；轮询只读 Kubernetes，重复写入通过资源版本和幂等 upsert 合并。它用于检查多个服务副本是否读取同一份状态，不替代 Operator 的 CR 状态。

提交 FlinkDeployment 仍使用同一个控制面：

```sh
curl -fsS -X POST 'http://127.0.0.1:18080/v1/deployments?namespace=flink-lineage-test&dryRun=true' \
  -H 'Content-Type: application/json' \
  --data @examples/flinkdeployment.json
```

创建 checkpoint 或 savepoint 请求：

```sh
curl -fsS -X POST 'http://127.0.0.1:18080/v1/snapshots?namespace=flink-lineage-test' \
  -H 'Content-Type: application/json' \
  --data '{"targetKind":"deployment","targetName":"orders","snapshotName":"orders-savepoint","type":"savepoint"}'
```

`type` 可以是 `savepoint` 或 `checkpoint`。请求被 API Server 接受，只表示 CR 已提交；最终结果要读取对应 `FlinkStateSnapshot.status`。

## 字段含义

| 输出字段 | 来源 | 含义 |
| --- | --- | --- |
| `lifecycleState` | Flink CR | Operator 对资源生命周期的判断 |
| `jobState` | `status.jobStatus.state` | Flink Job 状态，例如 `RUNNING`、`FAILED`、`SUSPENDED` |
| `jobManagerDeploymentStatus` | Flink CR | JobManager 部署状态 |
| `conditions` / `error` | Flink CR | Operator 条件和错误信息 |
| `checkpoint` | `status.jobStatus.checkpointInfo` | 最近 checkpoint 的触发信息和时间摘要 |
| `savepoint` | `status.jobStatus.savepointInfo` | 最后 savepoint、路径、触发信息和历史摘要 |
| `path` | FlinkStateSnapshot.status | 本次快照完成路径 |
| `state` / `failures` | FlinkStateSnapshot.status | 本次快照状态和失败信息 |

`/v1/state` 中每个 item 还包含：

- `deleted`：轮询发现 PostgreSQL 中的历史资源已经从 Kubernetes 消失后标记为 `true`。
- `history`：状态变化、首次观测和删除事件；相同状态的重复轮询不会新增事件，最多保留 100 条。

`checkpointInfo` 与 `savepointInfo` 是 Operator 写入的摘要。逐个 checkpoint 的大小、耗时、完成数量和 task/subtask 明细不在当前 Kubernetes API 适配器中；这些数据需要 Flink REST `/jobs/{jobId}/checkpoints`。
