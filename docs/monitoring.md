# 状态监控

监控分为两类：Flink 作业 CR 的运行状态，以及 `FlinkStateSnapshot` 的 checkpoint/savepoint 请求状态。

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
curl -fsS -X DELETE 'http://127.0.0.1:18080/v1/snapshots/orders-savepoint?namespace=flink-lineage-test'
```

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

`checkpointInfo` 与 `savepointInfo` 是 Operator 写入的摘要。逐个 checkpoint 的大小、耗时、完成数量和 task/subtask 明细不在当前 Kubernetes API 适配器中；这些数据需要 Flink REST `/jobs/{jobId}/checkpoints`。
