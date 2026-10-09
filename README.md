# ZIO Flink Operator Control Plane

这是一个 Scala 3 + ZIO HTTP 控制面：通过 Kubernetes API 调用 Flink Kubernetes Operator，提交 `FlinkDeployment`、`FlinkSessionJob` 和 `FlinkStateSnapshot`，并观察任务、checkpoint、savepoint 与 Operator 状态。项目北极星是：**类型化、可审计、可验证结果的 Flink 操作控制面**。

程序只提供 HTTP 入口，不调用 `kubectl`，不直接操作 JobManager。Kubernetes CR 是期望状态和状态来源，Flink Kubernetes Operator 负责实际 reconcile。

从 [文档入口](docs/README.md) 开始。

## 架构与数据流

核心链路是：`HTTP API → ZIO 控制面 → Kubernetes API → Flink Kubernetes Operator → Flink CR/Flink 作业`。

- 部署拓扑截图：

  ![Kubernetes 部署拓扑](docs/diagrams/deployment.png)

- 控制面架构截图：

  ![控制面架构](docs/diagrams/architecture.png)

- 状态与数据流截图：

  ![状态与数据流](docs/diagrams/dataflow.png)

- [交互式架构图](docs/diagrams/architecture.html)：组件边界、职责和部署关系。
- [交互式 Kubernetes 部署图](docs/diagrams/deployment.html)：GitHub、Jenkins、Registry、两节点和 Flink 运行时。
- [交互式数据流图](docs/diagrams/dataflow.html)：提交、状态观察、快照和 RustFS 存储链路。
- [设计与协议](docs/design.md)：Kubernetes API-only 约束、资源模型、状态语义和多副本边界。

## 最短流程

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export FLINK_NAMESPACE=flink-lineage-test

sbt -batch test
sbt run
curl -fsS http://127.0.0.1:8080/healthz
```

浏览器工作台位于 [http://127.0.0.1:8080/](http://127.0.0.1:8080/)。它与 API 使用同一个 HTTP 服务，启动时从只读 `/v1/config` 读取服务默认 namespace，提供作业状态、发布、Savepoint 操作和 Operation 轮询；CLI 不属于当前运行入口。服务没有认证授权，必须只在本地或受限内网开放。

服务启动后，也可以通过 HTTP 提交部署并查询操作：

```sh
curl -fsS -X POST "http://127.0.0.1:8080/v1/deployments?namespace=$FLINK_NAMESPACE&requestId=orders-apply-1" \
  -H 'Content-Type: application/json' \
  --data @examples/flinkdeployment.json

curl -fsS "http://127.0.0.1:8080/v1/operations/<operationId>"
curl -fsS "http://127.0.0.1:8080/v1/state/deployment/orders?namespace=$FLINK_NAMESPACE"
```

项目自带的示例 Job：

```sh
mvn -B -f job/pom.xml package -DskipTests
```

JAR 必须位于 Flink Pod 可访问的位置，并在 `FlinkDeployment.spec.job` 中声明 `jarURI` 和 `entryClass`。

## 快照与监控

使用 HTTP 请求创建 checkpoint 或 savepoint：

```sh
curl -fsS -X POST "http://127.0.0.1:8080/v1/snapshots?namespace=$FLINK_NAMESPACE&requestId=orders-savepoint-1" \
  -H 'Content-Type: application/json' \
  --data '{"targetKind":"deployment","targetName":"orders","snapshotName":"orders-savepoint","type":"savepoint"}'

curl -fsS "http://127.0.0.1:8080/v1/operations/<operationId>"
curl -fsS "http://127.0.0.1:8080/v1/snapshots/orders-savepoint?namespace=$FLINK_NAMESPACE"
```

写请求返回 `202` 和 `operationId`，worker 随后推进 Kubernetes reconcile 和 verification。相同 `requestId` 重试会返回同一个操作。状态字段和证据含义见 [状态监控](docs/monitoring.md)。

状态轮询间隔由 `ZIO_FLINK_STATE_POLL_INTERVAL_SECONDS` 控制，默认 15 秒。Kubernetes 模式以 CR 为事实源；PostgreSQL 模式会把轮询得到的状态和最多 100 条生命周期事件写入共享表。

## 性能评估

当前服务适合低到中等频率的 Flink 控制作业。目标集群的控制面基线为：60 秒、16 并发、9507 次请求全部成功；`/healthz` P95 约 159 ms，`/v1/state` P95 约 512.2 ms。10 分钟双节点稳定性测试完成 468 次请求、0 错误、P95 约 66.4 ms，期间无 Pod 重启。

这些指标包含网络和 Kubernetes API 延迟，只代表控制面读请求基线，不代表 Flink 作业吞吐容量。每个副本按固定间隔读取三类 CR，轮询请求量随副本数线性增加。

裸机可把状态观测和操作审计写入 PostgreSQL：

```sh
export ZIO_FLINK_STATE_BACKEND=postgres
export ZIO_FLINK_OPERATION_STORE=postgres
export POSTGRES_HOST=100.82.226.63
export POSTGRES_PORT=30660
export POSTGRES_DB=xxt
export POSTGRES_USER=root
export POSTGRES_PASSWORD='由 Secret 注入'
sbt run
```

Kubernetes 部署默认使用 API Server 中的 Flink CR 作为多副本共享状态，不需要额外缓存。savepoint 和 checkpoint 文件仍由 Flink 运行时写入 RustFS。

## 版本与边界

项目使用 Scala 3.3.5、ZIO 2.1.11、ZIO Streams 2.1.11、Kubernetes Java Client 20.0.1，建议 JDK 17。

当前服务读取 Operator CR 的状态摘要。逐个 checkpoint 的 task/subtask 明细需要 Flink REST，当前不属于 Kubernetes API 适配器。目标集群的 Operator reconcile、两副本统一 operation 状态、checkpoint/savepoint 写入 RustFS、Savepoint 恢复连续性、控制面 Pod 故障演练、FlinkOperationLock 租约接管和 PostgreSQL 双节点运行已有真实验收记录；压力和长稳指标覆盖控制面接口，不代表生产容量。

构建、Kubernetes/RustFS 配置和本地多副本部署见 [部署](docs/deployment.md)；验证记录见 [测试与证据](docs/testing.md)。
