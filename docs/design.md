# 设计与协议

项目只有一个核心：用 ZIO 编写 Flink 控制程序，通过 Kubernetes API 操作 Flink Kubernetes Operator，完成任务提交、状态观察和状态快照。

```text
CLI / HTTP
    |
    v
ZIO OperatorProgram
    |
    v
KubernetesApi
    |
    v
Kubernetes API Server
    |
    v
Flink Kubernetes Operator
    |
    v
FlinkDeployment / FlinkSessionJob / FlinkStateSnapshot
```

对应的可交互图：

- [架构图](diagrams/architecture.html)
- [数据流图](diagrams/dataflow.html)

## 资源边界

| 资源 | 用途 |
| --- | --- |
| `FlinkDeployment` | 当前 CLI 模板创建 Application Cluster 并运行一个 Flink Job |
| `FlinkSessionJob` | 把 Job 提交到已有的 Session Cluster |
| `FlinkStateSnapshot` | 请求 savepoint 或 checkpoint，并观察快照状态 |

所有资源都通过 Kubernetes API Server 提交。程序不调用 `kubectl`，不直接操作 JobManager Pod。

`apply` 使用 Server-Side Apply，旧的 `savepoint` 与 `suspend-savepoint` 使用局部 merge patch，作为兼容入口保留。新的 checkpoint/savepoint 流程使用 `FlinkStateSnapshot`，由 Operator 记录请求、结果路径和失败信息。

## 状态来源

任务状态来自 Flink CR 的 `status`：

- 生命周期：`lifecycleState`、`jobManagerDeploymentStatus`、`reconciliationStatus`。
- Job：`jobStatus.jobId`、`jobStatus.jobName`、`jobStatus.state`、`startTime`、`updateTime`。
- 条件与错误：`conditions`、`status.error`。
- checkpoint 摘要：`jobStatus.checkpointInfo`。
- savepoint 摘要：`jobStatus.savepointInfo`，包括最后路径和历史记录。

快照请求的状态来自 `FlinkStateSnapshot.status`，包括 `state`、`path`、`error`、`failures`、`triggerId` 和时间戳。字段以目标集群安装的 CRD 为准。

当前监控是 Operator CR 状态和 checkpoint/savepoint 摘要。逐个 checkpoint 的 task/subtask 明细需要额外接入 Flink REST，本项目当前没有把它混入 Kubernetes API 适配器。

## ZIO 边界

业务程序使用 `ZIO[KubernetesApi, Throwable, A]`。`KubernetesApi` 是副作用端口，`KubernetesApiLive` 是生产实现，fake API 用于测试。

`ZStream` 消费 Kubernetes watch 事件，reducer 按事件到达顺序维护状态。`Scope` 负责关闭 watch，`Schedule` 负责有界重试，`ZLayer` 负责组装 Kubernetes 客户端。HTTP 服务另外运行一个 ZIO 后台轮询器，按固定间隔 list 三类 Flink CR，把状态合并到共享状态后端。

资源版本始终按不透明字符串处理。当前 watch 支持事件解析、EOF 重连、状态合并和错误传播；可靠的 list → resourceVersion watch、410 Gone relist 和跨重启游标持久化仍需要单独实现和验证。

## 多副本与状态后端

HTTP 副本不保存本地会话状态，Service 可以把请求路由到任意副本。状态后端由 `ZIO_FLINK_STATE_BACKEND` 选择：

| 模式 | 统一状态来源 | 用途 |
| --- | --- | --- |
| `kubernetes` | FlinkDeployment、FlinkSessionJob、FlinkStateSnapshot CR | Kubernetes 部署默认模式，CR 是事实源，副本直接读取 API Server |
| `postgres` | `zio_flink_operator_state` 表 | 裸机运行时保存最新状态、资源版本和最多 100 条生命周期事件，副本读取同一张表 |

PostgreSQL 只保存状态观测和资源版本，不保存 Flink checkpoint/savepoint 二进制。savepoint 仍由 Flink 运行时写入 RustFS。两种模式都需要 Kubernetes API，因为本项目的提交和 Operator 状态来源始终是 Kubernetes API。

状态接口：

- `GET /v1/state?namespace=<namespace>`：列出当前后端中的状态记录。
- `GET /v1/state/<kind>/<name>?namespace=<namespace>`：读取一条状态记录。

Kubernetes 模式不会额外引入 ConfigMap 缓存，避免 CR 与缓存出现双重事实源。裸机 PostgreSQL 模式的 upsert 使用 Kubernetes `resourceVersion` 防止较旧观测覆盖较新观测。

本地 OrbStack 只有一个节点，两个 Pod 只能证明进程副本和 Service 路由。目标集群清单使用一个 NodePort Service、两个副本、hostname 反亲和和 `DoNotSchedule`；两台机器的跨节点调度需要目标集群可达、节点标签正常、Operator 已安装，并通过滚动重启和状态接口验证。每个副本都轮询同一 Kubernetes API，状态统一来自 CR；这会按副本数增加 list 请求量，后续高规模场景应增加 leader election 或集中式 watch。
