# 测试与证据

测试区分四个层次：程序生成资源、API Server 接受资源、Operator reconcile、Flink 任务和快照真实完成。低层测试不能替代高层运行证据。

## 本地验证

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
sbt -batch test
sbt -batch assembly
mvn -B -f job/pom.xml package -DskipTests
```

当前本地执行结果以本轮实际命令为准。新增覆盖 ResourceObserver 的 resourceVersion/410 relist、PolicyEngine 接入、提交 generation 等待验证、Snapshot CR、Kubernetes Operation CR、资源锁、CLI/HTTP typed operation、AsyncOperationWorker、PostgreSQL OperationStore 幂等键和 operation lifecycle 查询。

本轮实际结果：`sbt -batch test` 通过 99 个测试；`sbt -batch assembly` 成功生成 assembly；`mvn -B -f job/pom.xml package -DskipTests` 返回 `BUILD SUCCESS`；本地 `docker build -f job/Dockerfile` 成功生成带 S3 插件和 StatefulCounterJob 的测试镜像。

以下本地结果只说明代码级契约和构建通过，不能替代现场验收。目标集群的 PipelineRun、Operator reconcile 和两副本统一 operation 状态已经有现场证据；HTTP dry-run、策略请求一致性、活动资源互斥、Operation resourceVersion CAS、worker 阶段恢复、快照 UID/路径校验、删除 UID 前置条件、watch EOF 重连和 Evidence 审计也有回归测试。真实 checkpoint/savepoint 写入 RustFS、恢复、跨节点故障演练、锁租约接管，以及 FallbackDetected 的生产语义仍未完成。
- 覆盖 CLI/domain、类型化控制面、FlinkDeployment/FlinkStateSnapshot JSON、fake Kubernetes API、HTTP wire contract、HTTP 控制面状态/快照接口、统一状态后端配置与接口、watch 状态投影、checkpoint/savepoint 字段和重试策略。
- Maven Job：应以本轮命令的 `BUILD SUCCESS` 为准。
- `job/target/zio-flink-wordcount-0.1.0.jar` 同时包含 `StatefulCounterJob`；目标集群现场测试应使用 `examples/flinkdeployment-stateful.json`，确认 checkpoint 计数、savepoint `status.path` 和恢复后 `stateful-counter` 日志连续。

测试按职责分组：CLI/domain、typed control-plane、policy、operation store/worker、operation factory、ResourceObserver、fake Kubernetes API、HTTP contract、watch/retry、状态后端和状态轮询。

这些测试默认不连接真实 Kubernetes。HTTP contract test 使用本地 HTTP server，只验证 Kubernetes Java Client 的请求协议。

## 目标集群验证

本轮目标集群验证记录：

- K8s `v1.23.17`，两节点 `xjw`、`xxt`；Flink Kubernetes Operator `1.16.1` 已 Ready，并只 watch `bigdata-lab`。
- `zio-flink-operator` 两个 Pod 分别调度到 `xjw`、`xxt`，NodePort 为 `30882`；从两台节点访问 `/healthz` 均返回 `{"status":"ok"}`。
- 从两台节点访问 `/v1/state?namespace=bigdata-lab` 都读到同一个 `zio-word-count` CR，backend 为 `kubernetes`，生命周期为 `STABLE`。
- 通过 HTTP 提交 `FlinkDeployment` 后，Operator 让 JobManager、TaskManager 进入 Ready，Job 进入 `FINISHED`，并产生 jobId `475c0a0e218426706464212870d4e5cf`。

### 最新现场验收（2026-10-06）

- PipelineRun `zio-flink-operator-lab-582f4`（Jenkins build `#24`）成功：包含 StatefulCounterJob 的 Maven 打包、带 S3 插件的 Flink Job 镜像构建/推送，以及控制面 Deployment rollout。控制面镜像为 `build-24-ba815f757367`，测试 Job 镜像为 `100.97.53.78:5001/xxt/zio-flink-stateful-job:build-24-ba815f757367`。
- PipelineRun `zio-flink-operator-lab-96csb`（Jenkins build `#23`）成功：提交 `7e0a00b` 的测试、assembly、镜像推送和目标 Deployment rollout 均通过。最终镜像为 `build-23-7e0a00bfa84d`。
- 新镜像 Deployment 为 `2/2`，两个 Pod 分别运行在 `xxt` 和 `xjw`；NodePort `30882` 的 `/healthz` 和 `/readyz` 均返回 `{"status":"ok"}`。
- 使用 `dryrun-7e0a00b` 在目标集群执行 HTTP `POST /v1/deployments?namespace=bigdata-lab&dryRun=true`，API 返回 dry-run 对象预览；随后查询 `FlinkDeployment/dryrun-7e0a00b` 返回 `NotFound`，证明该请求未创建业务 CR，也未进入 operation worker。
- PipelineRun `zio-flink-operator-lab-jlkxm`（Jenkins build `#22`）成功：提交 `4232b75` 的测试、assembly、镜像推送和目标 Deployment rollout 均通过。最终镜像为 `build-22-4232b75b9b9a`。
- Deployment 为 `2/2`，两个 Pod 分别运行在 `xxt` 和 `xjw`；两个 NodePort 地址的 `/healthz` 均返回 `{"status":"ok"}`。
- 请求 `smoke-20261006-5` 的 operation `c3f6530c-9777-43b4-bbb5-8b35a831cc9a` 在两个节点返回完全相同的 `COMPLETED` 事件链：`SUBMITTED → WAITING_FOR_OBSERVATION → OBSERVED → VERIFICATION_SUCCEEDED`。
- 对应 `FlinkDeployment/zio-control-plane-smoke-5` 状态为 `READY/FINISHED/DEPLOYED`。这证明了 HTTP 提交、Operation CR、resourceVersion/watch 观察、Operator reconcile、自然结束任务判定和两副本统一读取状态。

这组证据不包含 checkpoint/savepoint 的成功路径。`FlinkStateSnapshot` API 和状态投影已经实现；对已结束 Job 的 savepoint operation `dfa0f661-22ad-4f07-9ccd-77101c6d95f3` 已现场验证为两个节点一致的 `FAILED`，审计事件保留 Operator 的 `ABANDONED` 原因（目标 Job 不在运行）。RustFS endpoint、S3 插件、Secret、运行中 Job 的真实快照路径和恢复连续性仍需单独验收，属于 `evidence_incomplete`。

此前 build `#11` 的 Source 阶段曾因 GitHub 连接重置而 `external_blocked`；后续 PipelineRun `#22` 已成功，因此当前提交已有完整 CI/CD 证据。历史失败仍保留用于说明重试配置的背景。

## 测试覆盖

| 层级 | 能证明 | 不能证明 |
| --- | --- | --- |
| Scala/domain/CLI | 参数、资源 JSON、快照类型和 patch | API Server 接受资源 |
| Fake Kubernetes API | 业务调用的资源类型、namespace、name、body 和顺序 | Java Client 的真实 HTTP 请求 |
| HTTP wire contract | SSA、merge patch、list/get/delete、重试和错误分类 | 当前集群权限、CRD 版本和 Operator 行为 |
| HTTP control-plane tests | deployment status、snapshot create/list/get/delete、状态字段序列化 | 真实 Flink 作业和快照结果 |
| Server-side dry-run | API Server 接受资源和权限 | Operator reconcile、Pod、Job、checkpoint/savepoint |
| 集群集成 | CR status、Pod、Job、快照路径和恢复结果 | 生产 HA、长期稳定性和业务正确性 |

## 当前状态模型

FlinkDeployment 和 FlinkSessionJob 的监控摘要包括 lifecycle、Job 状态、JobManager 状态、conditions、error、reconciliation、checkpointInfo 和 savepointInfo。

FlinkStateSnapshot 的监控摘要包括目标 Job、state、path、error、failures、triggerId 和时间戳。checkpoint/savepoint 请求只有在对应 Snapshot CR 完成并出现结果路径后，才算完成。

逐个 checkpoint 的大小、耗时、完成数量和 task/subtask 明细需要 Flink REST；当前测试没有把这些数据伪装成 Kubernetes CR 字段。

## 本地部署证据

已验证的本地部署检查：

```sh
kubectl apply --dry-run=client -k deploy/local
kubectl apply -k deploy/local
kubectl -n flink-lineage-test rollout status deployment/zio-flink-operator --timeout=120s
```

本地 Deployment 为两个副本，OrbStack 当前只有一个节点。健康检查和 deployment dry-run 可以验证服务进程、Service、RBAC 和 API Server 访问；不能证明跨节点高可用或真实 Flink Job。

## 真实集群验收

在目标集群按以下顺序记录证据：

1. `apply --dry-run` 返回成功。
2. 真实 apply 返回成功，CR 进入 Operator 处理状态。
3. JobManager、TaskManager 和 Job 进入预期状态。
4. `FlinkStateSnapshot` 的 state、path、error、failures 与触发请求一致。
5. 从 savepoint 恢复后，Job 状态和业务状态连续。

如果只拿到 HTTP 200、CR 已创建或旧的 `lastSavepoint`，标记为 `evidence_incomplete`。如果被集群、Operator、权限、存储或 kubeconfig 阻塞，标记为 `external_blocked`，保留阻塞条件。
