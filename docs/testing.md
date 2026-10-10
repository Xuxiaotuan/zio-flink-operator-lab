# 测试与证据

测试区分四个层次：程序生成资源、API Server 接受资源、Operator reconcile、Flink 任务和快照真实完成。低层测试不能替代高层运行证据。

## 本地验证

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
sbt -batch test
sbt -batch assembly
mvn -B -f job/pom.xml package -DskipTests
```

当前本地执行结果以本轮实际命令为准。新增覆盖 ResourceObserver 的 resourceVersion/410 relist、PolicyEngine 接入、提交 generation 等待验证、Snapshot CR、Kubernetes Operation CR、资源锁、HTTP typed operation、AsyncOperationWorker、PostgreSQL OperationStore 幂等键和 operation lifecycle 查询。

本轮基线结果：`sbt -batch test` 通过 118 个测试；`sbt -batch assembly` 成功生成 assembly（本轮 SHA-1 `d033d01d382e30ec0981e4ce35469cef8e7ffbfe`）；`node --test src/test/web/app.test.mjs` 通过 8 个测试，`node --check src/main/resources/web/app.js` 通过，`mvn -B -f job/pom.xml package -DskipTests` 成功。

以下本地结果只说明代码级契约和构建通过，不能替代现场验收。目标集群的 PipelineRun、Operator reconcile、两副本统一 operation 状态、checkpoint/savepoint 写入 RustFS、savepoint 恢复、单副本故障演练、锁租约接管和 PostgreSQL 双节点运行已有现场证据；HTTP dry-run、策略请求一致性、活动资源互斥、Operation resourceVersion CAS、worker 阶段恢复、快照 UID/路径校验、删除 UID 前置条件、watch EOF 重连和 Evidence 审计也有回归测试。控制面压力与长稳测试的结果记录在本页最新现场验收中。

历史现场记录：StatefulCounterJob 已通过 RustFS checkpoint、FlinkStateSnapshot savepoint 和新 Deployment 恢复；该记录保留用于说明早期 Operator 行为，最新恢复验收以 2026-10-10 记录为准。

### 最新现场验收（2026-10-10）

- PipelineRun `zio-flink-operator-lab-jd8bc`（Jenkins build `#37`）成功，提交 `c44502a`；目标 Deployment 镜像为 `build-37-c44502a7cb3f`，两个副本分别在 `xjw`、`xxt`，均 `1/1`、重启 0 次。
- 第一轮恢复请求 `zio-e2e-restore-20261010-v2` 没有产生资源副作用，Operation 进入 `UNCERTAIN`；原因是 ujson 把 Scala `Long` 渲染成字符串，API Server 拒绝 `savepointRedeployNonce` 的 CRD 类型。该失败促成显式 `ujson.Num` 编码和数值类型回归测试。
- 修复后的 Operation `cc2b7a3f-0188-342f-8f05-62851aca2dac` 为 `COMPLETED`，事件包含 `SUBMITTED → WAITING_FOR_OBSERVATION → OBSERVED → VERIFICATION_SUCCEEDED`；generation/observedGeneration 都为 `3`，`lastReconciledSpec` 包含 `upgradeMode=savepoint`、源 Savepoint 和 `savepointRedeployNonce=1`。
- Flink Job ID 从 `e1a10ea8e1efb2936b3f176fd60a6e80` 变为 `c4758c4cf3758a58df9384d511e1c205`。JobManager 日志记录 `Restoring job ... from Savepoint`，Flink REST 显示 `restored=1`、`completed=10`，最新 checkpoint 路径为 `s3://flink-savepoints/zio-e2e-20261010/checkpoints/c4758c4cf3758a58df9384d511e1c205/chk-15`；源 Savepoint 的 RustFS `_metadata` HEAD 返回 HTTP 200。
- 验证完成后删除了本轮 `zio-e2e-*` 的 FlinkDeployment、FlinkStateSnapshot、FlinkOperation、JobManager、TaskManager 和 Service；复查没有临时资源残留。RustFS Savepoint 对象保留为审计和恢复证据。

现场故障演练：删除 xjw 节点上的一个控制面 Pod 后，xxt 节点副本持续返回 `{"status":"ok"}`，Deployment 自动补回 xjw 副本并恢复 `2/2`；Flink 作业和 Kubernetes CR 未受影响。
- 覆盖 HTTP/domain、类型化控制面、FlinkDeployment/FlinkStateSnapshot JSON、fake Kubernetes API、HTTP wire contract、HTTP 控制面状态/快照接口、统一状态后端配置与接口、watch 状态投影、checkpoint/savepoint 字段、回退审计、锁租约续期与接管、重试策略。
- Maven Job：应以本轮命令的 `BUILD SUCCESS` 为准。
- `job/target/zio-flink-wordcount-0.1.0.jar` 同时包含 `StatefulCounterJob`；目标集群现场测试应使用 `examples/flinkdeployment-stateful.json`，确认 checkpoint 计数、savepoint `status.path` 和恢复后 `stateful-counter` 日志连续。

测试按职责分组：HTTP/domain、typed control-plane、policy、operation store/worker、operation factory、ResourceObserver、fake Kubernetes API、HTTP contract、watch/retry、状态后端和状态轮询。

这些测试默认不连接真实 Kubernetes。HTTP contract test 使用本地 HTTP server，只验证 Kubernetes Java Client 的请求协议。

## 目标集群验证

本轮目标集群验证记录：

- K8s `v1.23.17`，两节点 `xjw`、`xxt`；Flink Kubernetes Operator `1.16.1` 已 Ready，并只 watch `bigdata-lab`。
- `zio-flink-operator` 两个 API Pod 分别调度到 `xjw`、`xxt`；前端 `zio-flink-operator-ui` 单副本通过 NodePort `30882` 对外提供页面和反向代理，从两台节点访问 `/healthz` 均返回 `{"status":"ok"}`。
- 从两台节点访问 `/v1/state?namespace=bigdata-lab` 都读到同一个 `zio-word-count` CR，backend 为 `kubernetes`，生命周期为 `STABLE`。
- 通过 HTTP 提交 `FlinkDeployment` 后，Operator 让 JobManager、TaskManager 进入 Ready，Job 进入 `FINISHED`，并产生 jobId `475c0a0e218426706464212870d4e5cf`。

### 历史现场验收（2026-10-07）

- PipelineRun `zio-flink-operator-lab-jddn6`（Jenkins build `#30`）成功：提交 `a4ffa3b` 的 112 个测试、assembly、控制面镜像和 Stateful Job 镜像均完成；目标 Deployment rollout 到 `build-30-a4ffa3bfb705`，两个副本分别位于 xjw、xxt，两台 NodePort 的 `/healthz` 均返回 `{"status":"ok"}`。
- 首次使用 build `#29` 做真实严格 Savepoint 升级时，operation `b34bf428-6a80-387f-8e96-b95ba49d11dd` 正确进入 `FAILED`，原因是 Operator 的升级结果路径位于 `status.jobStatus.upgradeSavepointPath`，旧观察逻辑只读取 `savepointInfo.lastSavepoint.location`，且不能把 READY 但证据暂缺视为确定失败。该现场失败促成 `a4ffa3b` 修复，并保留为回归依据。
- build `#30` 部署后，真实 `zio-e2e-strict-savepoint` 通过 HTTP 提交严格 Savepoint 升级；operation `966c9524-0494-373c-991a-ed2ccc30f27e` 最终为 `COMPLETED`，事件包含 `SUBMITTED → WAITING_FOR_OBSERVATION → OBSERVED → VERIFICATION_SUCCEEDED`，generation/observedGeneration 均为 `3`，审计证据保存路径 `s3://flink-savepoints/zio-e2e-strict/savepoints/savepoint-176ced-43a90de992e0`。
- 同一 Job 的 Flink 日志记录从该 Savepoint 恢复，并连续完成 checkpoint `302` 至 `318`；RustFS 对该路径的 `_metadata` HEAD 返回 HTTP 200。验证后通过 operation `40fe3b54-1380-37df-bd6f-24e3411e96ef` 删除 Deployment，两个升级 Snapshot CR 也已删除；复查没有 `zio-e2e-*` 或 smoke 的 FlinkDeployment、FlinkStateSnapshot、Pod、Service、ReplicaSet、Deployment 或 Lock 残留。对应的 FlinkOperation CR 和 RustFS 快照对象保留为审计和恢复证据。

### 历史多节点锁、PostgreSQL 与稳定性验收（2026-10-08）

- 真实租约接管：先在 `bigdata-lab` 创建已过期的 `FlinkOperationLock/flink-lock-874c2d79e17088bf`，旧 operation 为 `zio-e2e-lock-old`；通过双副本控制面提交后，operation `9afbee58-823b-3d6e-8f0e-4fdad1a9574d` 创建新锁 UID、更新 `spec.operationId` 并延长 `leaseUntil`，最终进入 `COMPLETED`。随后删除 operation `4a383749-387e-35bd-aadf-45eb38e9dbc3` 完成清理，Lock、Deployment、Snapshot、Pod 和 Service 均无残留。
- PostgreSQL 双节点：临时双副本控制面分别调度到 `xxt`、`xjw`，使用 PostgreSQL 状态与 operation store。两台 NodePort（`30884`）的 `/v1/state?namespace=bigdata-lab` 均返回 `backend=postgres` 和相同 `resourceVersion=205312379`；同一个 requestId 在两台入口得到 operation `6001acb1-e3c0-362d-8a25-d610bcc356be`，两台查询均为 `COMPLETED`，事件链一致为 `ACCEPTED → VALIDATION_STARTED → VALIDATION_PASSED → SUBMITTED → WAITING_FOR_OBSERVATION → VERIFICATION_STARTED → OBSERVED → VERIFICATION_SUCCEEDED`。清理 operation `67658226-1eae-30d4-8a83-d3201b878a8b` 完成后，临时 Deployment、Service、Secret 和测试表已删除。
- 压力基线：16 并发、60 秒、两节点交替访问健康和状态接口，共 9507 次请求，成功率 100%。`/healthz` 7610 次，P50/P95/P99/最大延迟为 `20.4/159.0/255.2/425.2 ms`；`/v1/state` 1897 次，P50/P95/P99/最大延迟为 `58.9/512.2/745.4/930.2 ms`。按成功率 ≥99%、健康 P95 ≤500ms、状态 P95 ≤2000ms 的本轮基线通过。
- 长稳基线：10 分钟、每 5 秒从两台 NodePort 访问 `/healthz` 和 `/v1/state`，共 468 次请求全部成功，错误率 `0%`，P95 `66.4ms`，最大 `451.7ms`；两个控制面 Pod 全程 `restartCount=0`。这是控制面读请求稳定性证据，不是 Flink 作业吞吐、故障注入或多小时生产 SLO 证明。

- PipelineRun `zio-flink-operator-lab-wjclx`（Jenkins build `#27`）成功：提交 `0561654` 的测试、assembly、控制面镜像和 Stateful Job 镜像均完成，目标 Deployment rollout 到 `build-27-0561654c8687`。
- PipelineRun `zio-flink-operator-lab-wh67d`（Jenkins build `#28`）成功：提交 `0e72734` 的 108 个测试、assembly、控制面镜像和 Stateful Job 镜像均完成，目标 Deployment rollout 到 `build-28-0e72734e935c`；两副本分别位于 xxt、xjw。

- PipelineRun `zio-flink-operator-lab-582f4`（Jenkins build `#24`）成功：包含 StatefulCounterJob 的 Maven 打包、带 S3 插件的 Flink Job 镜像构建/推送，以及控制面 Deployment rollout。控制面镜像为 `build-24-ba815f757367`，测试 Job 镜像为 `100.97.53.78:5001/xxt/zio-flink-stateful-job:build-24-ba815f757367`。
- PipelineRun `zio-flink-operator-lab-96csb`（Jenkins build `#23`）成功：提交 `7e0a00b` 的测试、assembly、镜像推送和目标 Deployment rollout 均通过。最终镜像为 `build-23-7e0a00bfa84d`。
- 新镜像 Deployment 为 `2/2`，两个 Pod 分别运行在 `xxt` 和 `xjw`；NodePort `30882` 的 `/healthz` 和 `/readyz` 均返回 `{"status":"ok"}`。
- 使用 `dryrun-7e0a00b` 在目标集群执行 HTTP `POST /v1/deployments?namespace=bigdata-lab&dryRun=true`，API 返回 dry-run 对象预览；随后查询 `FlinkDeployment/dryrun-7e0a00b` 返回 `NotFound`，证明该请求未创建业务 CR，也未进入 operation worker。
- PipelineRun `zio-flink-operator-lab-jlkxm`（Jenkins build `#22`）成功：提交 `4232b75` 的测试、assembly、镜像推送和目标 Deployment rollout 均通过。最终镜像为 `build-22-4232b75b9b9a`。
- Deployment 为 `2/2`，两个 Pod 分别运行在 `xxt` 和 `xjw`；两个 NodePort 地址的 `/healthz` 均返回 `{"status":"ok"}`。
- 请求 `smoke-20261006-5` 的 operation `c3f6530c-9777-43b4-bbb5-8b35a831cc9a` 在两个节点返回完全相同的 `COMPLETED` 事件链：`SUBMITTED → WAITING_FOR_OBSERVATION → OBSERVED → VERIFICATION_SUCCEEDED`。
- 对应 `FlinkDeployment/zio-control-plane-smoke-5` 状态为 `READY/FINISHED/DEPLOYED`。这证明了 HTTP 提交、Operation CR、resourceVersion/watch 观察、Operator reconcile、自然结束任务判定和两副本统一读取状态。

本轮现场证据覆盖了运行中 Job 的 checkpoint、savepoint、RustFS 结果路径和 savepoint 恢复连续性；对已结束 Job 的 savepoint operation `dfa0f661-22ad-4f07-9ccd-77101c6d95f3` 仍保留为负向契约证据，审计事件记录 Operator 的 `ABANDONED` 原因（目标 Job 不在运行）。

此前 build `#11` 的 Source 阶段曾因 GitHub 连接重置而 `external_blocked`；后续 PipelineRun `#22` 已成功，因此当前提交已有完整 CI/CD 证据。历史失败仍保留用于说明重试配置的背景。

## 测试覆盖

| 层级 | 能证明 | 不能证明 |
| --- | --- | --- |
| Scala/domain/HTTP | 参数、资源 JSON、快照类型和 patch | API Server 接受资源 |
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

本地 API Deployment 为两个副本、前端 Deployment 为一个副本，OrbStack 当前只有一个节点。健康检查和 deployment dry-run 可以验证服务进程、Service、RBAC 和 API Server 访问；不能证明跨节点高可用或真实 Flink Job。

## 真实集群验收

在目标集群按以下顺序记录证据：

1. `apply --dry-run` 返回成功。
2. 真实 apply 返回成功，CR 进入 Operator 处理状态。
3. JobManager、TaskManager 和 Job 进入预期状态。
4. `FlinkStateSnapshot` 的 state、path、error、failures 与触发请求一致。
5. 从 savepoint 恢复后，Job 状态和业务状态连续。

如果只拿到 HTTP 200、CR 已创建或旧的 `lastSavepoint`，标记为 `evidence_incomplete`。如果被集群、Operator、权限、存储或 kubeconfig 阻塞，标记为 `external_blocked`，保留阻塞条件。
