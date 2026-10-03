# 测试与证据

测试区分四个层次：程序生成资源、API Server 接受资源、Operator reconcile、Flink 任务和快照真实完成。低层测试不能替代高层运行证据。

## 本地验证

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
sbt -batch test
sbt -batch assembly
mvn -B -f job/pom.xml package -DskipTests
```

当前本地执行结果：

- SBT：45 tests passed，0 failed，0 ignored。
- 覆盖 CLI/domain、FlinkDeployment/FlinkStateSnapshot JSON、fake Kubernetes API、HTTP wire contract、HTTP 控制面状态/快照接口、统一状态后端配置与接口、watch 状态投影、checkpoint/savepoint 字段和重试策略。
- Maven Job：应以本轮命令的 `BUILD SUCCESS` 为准。

当前测试按职责分组：CLI 4、domain 3、fake Kubernetes API 3、Kubernetes HTTP contract 5、savepoint patch 3、watch model 5、watch stream/retry 11、HTTP 控制面 7、状态后端配置 4。

这些测试默认不连接真实 Kubernetes。HTTP contract test 使用本地 HTTP server，只验证 Kubernetes Java Client 的请求协议。

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
