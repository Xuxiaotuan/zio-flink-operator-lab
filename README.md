# ZIO Flink Operator Control Plane

这是一个 Scala 3 + ZIO 控制面：通过 Kubernetes API 调用 Flink Kubernetes Operator，提交 `FlinkDeployment`、`FlinkSessionJob` 和 `FlinkStateSnapshot`，并观察任务、checkpoint、savepoint 与 Operator 状态。

程序不调用 `kubectl`，不直接操作 JobManager。Kubernetes CR 是期望状态和状态来源，Flink Kubernetes Operator 负责实际 reconcile。

从 [文档入口](docs/README.md) 开始。

## 最短流程

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export FLINK_NAMESPACE=flink-lineage-test

sbt -batch test
sbt "run render deployment --name orders --namespace $FLINK_NAMESPACE"
sbt "run apply deployment --name orders --namespace $FLINK_NAMESPACE --dry-run"
sbt "run apply deployment --name orders --namespace $FLINK_NAMESPACE"
sbt "run watch deployment --name orders --namespace $FLINK_NAMESPACE"
```

项目自带的示例 Job：

```sh
mvn -B -f job/pom.xml package -DskipTests
```

JAR 必须位于 Flink Pod 可访问的位置，再通过 `--jar-uri` 和 `--entry-class` 提交。

## 快照与监控

使用 `FlinkStateSnapshot` 请求 checkpoint 或 savepoint：

```sh
sbt "run apply state-snapshot --name orders-savepoint --target-kind deployment --target-name orders --snapshot-type savepoint --namespace $FLINK_NAMESPACE"
sbt "run watch state-snapshot --name orders-savepoint --namespace $FLINK_NAMESPACE"
```

HTTP 控制面由 `sbt "run serve"` 启动，默认监听 `0.0.0.0:8080`。它提供部署状态、快照创建、列表、查询和删除接口，可部署多个无状态副本。

状态字段、HTTP 示例、checkpoint/savepoint 边界见 [状态监控](docs/monitoring.md)。

## 版本与边界

项目使用 Scala 3.3.5、ZIO 2.1.11、ZIO Streams 2.1.11、Kubernetes Java Client 20.0.1，建议 JDK 17。

当前服务读取 Operator CR 的状态摘要。逐个 checkpoint 的 task/subtask 明细需要 Flink REST，当前不属于 Kubernetes API 适配器。真实 Operator reconcile、checkpoint、savepoint 和恢复结果必须在目标集群单独验证；本地单测和 dry-run 不代表这些结果已经完成。

构建、Kubernetes/RustFS 配置和本地多副本部署见 [部署](docs/deployment.md)；验证记录见 [测试与证据](docs/testing.md)。
