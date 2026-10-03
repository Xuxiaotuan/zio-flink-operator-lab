# 运行指南

这份文档只描述一条流程：准备 Flink Job，使用 ZIO 提交 Operator CR，观察任务和快照状态。

## 前置条件

- JDK 17、SBT、Maven。
- Kubernetes context 可用。
- 目标集群已安装匹配版本的 Flink Kubernetes Operator 和 CRD。
- Flink Operator 正在观察目标 namespace。
- Flink 镜像和 Job JAR 可由 Flink Pod 读取。

程序通过 kubeconfig 创建 Kubernetes Java Client。凭据只放在 kubeconfig 或 Kubernetes Secret，不写入项目。

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export FLINK_NAMESPACE=flink-lineage-test
kubectl config current-context
kubectl get crd flinkdeployments.flink.apache.org flinksessionjobs.flink.apache.org flinkstatesnapshots.flink.apache.org
```

## 构建

```sh
sbt -batch test
mvn -B -f job/pom.xml package -DskipTests
```

`job/target/zio-flink-wordcount-0.1.0.jar` 是示例 Job。提交前把它放进 Flink 镜像、挂载目录或受支持的远程 URI。

## 提交 FlinkDeployment

先生成资源，不连接集群：

```sh
sbt "run render deployment --name orders --namespace $FLINK_NAMESPACE"
```

使用 API Server dry-run 检查 CR 和权限：

```sh
sbt "run apply deployment --name orders --namespace $FLINK_NAMESPACE --dry-run"
```

确认 dry-run 后提交：

```sh
sbt "run apply deployment \
  --name orders \
  --namespace $FLINK_NAMESPACE \
  --image YOUR_FLINK_IMAGE \
  --jar-uri local:///opt/flink/usrlib/zio-flink-wordcount-0.1.0.jar \
  --entry-class cn.xuyinyin.flinklab.job.WordCountJob"
```

`--target-directory` 只用于首次 apply，为 Flink 写入 `state.savepoints.dir`。目录必须是 Flink Pod 可读写的持久存储。

## 提交 SessionJob

SessionJob 必须指向已经存在的 Session Cluster：

```sh
sbt "run apply session-job \
  --name orders-job \
  --namespace $FLINK_NAMESPACE \
  --deployment existing-session-cluster \
  --jar-uri local:///opt/flink/usrlib/zio-flink-wordcount-0.1.0.jar \
  --entry-class cn.xuyinyin.flinklab.job.WordCountJob"
```

## 观察任务状态

```sh
sbt "run status deployment --name orders --namespace $FLINK_NAMESPACE"
sbt "run watch deployment --name orders --namespace $FLINK_NAMESPACE"
```

状态来源是 Flink CR 的 `status`。提交成功只代表 API Server 接受了期望状态；需要继续观察 Operator 生命周期、Job 状态、条件和错误。

## 请求 checkpoint 或 savepoint

`FlinkStateSnapshot` 是统一的快照入口：

```sh
sbt "run apply state-snapshot \
  --name orders-savepoint \
  --namespace $FLINK_NAMESPACE \
  --target-kind deployment \
  --target-name orders \
  --snapshot-type savepoint"

sbt "run watch state-snapshot --name orders-savepoint --namespace $FLINK_NAMESPACE"
```

checkpoint 只需把 `--snapshot-type` 改成 `checkpoint`。快照请求的最终结果以 `FlinkStateSnapshot.status.state`、`path`、`error` 和 `failures` 为准。

旧的 `savepoint --nonce` 和 `suspend-savepoint` 命令仍保留为兼容入口；新流程使用 StateSnapshot CR。

## 启动 HTTP 控制面

```sh
sbt "run serve"
```

默认监听 `0.0.0.0:8080`。HTTP 接口、状态字段和快照示例见 [状态监控](monitoring.md)。多副本部署见 [部署](deployment.md)。
