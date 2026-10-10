# 运行指南

这份文档只描述一条流程：准备 Flink Job，启动 HTTP 控制面，通过 HTTP 提交 Operator CR，并观察任务和快照状态。

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

## 启动 HTTP 控制面

```sh
sbt run
curl -fsS http://127.0.0.1:8080/healthz
```

默认监听 `0.0.0.0:8080`。服务启动后，所有提交、查询、观察和快照请求都通过 HTTP API 完成。

在 Kubernetes 部署中，打开前端 Service 的地址可以使用浏览器工作台；ZIO API 自身只提供 HTTP API：

```sh
kubectl -n flink-lineage-test port-forward svc/zio-flink-operator-ui 18082:8080
open http://127.0.0.1:18082/
```

- **总览**：读取当前 namespace 的 FlinkDeployment 和 FlinkStateSnapshot；
- **作业**：查看 Job、checkpoint、savepoint 和 Operator 条件；
- **发布**：执行 Kubernetes dry-run 或提交异步 `FlinkOperation`；
- **操作记录**：按 `operationId` 轮询审计事件，区分 `ACCEPTED`、`COMPLETED`、`FAILED` 和 `UNCERTAIN`。

工作台的作业页参考 Flink 运维控制台组织：左侧作业列表，右侧按“概览 / Checkpoint / Savepoint / 配置 / 操作”查看单个作业；发布页使用编辑器和提交参数分栏布局。状态标签和操作结果都来自 HTTP API，不在前端臆造运行状态。

浏览器只是 HTTP 客户端，不直接访问 Kubernetes。当前版本没有认证授权，服务只能部署在本机或受限内网。提交响应的 `202` 只代表请求已受理，不代表 Flink 作业已经完成。

## 提交 FlinkDeployment

使用示例 JSON 提交 Application Cluster：

```sh
curl -fsS -X POST "http://127.0.0.1:8080/v1/deployments?namespace=$FLINK_NAMESPACE&requestId=orders-apply-1" \
  -H 'Content-Type: application/json' \
  --data @examples/flinkdeployment.json
```

响应中的 `operationId` 用于查询异步生命周期：

```sh
curl -fsS "http://127.0.0.1:8080/v1/operations/<operationId>"
curl -fsS "http://127.0.0.1:8080/v1/state/deployment/orders?namespace=$FLINK_NAMESPACE"
```

`FlinkDeployment.spec.job.jarURI` 必须是 Flink Pod 可访问的地址；`entryClass`、`parallelism` 和状态保护策略由 JSON 请求声明。

## 提交 SessionJob

SessionJob 请求使用 `POST /v1/session-jobs`，JSON 中声明已有 Session Cluster 和 Job 配置：

```sh
curl -fsS -X POST "http://127.0.0.1:8080/v1/session-jobs?namespace=$FLINK_NAMESPACE&requestId=orders-session-1" \
  -H 'Content-Type: application/json' \
  --data @examples/flinksessionjob.json
```

## 观察任务状态

```sh
curl -fsS "http://127.0.0.1:8080/v1/deployments/orders/status?namespace=$FLINK_NAMESPACE"
curl -fsS "http://127.0.0.1:8080/v1/state?namespace=$FLINK_NAMESPACE"
```

提交成功只代表 API Server 接受了期望状态；需要继续观察 Operator 生命周期、Job 状态、条件和错误。

## 请求 checkpoint 或 savepoint

`FlinkStateSnapshot` 是统一的快照入口：

```sh
curl -fsS -X POST "http://127.0.0.1:8080/v1/snapshots?namespace=$FLINK_NAMESPACE&requestId=orders-savepoint-1" \
  -H 'Content-Type: application/json' \
  --data '{"targetKind":"deployment","targetName":"orders","snapshotName":"orders-savepoint","type":"savepoint"}'

curl -fsS "http://127.0.0.1:8080/v1/operations/<operationId>"
curl -fsS "http://127.0.0.1:8080/v1/snapshots/orders-savepoint?namespace=$FLINK_NAMESPACE"
```

checkpoint 只需把 `type` 改成 `checkpoint`。最终结果以 `FlinkStateSnapshot.status.state`、`path`、`error` 和 `failures` 为准；RustFS 对象路径还需要在对象存储侧核对。

更多接口、状态字段和多副本部署见 [状态监控](monitoring.md) 与 [部署](deployment.md)。
