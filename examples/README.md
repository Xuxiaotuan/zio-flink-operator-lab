# Examples

这里的 JSON 只用于查看和测试 CR 结构，不包含集群凭据。

生成并提交资源使用 CLI 或服务端 API：

```sh
sbt "run render deployment --name word-count --namespace flink-lineage-test"
sbt "run apply deployment --name word-count --namespace flink-lineage-test --dry-run"
```

实际提交前确认目标集群的 Flink Operator、CRD 和 Flink 镜像版本。
