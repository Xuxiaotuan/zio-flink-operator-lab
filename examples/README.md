# Examples

这里的 JSON 只用于查看和测试 CR 结构，不包含集群凭据。

通过 HTTP 控制面提交资源：

```sh
sbt run
curl -fsS -X POST 'http://127.0.0.1:8080/v1/deployments?namespace=flink-lineage-test&requestId=word-count-apply-1' \
  -H 'Content-Type: application/json' \
  --data @flinkdeployment.json
```

实际提交前确认目标集群的 Flink Operator、CRD 和 Flink 镜像版本。
