package cn.xuyinyin.flinklab.kubernetes
/** 测试用 KubernetesApi fake，记录调用并返回可控的资源和 watch 事件。 */

import cn.xuyinyin.flinklab.cli.ResourceKind
import cn.xuyinyin.flinklab.domain.FlinkTypes.Namespace
import cn.xuyinyin.flinklab.operator.watch.WatchEvent
import zio.*
import zio.stream.*

final class FakeKubernetesApi private (
    val records: Ref[Vector[String]],
    events: Chunk[WatchEvent]
) extends KubernetesApi:
  override def apply(namespace: Namespace, resource: String, dryRun: Boolean): IO[Throwable, String] =
    ZIO.attempt {
      val json = ujson.read(resource)
      val kind = json("kind").str.toLowerCase
      val name = json("metadata")("name").str
      (kind, name)
    }.flatMap { case (kind, name) =>
      records.update(_ :+ s"apply:${namespace.namespaceValue}:$kind:$name:$resource").as("applied")
    }

  override def list(namespace: Namespace, kind: ResourceKind): IO[Throwable, String] =
    records.update(_ :+ s"list:${namespace.namespaceValue}:${kind.apiResource}").as(
      if kind == ResourceKind.StateSnapshot then
        "{\"items\":[{\"apiVersion\":\"flink.apache.org/v1beta1\",\"kind\":\"FlinkStateSnapshot\",\"metadata\":{\"name\":\"orders-snapshot\",\"resourceVersion\":\"2\"},\"spec\":{\"jobReference\":{\"kind\":\"FlinkDeployment\",\"name\":\"orders\"}},\"status\":{\"state\":\"COMPLETED\",\"path\":\"s3://flink-savepoints/orders-snapshot\"}}],\"metadata\":{\"resourceVersion\":\"2\"}}"
      else
        "{\"items\":[{\"apiVersion\":\"flink.apache.org/v1beta1\",\"kind\":\"FlinkDeployment\",\"metadata\":{\"name\":\"orders\",\"resourceVersion\":\"2\"},\"status\":{\"lifecycleState\":\"STABLE\",\"jobManagerDeploymentStatus\":\"READY\",\"jobStatus\":{\"jobId\":\"job-1\",\"state\":\"RUNNING\",\"checkpointInfo\":{\"lastCheckpoint\":{\"timeStamp\":\"100\",\"triggerType\":\"PERIODIC\"}},\"savepointInfo\":{\"lastSavepoint\":{\"location\":\"s3://flink-savepoints/orders\"}}}}}],\"metadata\":{\"resourceVersion\":\"2\"}}"
    )

  override def get(namespace: Namespace, kind: ResourceKind, name: String): IO[Throwable, String] =
    records.update(_ :+ s"get:${namespace.namespaceValue}:${kind.apiResource}:$name").as(
      if kind == ResourceKind.StateSnapshot then
        s"{\"apiVersion\":\"flink.apache.org/v1beta1\",\"kind\":\"FlinkStateSnapshot\",\"metadata\":{\"name\":\"$name\",\"resourceVersion\":\"2\"},\"spec\":{\"jobReference\":{\"kind\":\"FlinkDeployment\",\"name\":\"orders\"}},\"status\":{\"state\":\"COMPLETED\",\"path\":\"s3://flink-savepoints/orders-snapshot\"}}"
      else
        s"{\"apiVersion\":\"flink.apache.org/v1beta1\",\"kind\":\"FlinkDeployment\",\"metadata\":{\"name\":\"$name\",\"resourceVersion\":\"2\"},\"status\":{\"lifecycleState\":\"STABLE\",\"jobManagerDeploymentStatus\":\"READY\",\"jobStatus\":{\"jobId\":\"job-1\",\"jobName\":\"orders\",\"state\":\"RUNNING\",\"checkpointInfo\":{\"formatType\":\"FULL\",\"lastCheckpoint\":{\"formatType\":\"FULL\",\"timeStamp\":\"100\",\"triggerNonce\":\"7\",\"triggerType\":\"PERIODIC\"},\"triggerId\":\"cp-1\",\"triggerTimestamp\":\"100\",\"triggerType\":\"PERIODIC\"},\"savepointInfo\":{\"formatType\":\"CANONICAL\",\"lastSavepoint\":{\"location\":\"s3://flink-savepoints/orders\",\"timeStamp\":\"90\",\"triggerNonce\":\"6\",\"triggerType\":\"MANUAL\"},\"savepointHistory\":[{\"location\":\"s3://flink-savepoints/orders\"}],\"triggerId\":\"sp-1\",\"triggerTimestamp\":\"90\",\"triggerType\":\"MANUAL\"},\"upgradeSavepointPath\":\"s3://flink-savepoints/orders-upgrade\"},\"reconciliationStatus\":{\"state\":\"DEPLOYED\"},\"conditions\":[{\"type\":\"Running\",\"status\":\"True\",\"message\":\"ready\"}]}}"
    )

  override def delete(namespace: Namespace, kind: ResourceKind, name: String): IO[Throwable, String] =
    records.update(_ :+ s"delete:${namespace.namespaceValue}:${kind.apiResource}:$name").as("deleted")

  override def patch(namespace: Namespace, kind: ResourceKind, name: String, patch: String): IO[Throwable, String] =
    records.update(_ :+ s"patch:${namespace.namespaceValue}:${kind.apiResource}:$name:$patch").as("patched")

  override def watch(namespace: Namespace, kind: ResourceKind, name: String): ZStream[Any, Throwable, WatchEvent] =
    ZStream.fromChunk(events)

  override def watchFrom(namespace: Namespace, kind: ResourceKind, name: Option[String], resourceVersion: Option[String]): ZStream[Any, Throwable, WatchEvent] =
    ZStream.fromChunk(events)

object FakeKubernetesApi:
  def make: UIO[FakeKubernetesApi] =
    for
      records <- Ref.make(Vector.empty[String])
      event <- ZIO.fromEither(WatchEvent.fromJson(ujson.read("""{"type":"MODIFIED","object":{"metadata":{"name":"orders","resourceVersion":"1"},"status":{"jobStatus":{"state":"RUNNING"}}}}"""))).mapError(new IllegalArgumentException(_)).orDie
    yield new FakeKubernetesApi(records, Chunk.single(event))
