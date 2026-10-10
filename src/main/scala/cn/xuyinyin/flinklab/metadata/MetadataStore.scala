package cn.xuyinyin.flinklab.metadata

import cn.xuyinyin.flinklab.domain.ControlPlaneError
import zio.*

trait MetadataStore:
  def initialize: IO[Throwable, Unit] = ZIO.unit
  def listCatalogs: IO[ControlPlaneError, List[CatalogRegistration]]
  def createCatalog(catalog: CatalogRegistration): IO[ControlPlaneError, CatalogRegistration]
  def listTables(catalogId: String, schema: Option[String]): IO[ControlPlaneError, List[TableMetadata]]
  def getTable(catalogId: String, schema: String, name: String): IO[ControlPlaneError, Option[TableMetadata]]
  def listSnapshots(catalogId: String): IO[ControlPlaneError, List[SchemaSnapshot]]
  def saveSnapshot(snapshot: SchemaSnapshot): IO[ControlPlaneError, Unit]
  def saveLineage(edge: LineageEdge): IO[ControlPlaneError, Unit]
  def graph(root: String): IO[ControlPlaneError, List[LineageEdge]]

object MetadataStore:
  /** 每个 schema 只投影最新完整快照；空快照也必须覆盖旧表，迟到快照只保留历史。
    * 同一时间以 version 字典序打破平局，使多个副本的读取结果确定。
    */
  private[metadata] def currentTables(snapshots: List[SchemaSnapshot], schema: Option[String]): List[TableMetadata] =
    snapshots.filter(s => schema.forall(_ == s.schema)).groupBy(_.schema).values
      .flatMap(_.maxBy(s => (java.time.Instant.parse(s.observedAt), s.version)).tables)
      .toList.sortBy(t => (t.schema, t.name))

  def configured(env: Map[String, String]): Either[String, String] =
    env.get("ZIO_FLINK_METADATA_STORE").map(_.trim.toLowerCase) match
      case Some("memory") | Some("in-memory") => Right("memory")
      case Some("postgres") | Some("postgresql") | None => Right("postgres")
      case Some(other) => Left(s"unsupported metadata store: $other")

final class InMemoryMetadataStore private (ref: Ref[MetadataStoreState]) extends MetadataStore:
  override def listCatalogs = ref.get.map(_.catalogs.values.toList.sortBy(_.id))
  override def createCatalog(catalog: CatalogRegistration) =
    ZIO.fromEither(catalog.validate.left.map(ControlPlaneError.StoreFailure.apply)) *> ref.modify { state =>
      state.catalogs.get(catalog.id) match
        case Some(existing) if existing == catalog => (Right(existing), state)
        case Some(_) => (Left(ControlPlaneError.StoreFailure(s"catalog already exists: ${catalog.id}")), state)
        case None => (Right(catalog), state.copy(catalogs = state.catalogs.updated(catalog.id, catalog)))
    }.flatMap(ZIO.fromEither)
  override def listTables(catalogId: String, schema: Option[String]) = listSnapshots(catalogId).map(MetadataStore.currentTables(_, schema))
  override def getTable(catalogId: String, schema: String, name: String) = listTables(catalogId, Some(schema)).map(_.find(_.name == name))
  override def listSnapshots(catalogId: String) = ref.get.map(_.snapshots.filter(_.catalogId == catalogId).sortBy(_.version))
  override def saveSnapshot(snapshot: SchemaSnapshot) =
    for
      state <- ref.get
      _ <- ZIO.fromEither(snapshot.validate.left.map(ControlPlaneError.StoreFailure.apply))
      _ <- ZIO.fromEither(state.catalogs.get(snapshot.catalogId).toRight(ControlPlaneError.StoreFailure(s"catalog not found: ${snapshot.catalogId}")))
      _ <- ref.update { current =>
        current.copy(snapshots = current.snapshots.filterNot(s => s.catalogId == snapshot.catalogId && s.schema == snapshot.schema && s.version == snapshot.version) :+ snapshot)
      }
    yield ()
  override def saveLineage(edge: LineageEdge) = ref.update(state => state.copy(edges = (edge :: state.edges).distinctBy(e => (e.source.name, e.target.name, e.operation, e.sourceType, e.jobId)))).unit
  override def graph(root: String) = ref.get.map(_.edges.filter(edge => edge.source.name == root || edge.target.name == root).reverse)

final case class MetadataStoreState(catalogs: Map[String, CatalogRegistration] = Map.empty, snapshots: List[SchemaSnapshot] = Nil, edges: List[LineageEdge] = Nil)
object InMemoryMetadataStore:
  def make: UIO[InMemoryMetadataStore] = Ref.make(MetadataStoreState()).map(new InMemoryMetadataStore(_))
  val layer: ZLayer[Any, Nothing, MetadataStore] = ZLayer.fromZIO(make.map(identity[MetadataStore]))
