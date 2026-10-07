package cn.xuyinyin.flinklab.domain

/** 领域边界类型：用 opaque type 区分 namespace、资源名、UID、resourceVersion 和快照路径。 */
object FlinkTypes:

  /** Kubernetes namespace；不允许在领域层误传普通字符串。 */
  opaque type Namespace = String
  object Namespace:
    def from(value: String): Either[String, Namespace] =
      validate("namespace", value)
    def unsafe(value: String): Namespace = value
  extension (value: Namespace)
    def namespaceValue: String = value

  /** Kubernetes 资源名；格式校验集中在构造函数。 */
  opaque type DeploymentName = String
  object DeploymentName:
    def from(value: String): Either[String, DeploymentName] =
      if value.matches("[a-z0-9]([-a-z0-9]*[a-z0-9])?")
      then Right(value)
      else Left(s"deployment name must be a lowercase Kubernetes name: $value")
    def unsafe(value: String): DeploymentName = value
  extension (value: DeploymentName)
    def nameValue: String = value

  /** Flink Job JAR 地址，支持 local:/// 或绝对 URI。 */
  opaque type JobJarUri = String
  object JobJarUri:
    def from(value: String): Either[String, JobJarUri] =
      if value.nonEmpty && (value.startsWith("local:///") || value.matches("[a-zA-Z][a-zA-Z0-9+.-]*://.+"))
      then Right(value)
      else Left(s"jar URI must be local:/// or an absolute URI: $value")
    def unsafe(value: String): JobJarUri = value
  extension (value: JobJarUri)
    def uriValue: String = value

  private def validate(label: String, value: String): Either[String, String] =
    if value.nonEmpty then Right(value) else Left(s"$label must not be empty")
