package cn.xuyinyin.flinklab.domain

object FlinkTypes:

  opaque type Namespace = String
  object Namespace:
    def from(value: String): Either[String, Namespace] =
      validate("namespace", value)
    def unsafe(value: String): Namespace = value
  extension (value: Namespace)
    def namespaceValue: String = value

  opaque type DeploymentName = String
  object DeploymentName:
    def from(value: String): Either[String, DeploymentName] =
      if value.matches("[a-z0-9]([-a-z0-9]*[a-z0-9])?")
      then Right(value)
      else Left(s"deployment name must be a lowercase Kubernetes name: $value")
    def unsafe(value: String): DeploymentName = value
  extension (value: DeploymentName)
    def nameValue: String = value

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
