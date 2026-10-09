package cn.xuyinyin.flinklab.domain

/** Flink Kubernetes Operator 支持的自定义资源种类。 */
enum ResourceKind:
  case Deployment, SessionJob, StateSnapshot, Operation, OperationLock

object ResourceKind:
  def parse(value: String): Either[String, ResourceKind] =
    value match
      case "deployment" => Right(Deployment)
      case "session-job" => Right(SessionJob)
      case "state-snapshot" => Right(StateSnapshot)
      case "operation" => Right(Operation)
      case "operation-lock" => Right(OperationLock)
      case other => Left(s"unknown resource kind: $other (use deployment, session-job, state-snapshot or operation)")

  def fromApiResource(value: String): Option[ResourceKind] =
    value match
      case "flinkdeployment" | "flinkdeployments" => Some(ResourceKind.Deployment)
      case "flinksessionjob" | "flinksessionjobs" => Some(ResourceKind.SessionJob)
      case "flinkstatesnapshot" | "flinkstatesnapshots" => Some(ResourceKind.StateSnapshot)
      case "flinkoperation" | "flinkoperations" => Some(ResourceKind.Operation)
      case "flinkoperationlock" | "flinkoperationlocks" => Some(ResourceKind.OperationLock)
      case _ => None

  extension (kind: ResourceKind)
    def apiResource: String = kind match
      case ResourceKind.Deployment => "flinkdeployment"
      case ResourceKind.SessionJob => "flinksessionjob"
      case ResourceKind.StateSnapshot => "flinkstatesnapshot"
      case ResourceKind.Operation => "flinkoperation"
      case ResourceKind.OperationLock => "flinkoperationlock"
