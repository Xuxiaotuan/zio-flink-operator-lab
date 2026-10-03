package cn.xuyinyin.flinklab.cli

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

sealed trait Command
object Command:
  case object Help extends Command
  final case class Render(kind: ResourceKind, options: Map[String, String]) extends Command
  final case class Watch(kind: ResourceKind, options: Map[String, String]) extends Command
  final case class Savepoint(kind: ResourceKind, options: Map[String, String]) extends Command
  final case class SuspendSavepoint(kind: ResourceKind, options: Map[String, String]) extends Command
  final case class Apply(kind: ResourceKind, options: Map[String, String], dryRun: Boolean) extends Command
  final case class Upgrade(kind: ResourceKind, options: Map[String, String]) extends Command
  final case class Resume(kind: ResourceKind, options: Map[String, String]) extends Command
  final case class Restart(kind: ResourceKind, options: Map[String, String]) extends Command
  final case class Status(kind: ResourceKind, options: Map[String, String]) extends Command
  final case class Delete(kind: ResourceKind, options: Map[String, String]) extends Command

  def parse(args: List[String]): Either[String, Command] =
    args match
      case Nil | "help" :: Nil | "--help" :: Nil => Right(Help)
      case action :: kind :: tail =>
        for
          parsedKind <- ResourceKind.parse(kind)
          parsed <- parseOptions(tail)
          command <- action match
            case "render" => validate(action, parsed).map(_ => Render(parsedKind, parsed.values))
            case "watch" => validate(action, parsed).map(_ => Watch(parsedKind, parsed.values))
            case "savepoint" => validate(action, parsed).map(_ => Savepoint(parsedKind, parsed.values))
            case "suspend-savepoint" => validate(action, parsed).map(_ => SuspendSavepoint(parsedKind, parsed.values))
            case "apply" => validate(action, parsed).map(_ => Apply(parsedKind, parsed.values, parsed.dryRun))
            case "upgrade" => validate(action, parsed).map(_ => Upgrade(parsedKind, parsed.values))
            case "resume" => validate(action, parsed).map(_ => Resume(parsedKind, parsed.values))
            case "restart" => validate(action, parsed).map(_ => Restart(parsedKind, parsed.values))
            case "status" => validate(action, parsed).map(_ => Status(parsedKind, parsed.values))
            case "delete" => validate(action, parsed).map(_ => Delete(parsedKind, parsed.values))
            case other => Left(s"unknown command: $other (use render, apply, watch, savepoint, suspend-savepoint, status or delete)")
        yield command
      case _ => Left("expected: <render|apply|upgrade|resume|restart|watch|savepoint|suspend-savepoint|status|delete> <deployment|session-job|state-snapshot> [options]")

  private final case class ParsedOptions(values: Map[String, String], dryRun: Boolean)

  private def parseOptions(tokens: List[String]): Either[String, ParsedOptions] =
    def loop(rest: List[String], values: Map[String, String], dryRun: Boolean): Either[String, ParsedOptions] =
      rest match
        case Nil => Right(ParsedOptions(values, dryRun))
        case "--dry-run" :: tail => loop(tail, values, dryRun = true)
        case key :: value :: tail if key.startsWith("--") && !value.startsWith("--") =>
          loop(tail, values.updated(key.drop(2), value), dryRun)
        case key :: _ if key.startsWith("--") => Left(s"option $key needs a value")
        case value :: _ => Left(s"unexpected argument: $value")
    loop(tokens, Map.empty, dryRun = false)

  private def validate(action: String, parsed: ParsedOptions): Either[String, Unit] =
    val common = Set("name", "namespace")
    val allowed = action match
      case "render" | "apply" | "upgrade" | "restart" => common ++ Set("image", "flink-version", "jar-uri", "entry-class", "parallelism", "upgrade-mode", "fallback", "target-directory", "service-account", "deployment", "target-kind", "target-name", "snapshot-type")
      case "watch" | "status" | "delete" => common
      case "savepoint" => common ++ Set("nonce")
      case "suspend-savepoint" => common
      case _ => common
    if parsed.dryRun && action != "apply" then Left("--dry-run is only valid for apply")
    else parsed.values.keys.find(key => !allowed.contains(key)) match
      case Some(key) => Left(s"unknown option: --$key")
      case None => Right(())
