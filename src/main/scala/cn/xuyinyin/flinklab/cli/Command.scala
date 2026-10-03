package cn.xuyinyin.flinklab.cli

enum ResourceKind:
  case Deployment, SessionJob, StateSnapshot

object ResourceKind:
  def parse(value: String): Either[String, ResourceKind] =
    value match
      case "deployment" => Right(Deployment)
      case "session-job" => Right(SessionJob)
      case "state-snapshot" => Right(StateSnapshot)
      case other => Left(s"unknown resource kind: $other (use deployment, session-job or state-snapshot)")

  extension (kind: ResourceKind)
    def apiResource: String = kind match
      case ResourceKind.Deployment => "flinkdeployment"
      case ResourceKind.SessionJob => "flinksessionjob"
      case ResourceKind.StateSnapshot => "flinkstatesnapshot"

sealed trait Command
object Command:
  case object Help extends Command
  final case class Render(kind: ResourceKind, options: Map[String, String]) extends Command
  final case class Watch(kind: ResourceKind, options: Map[String, String]) extends Command
  final case class Savepoint(kind: ResourceKind, options: Map[String, String]) extends Command
  final case class SuspendSavepoint(kind: ResourceKind, options: Map[String, String]) extends Command
  final case class Apply(kind: ResourceKind, options: Map[String, String], dryRun: Boolean) extends Command
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
            case "render" => Right(Render(parsedKind, parsed.values))
            case "watch" => Right(Watch(parsedKind, parsed.values))
            case "savepoint" => Right(Savepoint(parsedKind, parsed.values))
            case "suspend-savepoint" => Right(SuspendSavepoint(parsedKind, parsed.values))
            case "apply" => Right(Apply(parsedKind, parsed.values, parsed.dryRun))
            case "status" => Right(Status(parsedKind, parsed.values))
            case "delete" => Right(Delete(parsedKind, parsed.values))
            case other => Left(s"unknown command: $other (use render, apply, watch, savepoint, suspend-savepoint, status or delete)")
        yield command
      case _ => Left("expected: <render|apply|watch|savepoint|suspend-savepoint|status|delete> <deployment|session-job|state-snapshot> [options]")

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
