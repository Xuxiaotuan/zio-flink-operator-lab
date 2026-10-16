package cn.xuyinyin.flinklab.lineage

import cn.xuyinyin.flinklab.metadata.{LineageEdge, LineageNode}
import org.apache.calcite.config.Lex
import org.apache.calcite.sql.*
import org.apache.calcite.sql.parser.{SqlParseException, SqlParser}
import org.apache.flink.sql.parser.ddl.SqlCreateTableAs
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl
import org.apache.flink.sql.parser.validate.FlinkSqlConformance

import scala.jdk.CollectionConverters.*

/** 通过 Flink 1.20.1 语法树提取表级血缘；不绑定 Catalog，也不生成执行计划。 */
object SqlLineageParser:
  private val config = SqlParser.config()
    .withParserFactory(FlinkSqlParserImpl.FACTORY)
    .withLex(Lex.JAVA)
    .withIdentifierMaxLength(256)
    .withConformance(FlinkSqlConformance.DEFAULT)

  // CTE 在当前查询作用域内展开到基础表；pending 阻止把递归或前向引用误记为物理表。
  private case class Scope(ctes: Map[String, List[String]] = Map.empty, pending: Set[String] = Set.empty)
  private type Sources = Either[String, List[String]]
  private val dottedIdentifierError = "unsupported identifier component containing '.': flat dataset names cannot represent it unambiguously"

  def extract(sql: String, observedAt: String): Either[String, List[LineageEdge]] =
    if sql == null || sql.trim.isEmpty then Left("sql must not be empty")
    else
      try
        val statements = SqlParser.create(sql, config).parseStmtList().getList.asScala.toList
        statements match
          case (insert: SqlInsert) :: Nil =>
            insert.getTargetTable match
              case target: SqlIdentifier => edges(target, insert.getSource, observedAt)
              case _ => Left("unsupported INSERT target")
          case (ctas: SqlCreateTableAs) :: Nil => edges(ctas.getTableName, ctas.getAsQuery, observedAt)
          case (node: SqlSelect) :: Nil => Right(Nil)
          case (node: SqlWith) :: Nil => query(node, Scope()).map(_ => Nil)
          case (node: SqlOrderBy) :: Nil => query(node, Scope()).map(_ => Nil)
          case _ :: Nil => Left("unsupported SQL statement for table lineage")
          case _ => Left("exactly one SQL statement is required")
      catch
        case error: SqlParseException => Left(s"invalid Flink SQL: ${error.getMessage}")

  private def edges(target: SqlIdentifier, source: SqlNode, observedAt: String): Either[String, List[LineageEdge]] =
    if hasDottedComponent(target) then Left(dottedIdentifierError)
    else query(source, Scope()).flatMap { sources =>
      if sources.isEmpty then Left("statement must read a source table")
      else Right(sources.distinct.map { name =>
        LineageEdge(LineageNode.Dataset(name), LineageNode.Dataset(identifier(target)), "SELECT", "SQL_STATIC", 0.7, observedAt)
      })
    }

  private def query(node: SqlNode, scope: Scope): Sources = node match
    case select: SqlSelect =>
      for
        from <- relation(select.getFrom, scope)
        nested <- expressions(select.getOperandList.asScala.toList.filterNot(_ eq select.getFrom), scope)
      yield from ++ nested
    case ordered: SqlOrderBy =>
      for
        from <- query(ordered.query, scope)
        nested <- expressions(List(ordered.orderList, ordered.offset, ordered.fetch), scope)
      yield from ++ nested
    case withQuery: SqlWith =>
      val items = withQuery.withList.getList.asScala.toList.collect { case item: SqlWithItem => item }
      val names = items.map(item => identifier(item.name))
      if items.exists(item => hasDottedComponent(item.name)) then Left(dottedIdentifierError)
      else if names.distinct.size != names.size then Left("duplicate CTE names are unsupported")
      else
        // 内层同名 CTE 遮蔽外层定义；逐个解析定义，只让后续定义和主体引用已解析的 CTE。
        val initial = scope.copy(ctes = scope.ctes -- names, pending = scope.pending ++ names)
        items.foldLeft[Either[String, Scope]](Right(initial)) { (result, item) =>
          result.flatMap { current =>
            query(item.query, current).map { sources =>
              val name = identifier(item.name)
              current.copy(ctes = current.ctes.updated(name, sources), pending = current.pending - name)
            }
          }
        }.flatMap(query(withQuery.body, _))
    case _ => Left(s"unsupported query node: ${node.getKind}")

  private def relation(node: SqlNode, scope: Scope): Sources = node match
    case null => Right(Nil)
    case table: SqlIdentifier =>
      val name = identifier(table)
      if hasDottedComponent(table) then Left(dottedIdentifierError)
      else if table.isSimple && scope.pending.contains(name) then Left("recursive or forward CTE references are unsupported")
      else Right(if table.isSimple then scope.ctes.getOrElse(name, List(name)) else List(name))
    case join: SqlJoin =>
      for
        left <- relation(join.getLeft, scope)
        right <- relation(join.getRight, scope)
        condition <- expression(join.getCondition, scope)
      yield left ++ right ++ condition
    case alias: SqlCall if alias.getKind == SqlKind.AS => relation(alias.getOperandList.get(0), scope)
    case _: SqlSelect | _: SqlWith | _: SqlOrderBy => query(node, scope)
    case _ => Left(s"unsupported relation node: ${node.getKind}")

  // 表达式只贡献嵌套查询的来源；这里的标识符是字段，不能误记为来源表。
  private def expression(node: SqlNode, scope: Scope): Sources = node match
    case null => Right(Nil)
    case _: SqlSelect | _: SqlWith | _: SqlOrderBy => query(node, scope)
    case list: SqlNodeList => expressions(list.getList.asScala.toList, scope)
    case call: SqlCall if call.isA(SqlKind.QUERY) => Left(s"unsupported query node: ${call.getKind}")
    case call: SqlCall => expressions(call.getOperandList.asScala.toList, scope)
    case _: SqlIdentifier | _: SqlLiteral | _: SqlDataTypeSpec | _: SqlIntervalQualifier | _: SqlDynamicParam => Right(Nil)
    case _ => Left(s"unsupported expression node: ${node.getKind}")

  private def expressions(nodes: List[SqlNode], scope: Scope): Sources =
    nodes.foldLeft[Sources](Right(Nil)) { (result, node) =>
      for
        accumulated <- result
        nested <- expression(node, scope)
      yield accumulated ++ nested
    }

  private def identifier(node: SqlIdentifier): String = node.names.asScala.mkString(".")

  // Dataset.name 只有扁平字符串，无法区分单分量 `a.b` 与两分量 a.b，因此拒绝前者。
  private def hasDottedComponent(node: SqlIdentifier): Boolean = node.names.asScala.exists(_.contains("."))
