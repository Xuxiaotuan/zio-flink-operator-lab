package cn.xuyinyin.flinklab.lineage

import cn.xuyinyin.flinklab.metadata.{LineageEdge, LineageNode}

/** 只解析可证明的表级 INSERT/CTAS；复杂 SQL 直接拒绝，避免伪造血缘。 */
object SqlLineageParser:
  private val Target = "(?is)^\\s*(?:INSERT\\s+(?:INTO|OVERWRITE)|CREATE\\s+TABLE)\\s+([`\\\"A-Za-z0-9_.-]+)\\s+(?:SELECT|AS\\s+SELECT)\\b.*".r
  private val From = "(?i)\\bFROM\\s+([`\\\"A-Za-z0-9_.-]+)(?:\\s+(?:AS\\s+)?[A-Za-z_][A-Za-z0-9_]*)?".r
  private val Join = "(?i)\\bJOIN\\s+([`\\\"A-Za-z0-9_.-]+)(?:\\s+(?:AS\\s+)?[A-Za-z_][A-Za-z0-9_]*)?".r

  def extract(sql: String, observedAt: String): Either[String, List[LineageEdge]] =
    if sql == null || sql.trim.isEmpty then Left("sql must not be empty")
    else
      sanitize(sql).flatMap { clean =>
        if clean.contains(";") || clean.matches("(?is).*\\bUNION\\b.*") || clean.matches("(?is).*\\bWITH\\b.*") || clean.matches("(?is).*\\bEXISTS\\s*\\(.*") || clean.matches("(?is).*\\bFROM\\s*\\(.*") || clean.matches("(?is).*\\bTABLE\\s*\\(.*") then Left("unsupported SQL syntax for conservative table lineage")
        else clean match
          case Target(targetRaw) =>
            val target = normalize(targetRaw)
            val froms = From.findAllMatchIn(clean).map(m => normalize(m.group(1))).toList
            val joins = Join.findAllMatchIn(clean).map(m => normalize(m.group(1))).toList
            if target.isEmpty then Left("target table is empty")
            else if froms.isEmpty then if clean.matches("(?is).*\\bFROM\\b.*") then Left("source table could not be parsed") else Left("statement must contain FROM")
            else if clean.matches("(?is).*\\bFROM\\s+[^\\s]+\\s*,.*") then Left("comma joins are not supported")
            else if clean.matches("(?is).*\\bJOIN\\s*$") then Left("incomplete JOIN clause")
            else
              val sources = (froms ++ joins).distinct
              Right(sources.map(source => LineageEdge(LineageNode.Dataset(source), LineageNode.Dataset(target), "SELECT", "SQL_STATIC", 0.7, observedAt)))
          case _ if clean.matches("(?is).*\\b(?:INSERT|CREATE\\s+TABLE)\\b.*") => Left("unsupported SQL syntax for conservative table lineage")
          case _ if clean.matches("(?is).*\\bCREATE\\s+VIEW\\b.*") => Left("unsupported SQL syntax for conservative table lineage")
          case _ => Right(Nil)
      }

  private def normalize(value: String): String = value.split("\\.").toList.map(_.trim.stripPrefix("`").stripSuffix("`").stripPrefix("\"").stripSuffix("\"")).filter(_.nonEmpty).mkString(".")

  private def sanitize(input: String): Either[String, String] =
    val out = new StringBuilder
    var i = 0
    var quote: Option[Char] = None
    while i < input.length do
      (quote, input.charAt(i)) match
        case (Some(q), c) if c == q => quote = None; out.append(' '); i += 1
        case (Some(_), _) => out.append(' '); i += 1
        case (None, '-') if i + 1 < input.length && input.charAt(i + 1) == '-' =>
          out.append(' '); out.append(' '); i += 2
          while i < input.length && input.charAt(i) != '\n' do
            out.append(' ')
            i += 1
        case (None, '/') if i + 1 < input.length && input.charAt(i + 1) == '*' =>
          out.append(' '); out.append(' '); i += 2
          var closed = false
          while i + 1 < input.length && !closed do
            if input.charAt(i) == '*' && input.charAt(i + 1) == '/' then
              closed = true
              out.append(' ')
              out.append(' ')
              i += 2
            else
              out.append(' ')
              i += 1
          if !closed then return Left("unterminated SQL comment")
        case (None, '\'') => quote = Some('\''); out.append(' '); i += 1
        case (None, c) => out.append(c); i += 1
    if quote.nonEmpty then Left("unterminated SQL string") else Right(out.result())
