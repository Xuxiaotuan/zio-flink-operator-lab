ThisBuild / scalaVersion := "3.3.5"
ThisBuild / organization := "cn.xuyinyin"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "zio-flink-operator-lab",
    assembly / mainClass := Some("cn.xuyinyin.flinklab.Main"),
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      // Flink 1.20.1 为 JSON_QUERY RETURNING 补丁覆盖这两个 Calcite 类（CALCITE-6365）。
      // 仅保留精确 Flink 工件的实现，其余类仍检查内容一致性。
      case "org/apache/calcite/sql/fun/SqlJsonQueryFunction.class" |
           "org/apache/calcite/sql/fun/SqlJsonQueryFunction$1.class" =>
        sbtassembly.CustomMergeStrategy("FlinkJsonQueryPatch") { conflicts =>
          conflicts.filter(_.module.exists(module =>
            module.organization == "org.apache.flink" && module.name == "flink-sql-parser" && module.version == "1.20.1"
          )) match {
            case Vector(patched) => Right(Vector(sbtassembly.Assembly.JarEntry(patched.target, patched.stream)))
            case _ => Left("Expected exactly one Flink 1.20.1 JSON_QUERY patch class")
          }
        }
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case PathList("META-INF", "versions", _*) => MergeStrategy.discard
      case "META-INF/okio.kotlin_module" => MergeStrategy.first
      case path => MergeStrategy.deduplicate
    },
    Compile / run / fork := true,
    Compile / run / mainClass := Some("cn.xuyinyin.flinklab.Main"),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Wunused:all"),
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.1.11",
      "dev.zio" %% "zio-streams" % "2.1.11",
      "io.kubernetes" % "client-java" % "20.0.1",
      "org.postgresql" % "postgresql" % "42.7.4",
      "com.zaxxer" % "HikariCP" % "5.1.0",
      "org.apache.flink" % "flink-sql-parser" % "1.20.1",
      // Flink's published parser excludes linq4j, but Calcite's parser config initializes Functions.
      "org.apache.calcite" % "calcite-linq4j" % "1.32.0",
      // flink-sql-parser declares Guava provided; its parser/Calcite AST need it at runtime.
      "com.google.guava" % "guava" % "32.1.3-jre",
      "com.lihaoyi" %% "ujson" % "4.1.0",
      "dev.zio" %% "zio-test" % "2.1.11" % Test,
      "dev.zio" %% "zio-test-sbt" % "2.1.11" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
