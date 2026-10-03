ThisBuild / scalaVersion := "3.3.5"
ThisBuild / organization := "cn.xuyinyin"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "zio-flink-operator-lab",
    assembly / mainClass := Some("cn.xuyinyin.flinklab.Main"),
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
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
      "com.lihaoyi" %% "ujson" % "4.1.0",
      "dev.zio" %% "zio-test" % "2.1.11" % Test,
      "dev.zio" %% "zio-test-sbt" % "2.1.11" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
