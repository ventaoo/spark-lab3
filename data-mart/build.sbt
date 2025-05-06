name := "datamart"

version := "0.1"

scalaVersion := "2.12.18"

ThisBuild / organization := "com.example"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql"  % "3.3.2",
  "com.typesafe"      % "config"     % "1.4.2",             // 配置支持
  "org.scalatest"    %% "scalatest"  % "3.2.16" % Test       // 单元测试
)

testFrameworks += new TestFramework("org.scalatest.tools.Framework")

assembly / mainClass := Some("com.example.datamart.DataMartApp")

enablePlugins(AssemblyPlugin)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}