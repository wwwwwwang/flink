
name := "flinkKafka"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.flink" % "flink-scala_2.11" % "1.5.0" % Provided
libraryDependencies += "org.apache.flink" % "flink-streaming-scala_2.11" % "1.5.0" % Provided
libraryDependencies += "org.apache.flink" %% "flink-examples-batch" % "1.5.0" % Provided
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka-0.10" % "1.5.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.5" % Provided
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.35"
libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".class" => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case "unwanted.txt" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}