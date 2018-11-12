
name := "flinkKafkaHdfs"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.flink" % "flink-scala_2.11" % "1.6.0" % Provided
libraryDependencies += "org.apache.flink" % "flink-streaming-scala_2.11" % "1.6.0" % Provided
libraryDependencies += "org.apache.flink" %% "flink-examples-batch" % "1.6.0" % Provided
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka-0.10" % "1.6.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.5" % Provided
libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"
libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.8.2"
libraryDependencies += "org.apache.parquet" % "parquet-hadoop" % "1.8.2"
libraryDependencies += "org.apache.flink" %% "flink-connector-filesystem" % "1.6.0"
//libraryDependencies += "org.apache.flink" % "flink-formats" % "1.6.0" pomOnly()
libraryDependencies += "org.apache.flink" % "flink-parquet" % "1.6.0"

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