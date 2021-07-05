name := "spark-elastic3"

version := "0.1"

scalaVersion := "2.12.13"

val sparkVersion = "3.1.1"
val elasticSparkVersion = "7.13.1"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % sparkVersion % "provided",
  "org.apache.spark" % "spark-sql_2.12" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.elasticsearch" % "elasticsearch-spark-30_2.12" % elasticSparkVersion,
  "org.elasticsearch" % "elasticsearch-hadoop" % elasticSparkVersion
)

excludeDependencies ++= Seq(
  "org.apache.spark" % "spark-yarn_2.11",
  "org.apache.spark" % "spark-streaming_2.11",
  "org.apache.spark" % "spark-sql_2.11",
  "org.apache.spark" % "spark-core_2.11"
)