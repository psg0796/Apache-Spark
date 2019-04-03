name := "lab2"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql"  % "2.4.0",
  "org.apache.kafka" % "kafka-clients" % "2.1.1",
  "org.apache.spark" %% "spark-streaming" % "2.4.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0",
  "org.apache.spark" %% "spark-tags" % "2.4.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0"
)