name := "ScalaSpark"

version := "1.0"

scalaVersion := "2.12.8"

///val sparkVersion = "2.3.0"/
val sparkVersion = "3.0.1"

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "mysql" % "mysql-connector-java" % "5.1.29",
  "org.postgresql" % "postgresql" % "9.4-1201-jdbc41",
  "org.apache.spark" %% "spark-core" % "1.0.1",
  "mysql" % "mysql-connector-java" % "5.1.6",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" %  sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1" % "provided",
  "org.apache.kafka" % "kafka-clients" % "0.11.0.1"
)
