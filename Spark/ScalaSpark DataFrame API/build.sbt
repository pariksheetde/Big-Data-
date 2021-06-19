name := "IPL_Data_Analysis_1"

version := "1.0"

organization := "ScalaSpark"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "3.0.0" % "provided",
"org.apache.spark" %% "spark-sql" % "3.0.0" % "provided"
)
