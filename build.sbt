name := "programming-spark"
organization := "com.mr.dp"
version := "0.1"
//scalaVersion := "2.12.10"
scalaVersion := "2.11.8"

//val sparkVersion = "3.0.0"
val sparkVersion = "2.3.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.databricks" %% "spark-xml" % "0.9.0"
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies
