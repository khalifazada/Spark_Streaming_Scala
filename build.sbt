name := "SparkStreamingScala"

version := "1.0"

scalaVersion := "2.11.0"

val sparkVer = "2.4.4"

libraryDependencies ++=
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-streaming" % sparkVer,
    "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVer,
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
    "org.apache.spark" %% "spark-sql" % "2.4.4"
  )