name := "Operations"

version := "1.0"

scalaVersion := "2.11.0"

val spark = "org.apache.spark"
val snapshot = "1.5.1"

libraryDependencies ++= Seq(spark %% "spark-streaming" % snapshot, //%% adds project scala version to artifact name
  spark %% "spark-sql" % snapshot,
  spark %% "spark-streaming-kafka" % snapshot)

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

mainClass in assembly := Some("usps.iv.ops.MainObject")

assemblyMergeStrategy in assembly := {
  case x if x.startsWith("META-INF") => MergeStrategy.discard
  case x if x.endsWith(".html") => MergeStrategy.discard
  case x if x.endsWith(".conf") => MergeStrategy.concat //Needed for Akka
  case x if x.contains("slf4j-api") => MergeStrategy.last
  case x => MergeStrategy.first
}