name := "Operations"

version := "1.0"

scalaVersion := "2.11.0"

val spark = "org.apache.spark"
val snapshot = "1.5.1"

libraryDependencies ++= Seq(spark %% "spark-streaming" % snapshot, //%% adds project scala version to artifact name
  spark %% "spark-streaming-kafka" % snapshot,
  spark %% "spark-sql" % snapshot,
  "com.esotericsoftware.yamlbeans" % "yamlbeans" % "1.08",
  "com.datastax.spark" %% "spark-cassandra-connector" % snapshot)

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

mainClass in assembly := Some("app.MainObject")

assemblyMergeStrategy in assembly := {
  case x if x.startsWith("META-INF") => MergeStrategy.discard
  case x if x.endsWith(".html") => MergeStrategy.discard
  case x if x.endsWith(".conf") => MergeStrategy.concat //Needed for Akka
  case x if x.contains("slf4j-api") => MergeStrategy.last
  case x => MergeStrategy.first
}