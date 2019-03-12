

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.mytaxi.data.test",
      version := "0.1.0",
      scalaVersion := "2.12.2",
      assemblyJarName in assembly := "myservice.jar"
)),
    name := "myservice",
    libraryDependencies ++= List(
      "org.scalatest" %% "scalatest" % "3.0.5",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "org.apache.kafka" % "kafka-clients" % "2.1.0",
      "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "2.4.0",
      "org.apache.spark" %% "spark-sql" % "2.4.0",
      "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0"
    )
)
assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
