 import scala.util.Properties

name := """bdg-dssconf-spark"""

version := "0.1-SNAPSHOT"

organization := "org.biodatageeks"

scalaVersion := "2.11.8"

val DEFAULT_SPARK_2_VERSION = "2.2.1"
val DEFAULT_HADOOP_VERSION = "2.6.5"


lazy val sparkVersion = Properties.envOrElse("SPARK_VERSION", DEFAULT_SPARK_2_VERSION)
lazy val hadoopVersion = Properties.envOrElse("SPARK_HADOOP_VERSION", DEFAULT_HADOOP_VERSION)


libraryDependencies +=  "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided"

libraryDependencies +=  "org.apache.spark" % "spark-sql_2.11" % sparkVersion
libraryDependencies +=  "org.apache.spark" %% "spark-hive" % sparkVersion

libraryDependencies += "com.holdenkarau" % "spark-testing-base_2.11" % "2.2.0_0.7.4" % "test" excludeAll ExclusionRule(organization = "javax.servlet") excludeAll (ExclusionRule("org.apache.hadoop"))

libraryDependencies += "org.apache.spark" %% "spark-hive"       % "2.0.0" % "test"


libraryDependencies += "org.bdgenomics.utils" %% "utils-misc-spark2" % "0.2.10"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.8"
libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"


libraryDependencies += "org.hammerlab.bdg-utils" %% "cli" % "0.3.0"

libraryDependencies += "ch.cern.sparkmeasure" %% "spark-measure" % "0.11"




fork := true
fork in Test := true
parallelExecution in Test := true
javaOptions in test += "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=9999"
//javaOptions in run ++= Seq(
//  "-Dlog4j.debug=true",
//  "-Dlog4j.configuration=log4j.properties")

javaOptions ++= Seq("-Xms512M", "-Xmx8192M", "-XX:+CMSClassUnloadingEnabled")

updateOptions := updateOptions.value.withLatestSnapshots(false)

outputStrategy := Some(StdoutOutput)


resolvers ++= Seq(
  "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven",
  "spring" at "http://repo.spring.io/libs-milestone/"
)


assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case PathList("org", xs@_*) => MergeStrategy.first
  case PathList("javax", xs@_*) => MergeStrategy.first
  case PathList("com", xs@_*) => MergeStrategy.first
  case PathList("shadeio", xs@_*) => MergeStrategy.first

  case PathList("au", xs@_*) => MergeStrategy.first
  case ("META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat") => MergeStrategy.first
  case ("images/ant_logo_large.gif") => MergeStrategy.first

  case "overview.html" => MergeStrategy.rename
  case "mapred-default.xml" => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "parquet.thrift" => MergeStrategy.last
  case "plugin.xml" => MergeStrategy.last

  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


