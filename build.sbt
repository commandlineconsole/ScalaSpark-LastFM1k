lazy val root = (project in file(".")).settings(
  name := "TechnicalAssesment",
  version := "0.1.0",
  scalaVersion := "2.11.7"
)

val sparkVersion = "2.1.0"
val configVersion = "1.3.1"
val scalaTestVersion = "3.0.0"

// Build Dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "com.typesafe" % "config" % configVersion
)

// Test Dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % scalaTestVersion
)

assemblyJarName in assembly := "SparkFatJar.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _ *) => MergeStrategy.discard
  case x                              => MergeStrategy.first
}

fork in Test := true
javaOptions ++= Seq("-Xms512M",
                    "-Xmx2048M",
                    "-XX:MaxPermSize=2048M",
                    "-XX:+CMSClassUnloadingEnabled")

parallelExecution in Test := false
