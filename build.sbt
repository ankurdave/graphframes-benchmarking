name := "graphframes-benchmarking"

version := "0.1-SNAPSHOT"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.10.4"

licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided"

libraryDependencies += "com.google.guava" % "guava" % "14.0.1"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture"// ,
  // "-Ywarn-unused-import"
)

scalacOptions in (Compile, console) := Seq()

// Run tests with more memory
javaOptions in test += "-Xmx2G"

fork := true
