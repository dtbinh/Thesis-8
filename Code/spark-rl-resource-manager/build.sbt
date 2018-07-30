organization := "com.sap"
name := "spark-rl-resource-manager"
version := "1.0.0"
isSnapshot := true

scalaVersion := "2.11.12"

compileOrder := CompileOrder.JavaThenScala

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.2.2" % "provided",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.mockito" % "mockito-core" % "2.19.0" % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
)

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Xfuture",
  "-Ywarn-unused-import"
)