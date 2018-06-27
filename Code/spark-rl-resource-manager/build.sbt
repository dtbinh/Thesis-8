name := "spark-rl-resource-manager"

version := "0.1"

scalaVersion := "2.11.12"

compileOrder := CompileOrder.JavaThenScala

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.2.1" % "provided",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.mockito" % "mockito-core" % "2.19.0" % "test"
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