name := """spark-resource-manager"""

version := "1.0.0"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.1.1"  % "provided",
  "org.scalatest"    %% "scalatest"       % "3.0.3"  % "test",
  "org.mockito"      % "mockito-core"     % "2.8.47" % "test"
)

// @formatter:off
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
// @formatter:on

compileOrder := CompileOrder.JavaThenScala
