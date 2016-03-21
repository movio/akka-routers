organization := "com.kalmanb"

name := "akka-routers"

version := "2.3.14_0.2.0-SNAPSHOT"

scalaVersion := "2.11.8"

publishTo := Some("repo" at "http://")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "junit" % "junit" % "4.11" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test" 
)

// TODO - akka version based on version
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.14",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.14" % "test"
)





