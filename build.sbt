
packAutoSettings

name := "nfst"

organization in ThisBuild := "prototypes"

scalacOptions in ThisBuild := Seq("-deprecation", "-feature", "-unchecked", "-optimise", "-language:reflectiveCalls")

crossScalaVersions in ThisBuild := Seq("2.10.6", "2.11.7")

libraryDependencies in ThisBuild ++= Seq(
  "org.apache.lucene" % "lucene-core" % "5.4.1"
, "org.rogach" %% "scallop" % "0.9.5" // exclude("org.scala-lang", "scala-library") exclude("org.scala-lang", "scala-reflect")
, "org.scalatest" %% "scalatest" % "2.2.6" % "test"
, "org.scalacheck" %% "scalacheck" % "1.11.6" % "test"
)

