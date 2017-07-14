name := "learningFS2"

version := "1.0"

scalaVersion := "2.11.8"

lazy val http4sVersion = "0.17.0-M3"
lazy val circeVersion = "0.7.0"
lazy val catsEffectsVersion = "0.3"

libraryDependencies ++= Seq(
  "org.http4s" %% "http4s-core" % http4sVersion,
  "org.http4s" %% "http4s-blaze-client" % http4sVersion,
  "org.http4s" %% "http4s-circe" % http4sVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-literal" % circeVersion,
  "io.circe" %% "circe-optics" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.typelevel" %% "cats-effect" % catsEffectsVersion
)

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",     // yes, this is 2 args
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",       // N.B. doesn't work well with the ??? hole
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture",
  "-Ywarn-unused-import",   // 2.11 only
  "-Yno-imports"            // no automatic imports at all; all symbols must be imported explicitly
)