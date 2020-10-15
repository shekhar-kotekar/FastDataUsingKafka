name := "FastDataUsingKafka"

version := "0.1"

scalaVersion := "2.12.12"

val kafkaVersion = "2.6.0"
val logback = "1.2.3"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams-test-utils" % "2.6.0" % Test,
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "ch.qos.logback" % "logback-core" % logback,
  "ch.qos.logback" % "logback-classic" % logback,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "com.typesafe" % "config" % "1.4.0"
)