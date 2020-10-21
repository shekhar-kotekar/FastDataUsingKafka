name := "FastDataUsingKafka"

version := "0.1"

scalaVersion := "2.12.12"

val kafkaVersion = "2.6.0"
val logback = "1.2.3"
val json4sVersion = "3.7.0-M6"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "ch.qos.logback" % "logback-core" % logback,
  "ch.qos.logback" % "logback-classic" % logback,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.json4s" %% "json4s-native" % json4sVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,
  "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % "test",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.typesafe" % "config" % "1.4.0"
)

mainClass in (Compile, packageBin) := Some("com.shekhar.coding.assignment.StreamsMain")
