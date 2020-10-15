name := "FastDataUsingKafka"

version := "0.1"

scalaVersion := "2.13.0"

val kafkaVersion = "2.6.0"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams-test-utils" % "2.6.0" % Test
)