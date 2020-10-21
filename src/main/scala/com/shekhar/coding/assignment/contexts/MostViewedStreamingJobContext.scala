package com.shekhar.coding.assignment.contexts

import java.util.{Properties, UUID}

import com.shekhar.coding.assignment.contexts.JobContexts.propertiesRoot
import com.shekhar.coding.assignment.contexts.MostViewedStreamingJobContext._
import com.shekhar.coding.assignment.serdes.UserJsonSerDe
import com.typesafe.config.Config
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.Serdes

/**
 * This class uses the provided configuration to generate context needed to run the job
 *
 * @param config configuration object
 */
class MostViewedStreamingJobContext(config: Config) extends JobContexts {

  val userTopicName: String = config.getString(usersTopicProperty)
  val pageViewsTopicName: String = config.getString(pageViewsTopicProperty)
  val outputTopicName: String = config.getString(outputTopicProperty)

  val windowSizeInMilliSeconds: Long = config.getOrElse(windowSizeProperty, "5000").toLong
  val advanceWindowByInMilliSeconds: Long = config.getOrElse(advanceWindowByProperty, "100").toLong

  val properties: Properties = new Properties()

  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, s"streaming_application_${UUID.randomUUID().toString}")
  properties.put(
    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
    config.getOrElse(MostViewedStreamingJobContext.brokerAddressProperty, "localhost:9092")
  )
  properties.put(
    StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
    config.getOrElse(keySerdeClassProperty, Serdes.String.getClass.toString)
  )
  properties.put(
    StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
    config.getOrElse(valueSerdeClassProperty, classOf[UserJsonSerDe].toString)
  )

}

/**
 * Companion object used by context class
 */
object MostViewedStreamingJobContext {
  val brokerAddressProperty: String = s"$propertiesRoot.kafka.brokers"
  val outputTopicProperty: String = s"$propertiesRoot.kafka.outputTopicName"
  val usersTopicProperty: String = s"$propertiesRoot.kafka.usersTopicName"
  val pageViewsTopicProperty: String = s"$propertiesRoot.kafka.pageViewsTopicName"
  val keySerdeClassProperty: String = s"$propertiesRoot.kafka.keySerdeClass"
  val valueSerdeClassProperty: String = s"$propertiesRoot.kafka.valueSerdeClass"
  val windowSizeProperty: String = s"$propertiesRoot.kafka.windowSize"
  val advanceWindowByProperty: String = s"$propertiesRoot.kafka.advanceWindowBy"

  implicit class ConfigImplicits(config: Config) {
    def getOrElse(propertyName: String, elseValue: String): String = {
      if(config.hasPath(propertyName)) {
        config.getString(propertyName)
      } else {
        elseValue
      }
    }
  }
}
