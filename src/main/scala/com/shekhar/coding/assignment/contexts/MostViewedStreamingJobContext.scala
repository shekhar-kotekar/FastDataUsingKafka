package com.shekhar.coding.assignment.contexts

import java.util.{Properties, UUID}

import com.typesafe.config.Config
import StreamingJobContext._
import com.shekhar.coding.assignment.model.UserJsonSerDe
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig

/**
 * This class contains all the information needed to start the
 * most viewed pages job
 * @param config configuration object
 */
class StreamingJobContext(config: Config) {
  val properties: Properties = new Properties()
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, s"my_application_${UUID.randomUUID().toString}")
  properties.put(
    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
    config.getOrElse(StreamingJobContext.brokerAddressProperty, "localhost:9092")
  )
  properties.put(
    StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
    config.getOrElse(keySerdeClassProperty, Serdes.String().getClass.toString)
  )
  properties.put(
    StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
    config.getOrElse(valueSerdeClassProperty, classOf[UserJsonSerDe].toString)
  )
}

object StreamingJobContext {
  val propertiesRoot: String = "com.shekhar.assignment.properties"
  val brokerAddressProperty: String = s"$propertiesRoot.kafka.brokers"
  val usersTopicProperty: String = s"$propertiesRoot.kafka.usersTopicName"
  val pageViewsTopicProperty: String = s"$propertiesRoot.kafka.pageViewsTopicName"
  val keySerdeClassProperty: String = s"$propertiesRoot.kafka.keySerdeClass"
  val valueSerdeClassProperty: String = s"$propertiesRoot.kafka.valueSerdeClass"

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