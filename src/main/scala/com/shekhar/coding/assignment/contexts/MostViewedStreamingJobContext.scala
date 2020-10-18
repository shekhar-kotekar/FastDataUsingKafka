package com.shekhar.coding.assignment.contexts

import java.util.Properties

import com.shekhar.coding.assignment.contexts.JobContexts.propertiesRoot
import com.shekhar.coding.assignment.contexts.MostViewedStreamingJobContext._
import com.typesafe.config.Config

/**
 * This class contains all the information needed to start the
 * most viewed pages job
 * @param config configuration object
 */
class MostViewedStreamingJobContext(config: Config) extends JobContexts {
  val userTopicName: String = config.getString(usersTopicProperty)
  val pageViewsTopicName: String = config.getString(pageViewsTopicName)
  val properties: Properties = new Properties()

  /*properties.put(StreamsConfig.APPLICATION_ID_CONFIG, s"streaming_application_${UUID.randomUUID().toString}")
  properties.put(
    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
    config.getOrElse(MostViewedStreamingJobContext.brokerAddressProperty, "localhost:9092")
  )
  properties.put(
    StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
    config.getOrElse(keySerdeClassProperty, Serdes.String().getClass.getName)
  )
  properties.put(
    StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
    config.getOrElse(valueSerdeClassProperty, classOf[UserJsonSerDe].toString)
  )*/

}

object MostViewedStreamingJobContext {
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
