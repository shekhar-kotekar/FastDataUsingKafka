package com.shekhar.coding.assignment

import java.util.Properties

import com.shekhar.coding.assignment.contexts.MostViewedStreamingJobContext
import com.shekhar.coding.assignment.jobs.{MostViewedPagesJob, StreamingJobs}
import com.shekhar.coding.assignment.model.User
import com.shekhar.coding.assignment.serdes.UserJsonSerDe
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TopologyTestDriver}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

/**
 * This class contains various tests for most viewed pages job
 */
class TestMostViewedPagesJob extends WordSpec with Matchers with BeforeAndAfterAll with LazyLogging {

  private var testDriver: TopologyTestDriver = _
  private val config: Config = ConfigFactory.load()

  "Most viewed pages job" should {
    "return correct number of jobs" in {
      val jobContext: MostViewedStreamingJobContext = new MostViewedStreamingJobContext(config)
      val streamingJob: StreamingJobs = new MostViewedPagesJob(jobContext)

      val properties: Properties = new Properties()
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my_application_id")
      properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
      properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[UserJsonSerDe])

      testDriver = new TopologyTestDriver(streamingJob.getTopology, properties)

      val usersTopicName: String = config.getString(MostViewedStreamingJobContext.usersTopicProperty)
      val userSerde = new UserJsonSerDe
      val usersTopic: TestInputTopic[String, User] =
        testDriver.createInputTopic(usersTopicName, Serdes.String().serializer(), userSerde.serializer())

      val firstUser: User = User(1234L, "user_id_1", "region_id_1", "MALE")
      usersTopic.pipeInput("user_id_1", firstUser)

    }
  }

  override def afterAll(): Unit = {
    if(testDriver != null) {
      testDriver.close()
      logger.info("test driver closed.")
    }
    super.afterAll()
  }

}
