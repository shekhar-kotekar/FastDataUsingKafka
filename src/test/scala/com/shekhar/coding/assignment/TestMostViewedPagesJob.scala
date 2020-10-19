package com.shekhar.coding.assignment

import java.util.Properties

import com.shekhar.coding.assignment.contexts.MostViewedStreamingJobContext
import com.shekhar.coding.assignment.jobs.{MostViewedPagesJob, StreamingJobs}
import com.shekhar.coding.assignment.model.{PageViewsByUser, User}
import com.shekhar.coding.assignment.serdes.{GenericSerDe, UserJsonSerDe}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.JavaConverters._
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
      val usersByPageViewSerDe = new GenericSerDe[PageViewsByUser]
      val outputTopic: TestOutputTopic[String, PageViewsByUser] =
        testDriver.createOutputTopic("output_topic", Serdes.String().deserializer(), usersByPageViewSerDe)

      val usersTopicName: String = config.getString(MostViewedStreamingJobContext.usersTopicProperty)
      val userSerde = new UserJsonSerDe
      val usersTopic: TestInputTopic[String, User] =
        testDriver.createInputTopic(usersTopicName, Serdes.String().serializer(), userSerde.serializer())

      val firstUser: User = User(1234L, "user_id_1", "region_id_1", "MALE")
      //TODO: Find a way to generate many records and
      //TODO: Find a way to simulate windowing operation
      usersTopic.pipeInput("user_id_1", firstUser)

      val output = outputTopic.readValuesToList().asScala
      output.foreach(println)
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
