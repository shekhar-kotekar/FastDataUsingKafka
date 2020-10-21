package com.shekhar.coding.assignment

import java.time.{Duration, Instant}
import java.util.Properties

import com.shekhar.coding.assignment.contexts.MostViewedStreamingJobContext
import com.shekhar.coding.assignment.jobs.{MostViewedPagesJob, StreamingJobs}
import com.shekhar.coding.assignment.model.{AggregatedPageViews, AggregatedPageViewsSerDe, PageView, PageViewsByUser, User}
import com.shekhar.coding.assignment.serdes.{GenericSerDe, PageViewByUserSerDe, UserJsonSerDe}
import com.shekhar.coding.assignment.wrappers.DataGenerator
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import _root_.scala.collection.JavaConverters._
/**
 * This class contains various tests for most viewed pages job
 */
class TestMostViewedPagesJob extends WordSpec with Matchers with BeforeAndAfterAll with LazyLogging {

  private var testDriver: TopologyTestDriver = _
  private val config: Config = ConfigFactory.load()

  private val properties: Properties = new Properties()
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my_application_id")
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
  properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[UserJsonSerDe])

  val usersTopicName: String = config.getString(MostViewedStreamingJobContext.usersTopicProperty)
  val topPagesTopicName: String = config.getString(MostViewedStreamingJobContext.outputTopicProperty)
  val userSerde = new GenericSerDe[User]

  "Most viewed pages job" should {
    "return correct number of jobs" in {
      // generate job context and create the streaming job object which will have logic for the streaming
      val jobContext: MostViewedStreamingJobContext = new MostViewedStreamingJobContext(config)
      val streamingJob: StreamingJobs = new MostViewedPagesJob(jobContext)

      val topology = streamingJob.getTopology

      testDriver = new TopologyTestDriver(topology, properties)

      val usersTopic: TestInputTopic[String, User] = testDriver.createInputTopic(
        usersTopicName,
        Serdes.String().serializer(),
        userSerde.serializer()
      )

      // write user events to kafka topic
      val usersDataKeyValues: Seq[KeyValue[String, User]] =
        DataGenerator.generateUserData.map(user => new KeyValue(user.userid, user))

      logger.info("user test data piped")
      usersTopic.pipeKeyValueList(usersDataKeyValues.asJava, Instant.now(), Duration.ofMillis(1000))

      // write page view data to kafka topic
      val pageViewTopicName: String = config.getString(MostViewedStreamingJobContext.pageViewsTopicProperty)
      val pageViewSerDe = new GenericSerDe[PageView]
      val pageViewTopic: TestInputTopic[String, PageView] = testDriver
        .createInputTopic(pageViewTopicName, Serdes.String().serializer(), pageViewSerDe.serializer())

      val pageViewDataKeyValues: Seq[KeyValue[String, PageView]] =
        DataGenerator.generatePageViewsData.map(page => new KeyValue(page.userid, page))

      pageViewTopic.pipeKeyValueList(pageViewDataKeyValues.asJava, Instant.now(), Duration.ofMillis(1000))
      logger.info("page view data piped")

      val outputTopic: TestOutputTopic[String, AggregatedPageViews] =
        testDriver.createOutputTopic(topPagesTopicName, Serdes.String().deserializer(), new AggregatedPageViewsSerDe)

      val output = outputTopic.readKeyValuesToList()
      logger.info(s"Number of key values in output topic: ${output.size()}")
      output.asScala.foreach(i => logger.info(s"key: ${i.key}, value: ${i.value}"))
      //testDriver.close()
      //logger.info("test driver closed")
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
