package com.shekhar.coding.assignment.jobs

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.shekhar.coding.assignment.contexts.MostViewedStreamingJobContext
import com.shekhar.coding.assignment.model.{PageView, PageViewsByUser, User}
import com.shekhar.coding.assignment.serdes.{GenericSerDe, PageViewSerDe, UserJsonSerDe}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.{Grouped, JoinWindows, SessionWindows, TimeWindows}
import org.apache.kafka.streams.scala.Serdes.ByteArray
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Materialized, Produced, StreamJoined}

/**
 * This class has methods to continously analyse top 10 most viewed pages by a view time
 * using Kafka streams
 * @param jobContext contains information needed to start the job
 */
class MostViewedPagesJob(jobContext: MostViewedStreamingJobContext) extends StreamingJobs {

  override def getTopology: Topology = {
    val builder: StreamsBuilder = new StreamsBuilder
    val userSerDe = new UserJsonSerDe
    implicit val consumed = Consumed.`with`(Serdes.String(), userSerDe)
    val usersStreams:KStream[String, User] = builder.stream(jobContext.userTopicName)

    val pageViewSerde = new PageViewSerDe
    implicit val pageViewConsumed = Consumed.`with`(Serdes.String(), pageViewSerde)
    val pageViewsStream: KStream[String, PageView] = builder.stream(jobContext.pageViewsTopicName)

    val usersByPageViewSerde = new GenericSerDe[PageViewsByUser]
    implicit val produced: Produced[String, PageViewsByUser] = Produced.`with`(Serdes.String(), usersByPageViewSerde)
    implicit val streamJoined: StreamJoined[String, User, PageView] = StreamJoined.`with`(Serdes.String(), userSerDe, pageViewSerde)
    //TODO: Take this value from job context
    val windowSize = TimeUnit.MILLISECONDS.toMillis(500)
    val advanceWindowBy = TimeUnit.MILLISECONDS.toMillis(10)
    implicit val grouped: Grouped[String, PageViewsByUser] = Grouped.`with`(Serdes.String(), usersByPageViewSerde)
    usersStreams.join(pageViewsStream)(
      (v1: User, v2: PageView) => PageViewsByUser(v1.gender, v1.regionid, v2.pageid, v2.viewtime),
      JoinWindows.of(Duration.ofDays(1))
    ).to("output_topic")

    builder.build()
  }
}
