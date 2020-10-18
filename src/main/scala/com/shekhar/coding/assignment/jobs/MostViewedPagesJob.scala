package com.shekhar.coding.assignment.jobs

import com.shekhar.coding.assignment.contexts.MostViewedStreamingJobContext
import com.shekhar.coding.assignment.model.{PageView, User}
import com.shekhar.coding.assignment.serdes.{PageViewSerDe, UserJsonSerDe}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream

/**
 * This class has methods to continously analyse top 10 most viewed pages by a view time
 * using Kafka streams
 * @param jobContext contains information needed to start the job
 */
class MostViewedPagesJob(jobContext: MostViewedStreamingJobContext) extends StreamingJobs {

  override def getTopology: Topology = {
    val builder: StreamsBuilder = new StreamsBuilder

    implicit val consumed = Consumed.`with`(Serdes.String(), new UserJsonSerDe)
    val usersStreams:KStream[String, User] = builder.stream(jobContext.userTopicName)

    implicit val pageViewConsumed = Consumed.`with`(Serdes.String(), new PageViewSerDe)
    val pageViewsStream: KStream[String, PageView] = builder.stream(jobContext.pageViewsTopicName)

    usersStreams.inner(pageViewsStream, (lhs, rhs) => )

    builder.build()
  }
}
