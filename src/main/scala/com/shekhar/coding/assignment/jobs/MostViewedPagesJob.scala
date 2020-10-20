package com.shekhar.coding.assignment.jobs

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.shekhar.coding.assignment.contexts.MostViewedStreamingJobContext
import com.shekhar.coding.assignment.model._
import com.shekhar.coding.assignment.serdes.{PageViewByUserSerDe, PageViewSerDe, UserJsonSerDe}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.{Grouped, JoinWindows, TimeWindows, Windowed, WindowedSerdes}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.{ByteArrayWindowStore, StreamsBuilder}

/**
 * This class has methods to continously analyse top 10 most viewed pages by a view time
 * using Kafka streams
 * @param jobContext contains information needed to start the job
 */
class MostViewedPagesJob(jobContext: MostViewedStreamingJobContext) extends StreamingJobs {

  private val builder: StreamsBuilder = new StreamsBuilder
  private val userSerDe = new UserJsonSerDe
  private val pageViewSerde = new PageViewSerDe
  private val usersByPageViewSerde = new PageViewByUserSerDe

  override def getTopology: Topology = {

    implicit val consumed = Consumed.`with`(Serdes.String(), userSerDe)
    val usersStreams:KStream[String, User] = builder.stream(jobContext.userTopicName)


    implicit val pageViewConsumed = Consumed.`with`(Serdes.String(), pageViewSerde)
    val pageViewsStream: KStream[String, PageView] = builder.stream(jobContext.pageViewsTopicName)

    implicit val grouped: Grouped[String, PageViewsByUser] = Grouped.`with`(Serdes.String(), usersByPageViewSerde)
    implicit val valueSerde = new AggregatedPageViewsSerDe
    implicit val materialized = Materialized.`with`[String, AggregatedPageViews , ByteArrayWindowStore]
    val timeWindowSerde = WindowedSerdes.timeWindowedSerdeFrom(classOf[String])
    implicit val produced: Produced[Windowed[String], AggregatedPageViews] = Produced.`with`(timeWindowSerde, valueSerde)

    implicit val streamJoined: StreamJoined[String, User, PageView] = StreamJoined.`with`(Serdes.String(), userSerDe, pageViewSerde)

    // get window size and increment information from the context object
    val windowSize = TimeUnit.MILLISECONDS.toMillis(jobContext.windowSizeInMilliSeconds)
    val advanceWindowBy = TimeUnit.MILLISECONDS.toMillis(jobContext.advanceWindowByInMilliSeconds)

    /*
      Join users stream with page views stream,
      map the joined data to get a key of type pageid_gender to be able to group on this key
      Group the stream on newly created key and
      generate sum of viewtimes and count of user IDs
      Write the sum and counts to final topic
     */
    usersStreams.join(pageViewsStream)(
      (user: User, pageView: PageView) => PageViewsByUser(user.gender, pageView.pageid, pageView.viewtime),
      JoinWindows.of(Duration.ofDays(1)))
      .map((_, value) => (s"${value.pageid}_${value.gender}", value))
      .groupByKey(Grouped.`with`(Serdes.String(), usersByPageViewSerde))
      .windowedBy(TimeWindows.of(Duration.ofMillis(windowSize)).advanceBy(Duration.ofMillis(advanceWindowBy)))
      .aggregate(AggregatedPageViews.empty)((_, data, aggregate) => {
        AggregatedPageViews(data.gender, data.pageid, aggregate.viewtimes + data.viewtime, aggregate.userids + 1)
      })(materialized)
      .toStream
      .to(jobContext.outputTopicName)

    builder.build()
  }
}
