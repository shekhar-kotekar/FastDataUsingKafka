package com.shekhar.coding.assignment.jobs

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.shekhar.coding.assignment.contexts.MostViewedStreamingJobContext
import com.shekhar.coding.assignment.model.{AggregatedPageViews, AggregatedPageViewsSerDe, PageView, PageViewByUserSerDe, PageViewsByUser, User}
import com.shekhar.coding.assignment.serdes.{GenericSerDe, PageViewSerDe, UserJsonSerDe}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.{Grouped, JoinWindows, TimeWindows, Windowed, WindowedSerdes}
import org.apache.kafka.streams.scala.Serdes.String
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.{ByteArrayWindowStore, StreamsBuilder}

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

    //implicit val produced: Produced[String, PageView] = Produced.`with`(Serdes.String(), pageViewSerde)
    //pageViewsStream.to("output_topic")

    val usersByPageViewSerde = new PageViewByUserSerDe
    //implicit val produced: Produced[String, PageViewsByUser] = Produced.`with`(Serdes.String(), usersByPageViewSerde)
    implicit val streamJoined: StreamJoined[String, User, PageView] = StreamJoined.`with`(Serdes.String(), userSerDe, pageViewSerde)

    val windowSize = TimeUnit.MILLISECONDS.toMillis(jobContext.windowSizeInMilliSeconds)
    val advanceWindowBy = TimeUnit.MILLISECONDS.toMillis(jobContext.advanceWindowByInMilliSeconds)

    implicit val grouped: Grouped[String, PageViewsByUser] = Grouped.`with`(Serdes.String(), usersByPageViewSerde)
    implicit val valueSerde = new AggregatedPageViewsSerDe
    implicit val materialized = Materialized.`with`[String, AggregatedPageViews , ByteArrayWindowStore]

    val timeWindowSerde = WindowedSerdes.timeWindowedSerdeFrom(classOf[String])
    implicit val produced: Produced[Windowed[String], AggregatedPageViews] = Produced.`with`(timeWindowSerde, valueSerde)

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
      .to("output_topic")

//    val joinedStream = usersStreams
//      .join(pageViewsStream)(
//        (user: User, pageView: PageView) => PageViewsByUser(user.gender, pageView.pageid, pageView.viewtime),
//        JoinWindows.of(Duration.ofDays(1)))
//      .map((_, value) => (s"${value.pageid}_${value.gender}", value))
//      .groupByKey(Grouped.`with`(Serdes.String(), usersByPageViewSerde))
//      .windowedBy(TimeWindows.of(Duration.ofMillis(windowSize)).advanceBy(Duration.ofMillis(advanceWindowBy)))
//      .aggregate(AggregatedPageViews.empty)((_, data, aggregate) => {
//        AggregatedPageViews(data.gender, data.pageid, aggregate.viewtimes + data.viewtime, aggregate.userids + 1)
//      })(materialized)
//      .toStream
//
//    val timeWindowSerde = WindowedSerdes.timeWindowedSerdeFrom(classOf[String])
//    implicit val produced: Produced[Windowed[String], AggregatedPageViews] = Produced.`with`(timeWindowSerde, valueSerde)
//    joinedStream.to("output_topic")

    builder.build()
  }
}
