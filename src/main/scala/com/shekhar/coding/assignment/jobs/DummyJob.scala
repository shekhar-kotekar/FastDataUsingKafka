//package com.shekhar.coding.assignment.jobs
//
//import java.util.Properties
//
//import com.shekhar.coding.assignment.contexts.MostViewedStreamingJobContext
//import com.shekhar.coding.assignment.serdes.UserJsonSerDe
//import org.apache.kafka.common.serialization.Serdes
//import org.apache.kafka.streams.StreamsConfig
//import org.apache.kafka.streams.kstream.Consumed
//import org.apache.kafka.streams.scala.StreamsBuilder
//
//class DummyJob(jobContext: MostViewedStreamingJobContext) extends StreamingJobs {
//
//  implicit val consumed = Consumed.`with`(Serdes.String(), new UserJsonSerDe)
//
//  val builder: StreamsBuilder = new StreamsBuilder
///*
//
//  val properties: Properties = new Properties()
//  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my_application_id")
//  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//  properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
//  properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[UserJsonSerDe])
//*/
//
//  builder
//    .stream("users")
//    .foreach((key, value) => logger.info(s"key: $key, value: $value"))
//
//  val topology = builder.build()
//
//  override def startJob: Boolean = {
//    logger.info("streaming job started.")
//    true
//  }
//
//  override def close(): Unit = {
//    logger.info("streaming job closed.")
//  }
//}