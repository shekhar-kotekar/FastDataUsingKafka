package com.shekhar.coding.assignment

import com.shekhar.coding.assignment.contexts.MostViewedStreamingJobContext
import com.shekhar.coding.assignment.jobs.{MostViewedPagesJob, StreamingJobs}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.KafkaStreams

/**
 * Main object responsible for configuring objects needed to create and run job.
 * To run this job we need to provide absolute path to .conf file using -Dconfig.file=<absolute path> parameter
 */
object StreamsMain extends LazyLogging {

  private var mostViewedPagesJob: KafkaStreams = _

  def main(args: Array[String]): Unit = {
    try {
      // read config file and provide it to job context class to create the actual context
      val config: Config = ConfigFactory.load()
      val jobContext: MostViewedStreamingJobContext = new MostViewedStreamingJobContext(config)

      // provide context object to streaming job class so as to create topology of the job
      val stremingJob: StreamingJobs = new MostViewedPagesJob(jobContext)

      // start the job if everything is correct
      mostViewedPagesJob = new KafkaStreams(stremingJob.getTopology, jobContext.properties)
      mostViewedPagesJob.start()
    } catch {
      case ex: Exception => logger.error("Streaming job failed because", ex)
    } finally {
      // close the streaming job before exiting
      if(mostViewedPagesJob != null) {
        mostViewedPagesJob.close()
      }
    }
  }
}
