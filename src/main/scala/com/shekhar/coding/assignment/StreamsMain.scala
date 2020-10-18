package com.shekhar.coding.assignment

import com.shekhar.coding.assignment.contexts.MostViewedStreamingJobContext
import com.shekhar.coding.assignment.jobs.{MostViewedPagesJob, StreamingJobs}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object StreamsMain extends App with LazyLogging {

  val config: Config = ConfigFactory.load()
  val jobContext: MostViewedStreamingJobContext = new MostViewedStreamingJobContext(config)
  val stremingJob: StreamingJobs = new MostViewedPagesJob(jobContext)
  val future: Future[Unit] = Future {
    Thread.sleep(5000)
  }
  future.onComplete( {
    case Success(_) => logger.info("streams job closed")
    case Failure(error) => logger.error("Unable to run streaming job because, ", error)
  })
  Thread.sleep(6000)
}
