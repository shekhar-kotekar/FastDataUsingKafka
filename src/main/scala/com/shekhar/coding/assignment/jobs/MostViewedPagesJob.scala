package com.shekhar.coding.assignment.jobs

import com.shekhar.coding.assignment.contexts.MostViewedPagesJobContext
import com.typesafe.scalalogging.LazyLogging

/**
 * This class has methods to continously analyse top 10 most viewed pages by a view time
 * using Kafka streams
 * @param jobContext contains information needed to start the job
 */
class MostViewedPagesJob(jobContext: MostViewedPagesJobContext) extends LazyLogging {

}
