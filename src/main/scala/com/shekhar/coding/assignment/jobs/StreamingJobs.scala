package com.shekhar.coding.assignment.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.Topology

/**
 * Trait to be used by all the streaming jobs
 */
trait StreamingJobs extends LazyLogging {
  /**
   * Each implementation of this trait should implement following method
   * @return Object of type Topology which can be used to create and run Streaming job
   */
  def getTopology: Topology
}
