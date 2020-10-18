package com.shekhar.coding.assignment.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.Topology

trait StreamingJobs extends LazyLogging {
  def getTopology: Topology
}
