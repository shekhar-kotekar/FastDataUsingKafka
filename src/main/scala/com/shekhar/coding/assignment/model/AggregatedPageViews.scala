package com.shekhar.coding.assignment.model

import java.nio.charset.Charset
import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write

case class AggregatedPageViews(gender: String, pageid: String, viewtimes: Long, userids: Long)

class AggregatedPageViewsSerDe extends Serializer[AggregatedPageViews] with Deserializer[AggregatedPageViews] with Serde[AggregatedPageViews] {
  implicit val formats = DefaultFormats

  override def serialize(s: String, t: AggregatedPageViews): Array[Byte] = {
    write(t).getBytes(Charset.defaultCharset())
  }

  override def deserialize(s: String, bytes: Array[Byte]): AggregatedPageViews = {
    parse(new String(bytes)).extract[AggregatedPageViews]
  }

  override def close(): Unit = super.close()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    super.configure(configs, isKey)
  }

  override def serializer(): Serializer[AggregatedPageViews] = this

  override def deserializer(): Deserializer[AggregatedPageViews] = this
}

object AggregatedPageViews {
  def empty: AggregatedPageViews = AggregatedPageViews("", "", 0L, 0L)
}
