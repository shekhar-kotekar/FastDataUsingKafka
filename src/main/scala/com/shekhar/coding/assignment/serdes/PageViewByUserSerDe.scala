package com.shekhar.coding.assignment.serdes

import java.nio.charset.Charset
import java.util

import com.shekhar.coding.assignment.model.PageViewsByUser
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write

/**
 * This class provides serialization and de-serialization methods for page view by user objects
 */
class PageViewByUserSerDe extends Serializer[PageViewsByUser]
  with Deserializer[PageViewsByUser]
  with Serde[PageViewsByUser] with LazyLogging {

  implicit val formats = DefaultFormats

  override def serialize(s: String, t: PageViewsByUser): Array[Byte] = {
    write(t).getBytes(Charset.defaultCharset())
  }

  override def deserialize(s: String, bytes: Array[Byte]): PageViewsByUser = {
    val input = new String(bytes)
    parse(input).extract[PageViewsByUser]
  }

  override def close(): Unit = super.close()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    super.configure(configs, isKey)
  }

  override def serializer(): Serializer[PageViewsByUser] = this

  override def deserializer(): Deserializer[PageViewsByUser] = this
}
