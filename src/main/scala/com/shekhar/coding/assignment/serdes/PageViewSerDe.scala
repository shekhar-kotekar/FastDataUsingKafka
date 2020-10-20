package com.shekhar.coding.assignment.serdes

import java.nio.charset.Charset
import java.util

import com.shekhar.coding.assignment.model.PageView
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write

class PageViewSerDe extends Serializer[PageView] with Deserializer[PageView] with Serde[PageView] with LazyLogging {
  implicit val formats = DefaultFormats

  override def serialize(s: String, t: PageView): Array[Byte] = {
    write(t).getBytes(Charset.defaultCharset())
  }

  override def deserialize(s: String, bytes: Array[Byte]): PageView = {
    val input = new String(bytes)
    logger.info(s"deserializing $input")
    parse(input).extract[PageView]
  }

  override def close(): Unit = super.close()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    super.configure(configs, isKey)
  }

  override def serializer(): Serializer[PageView] = this

  override def deserializer(): Deserializer[PageView] = this
}
