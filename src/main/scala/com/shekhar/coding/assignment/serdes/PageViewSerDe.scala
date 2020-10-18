package com.shekhar.coding.assignment.serdes

import java.nio.charset.Charset
import java.util

import com.shekhar.coding.assignment.model.PageView
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write

//TODO: we can make this SerDe generic such that one class can be used for both
// user class and this page view class
class PageViewSerDe extends Serializer[PageView] with Deserializer[PageView] with Serde[PageView] {
  implicit val formats = DefaultFormats

  override def serialize(s: String, t: PageView): Array[Byte] = {
    write(t).getBytes(Charset.defaultCharset())
  }

  override def deserialize(s: String, bytes: Array[Byte]): PageView = {
    parse(new String(bytes)).extract[PageView]
  }

  override def close(): Unit = super.close()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    super.configure(configs, isKey)
  }

  override def serializer(): Serializer[PageView] = this

  override def deserializer(): Deserializer[PageView] = this
}
