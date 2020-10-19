package com.shekhar.coding.assignment.serdes

import java.nio.charset.Charset
import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write

class GenericSerDe[A <: AnyRef] extends Serializer[A] with Deserializer[A] with Serde[A] {
  implicit val formats = DefaultFormats

  override def serialize(s: String, t: A): Array[Byte] = {
    write(t).getBytes(Charset.defaultCharset())
  }

  override def deserialize(s: String, bytes: Array[Byte]): A = {
    parse(new String(bytes)).asInstanceOf[A]
  }

  override def close(): Unit = super.close()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    super.configure(configs, isKey)
  }

  override def serializer(): Serializer[A] = this

  override def deserializer(): Deserializer[A] = this
}

