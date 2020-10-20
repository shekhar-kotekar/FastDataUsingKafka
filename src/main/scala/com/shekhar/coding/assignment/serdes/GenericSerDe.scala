package com.shekhar.coding.assignment.serdes

import java.nio.charset.Charset
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write

//TODO : This serde is failing to de-seriliaze JSON to objects, no longer using it
class GenericSerDe[A <: AnyRef] extends Serializer[A] with Deserializer[A] with Serde[A] with LazyLogging {
  implicit val formats = DefaultFormats

  override def serialize(s: String, t: A): Array[Byte] = {
    write(t).getBytes(Charset.defaultCharset())
  }

  override def deserialize(s: String, bytes: Array[Byte]): A = {
    val input = new String(bytes)
    val deserializedObject = parse(input).asInstanceOf[A]
    deserializedObject
  }

  override def close(): Unit = super.close()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    super.configure(configs, isKey)
  }

  override def serializer(): Serializer[A] = this

  override def deserializer(): Deserializer[A] = this
}

