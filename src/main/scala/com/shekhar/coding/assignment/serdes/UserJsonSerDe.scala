package com.shekhar.coding.assignment.serdes

import java.nio.charset.Charset
import java.util

import com.shekhar.coding.assignment.model.User
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write

/**
 * This class provides methods to serialize and de-serialize user objects
 */
class UserJsonSerDe extends Serializer[User] with Deserializer[User] with Serde[User] with LazyLogging {

  implicit val formats = DefaultFormats

  override def serialize(s: String, t: User): Array[Byte] = {
    write(t).getBytes(Charset.defaultCharset())
  }

  override def deserialize(s: String, bytes: Array[Byte]): User = {
    val input = new String(bytes)
    logger.debug(s"deserializing $input")
    parse(input).extract[User]
  }

  override def close(): Unit = super.close()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    super.configure(configs, isKey)
  }

  override def serializer(): Serializer[User] = this

  override def deserializer(): Deserializer[User] = this
}
