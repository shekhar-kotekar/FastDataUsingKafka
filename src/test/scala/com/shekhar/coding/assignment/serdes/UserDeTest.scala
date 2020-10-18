package com.shekhar.coding.assignment.serdes

import com.shekhar.coding.assignment.model.User
import org.scalatest.{Matchers, WordSpec}

class UserDeTest extends WordSpec with Matchers {
  private val userSerDe: UserJsonSerDe = new UserJsonSerDe
  private val dummyUser: User = User(1L, "dummy_user_id_1", "dummy_Region_id_1", "MALE")
  private val topicName: String = "dummy_topic"

  "User Serde class" should {
    "be able to serialize and deserialize user object to avro type" in {
      val serializedUser: Array[Byte] = userSerDe.serialize(topicName, dummyUser)
      val deserializedUser: User = userSerDe.deserialize(topicName, serializedUser)
      deserializedUser.gender shouldEqual(dummyUser.gender)
      deserializedUser.regionid shouldEqual(dummyUser.regionid)
      deserializedUser.registertime shouldEqual(dummyUser.registertime)
      deserializedUser.userid shouldEqual(dummyUser.userid)
    }
  }

}
