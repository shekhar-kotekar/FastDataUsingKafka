package com.shekhar.coding.assignment.contexts

import java.util.Properties

trait JobContexts {
  def properties: Properties
}

object JobContexts {
  val propertiesRoot: String = "com.shekhar.assignment.properties"
}