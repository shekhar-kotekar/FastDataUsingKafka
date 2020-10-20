package com.shekhar.coding.assignment.model

//TODO: create enum for gender type
/**
 * Represents one user
 * @param registertime time at which user registered
 * @param userid user ID
 * @param regionid region ID for user
 * @param gender Users gender - We need to make it an Enum instead of string
 */
case class User(registertime: Long = 0L, userid: String, regionid: String = "", gender: String = "")
