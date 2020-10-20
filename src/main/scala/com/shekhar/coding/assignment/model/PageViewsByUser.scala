package com.shekhar.coding.assignment.model

/**
 * This class is used while joining user stream with the page view stream
 * @param gender Users gender
 * @param pageid Viewed page ID
 * @param viewtime Actual view time for the page
 */
case class PageViewsByUser(gender: String, pageid: String, viewtime: Long)
