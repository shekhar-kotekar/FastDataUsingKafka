package com.shekhar.coding.assignment.wrappers

import java.io.{File, IOException}

import com.shekhar.coding.assignment.model.{PageView, User}
import com.typesafe.scalalogging.LazyLogging

import scala.io.Source

/**
 * Companion object used to generate test data
 */
object DataGenerator extends LazyLogging {
  private val testDataDirPath: String = "/test_data"

  def generateUserData: Seq[User] = {
    val userDataCsvFilePath: File = new File(getClass.getResource(s"$testDataDirPath/users.csv").getPath)
    logger.info(s"user data file path: $userDataCsvFilePath")
    // readLines method on File object is an implicit method created by us
    val lines: Seq[String] = userDataCsvFilePath.readLines
    val users: Seq[User] = lines
      .map(line => {
        val data = line.split(",")
        User(data(0).toLong, data(1), data(2), data(3))
      })
    logger.info(s"Number of user records generated: ${users.length}")
    users
  }

  //TODO : Parsing string to long without exception handling
  def generatePageViewsData: Seq[PageView] = {
    val pageViewsDataFilePath: File = new File(getClass.getResource(s"$testDataDirPath/pageviews.csv").getPath)
    logger.info(s"page views file path: $pageViewsDataFilePath")
    val lines: Seq[String] = pageViewsDataFilePath.readLines
    val pageViews: Seq[PageView] = lines.map(line => {
      val data = line.split(",")
      PageView(data(0).toLong, data(1), data(2))
    })
    logger.info(s"Number of page views records generated: ${pageViews.length}")
    pageViews
  }

  implicit class FileReader(filePath: File) {
    def readLines: Seq[String] = {
      try {
        val source = Source.fromFile(filePath, "UTF-8")
        val lines: Seq[String] = source.getLines().toList
        source.close()
        lines
      } catch {
        case ex: IOException => logger.error(s"Error occurred while reading $filePath file", ex)
          Seq.empty
      }
    }
  }

}
