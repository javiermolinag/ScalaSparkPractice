package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait initSpark {
  val spark: SparkSession = SparkSession.builder()
    .appName("Spotify spark")
    .master("local[*]")
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val rootLogger: Unit = Logger.getRootLogger.setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
  }
}
