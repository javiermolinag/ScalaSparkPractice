package rdd

import common.RDDCaseClass
import constants.Constants.{EightNumber, FalseBoolean, FiveNumber, FourNumber, MinusOneNUmber, OneNumber, SevenNumber, SixNumber, ThreeNumber, TrueBoolean, TwoNumber}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class ProcMovieLensRDD(spark: SparkSession) extends RDDCaseClass with ReadProcessRDD with Serializable{

  /*
  todo: Rule1
    Number of movies
  */
  private def NumberOfMovies(movieLensRDD: RDD[MovieLensData]): Unit = {
    movieLensRDD
      .groupBy(rdd => rdd.movieId)
      .foreach(println(_))
  }

  /*
  todo: Rule2
    Number of movies by year
  */
  private def moviesByYear(movieLensRDD: RDD[MovieLensData]): Unit = {
    movieLensRDD
      .map(rdd => ((rdd.year, rdd.movieId, rdd.title),rdd.title))
      .groupByKey()
      .flatMapValues(values => values.toSet.toList)
      .map{case ((year,movieId, title),_) => ((year, movieId, title), OneNumber)}
      .reduceByKey((a,b) => a + b)
      .coalesce(OneNumber)
      .sortBy(item => item._1,FalseBoolean)
      .foreach(println(_))
  }

  def runProcess(): Unit = {
    val fileName: String =  "movie-lens/movieLens/*.csv"
    val rdd: RDD[Array[String]] = ReadFromFileCSV(spark, fileName, ",", ProcMovieLensRDD.deleteCommaSpaceTransform)

    val movieLensRDD: RDD[MovieLensData] = rdd
      .filter(line => line.length == EightNumber)
      .map(line => line.toList)
      .map(line => MovieLensData(
        ProcMovieLensRDD.quitDoubleQuotesTransform(line.head),
        ProcMovieLensRDD.quitDoubleQuotesTransform(line(OneNumber)),
        ProcMovieLensRDD.deleteSemicolonSpaceTransform(
          ProcMovieLensRDD.quitDoubleQuotesTransform(line(TwoNumber))),
        ProcMovieLensRDD.findYearTransform(
          ProcMovieLensRDD.quitDoubleQuotesTransform(line(TwoNumber))),
        ProcMovieLensRDD.splitMoviesTransform(line(ThreeNumber)),
        ProcMovieLensRDD.toDoubleTransform(
          ProcMovieLensRDD.quitDoubleQuotesTransform(line(FourNumber))).getOrElse(MinusOneNUmber),
        ProcMovieLensRDD.quitDoubleQuotesTransform(line(FiveNumber)),
        ProcMovieLensRDD.quitDoubleQuotesTransform(line(SixNumber)),
        ProcMovieLensRDD.quitDoubleQuotesTransform(line(SevenNumber))
      ))

    // NumberOfMovies(movieLensRDD)
    moviesByYear(movieLensRDD)
  }

}

object ProcMovieLensRDD {
  private val deleteCommaSpaceTransform: String => String =
    item => item.replaceAll(", ","; ")
  private val deleteSemicolonSpaceTransform: String => String =
    item => item.replaceAll("; ",", ")
  private val findYearTransform: String => String =
    item => "[(](([0-9]{4})|([0-9]{4}-.*?))[)]$".r.findFirstIn(item).getOrElse("")
  private val quitDoubleQuotesTransform: String => String =
    item => item.replace("\"","")
  private val toDoubleTransform: String => Option[Double] =
    item => try {Some(item.toDouble)} catch {case e: Exception => None}
  private val splitMoviesTransform: String => List[String] =
    item => item.split("[|]").toList
}
