package rdd

import common.RDDCaseClass
import constants.Constants.{OneNumber, TrueBoolean}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

class ProcMovieLensRDD(spark: SparkSession) extends RDDCaseClass with ReadProcessRDD with Serializable{

  /*
  todo: Rule1
    Number of movies
  */
  private def NumberOfMovies(movieLensRDD: RDD[MovieLensData]): Unit = {
    val tot: Int = movieLensRDD
      .map(rdd => (rdd.movieId,1))
      .groupByKey()
      .map(item => (item._1,1))
      .reduce((a,b) => a._2 + b._2)
    println(tot)
  }

  /*
  todo: Rule2
    Number of movies by year
  */
  private def moviesByYear(movieLensRDD: RDD[MovieLensData]): Unit = {
    movieLensRDD
      .map(rdd => ((rdd.year, rdd.movieId),rdd.title))
      .groupByKey()
      .flatMapValues(values => values.toSet.toList)
      .map{case ((year,movieId),title) => (year,1)}
      .reduceByKey((a,b) => a + b)
      // .foreach(println(_))
  }

  def runProcess(): Unit = {
    val fileName: String =  "movie-lens/movieLens/*.csv"
    val rdd: RDD[Array[String]] = ReadFromFileCSV(spark, fileName, ",", ProcMovieLensRDD.deleteCommaSpaceTransform)

    val movieLensRDD: RDD[MovieLensData] = rdd
      .filter(line => line.length == 8)
      .map(line => line.toList)
      .map(line => MovieLensData(
        ProcMovieLensRDD.quitDoubleQuotesTransform(line.head),
        line(1),
        ProcMovieLensRDD.cleanTitleTransform(line(2)),
        ProcMovieLensRDD.findYearTransform(line(2)),
        ProcMovieLensRDD.splitMoviesTRansform(line(3)),
        ProcMovieLensRDD.toDoubleTransform(ProcMovieLensRDD.quitDoubleQuotesTransform(line(4))).getOrElse(-1),
        ProcMovieLensRDD.quitDoubleQuotesTransform(line(5)),
        ProcMovieLensRDD.quitDoubleQuotesTransform(line(6)),
        ProcMovieLensRDD.quitDoubleQuotesTransform(line(7))
      ))

    moviesByYear(movieLensRDD)
  }

}

object ProcMovieLensRDD {
  private val deleteCommaSpaceTransform: String => String = item => item.replaceAll(", ","; ")
  private val findYearTransform: String => String = item => "[(][0-9]{4}[-]{0,1}.*?[)]".r.findFirstIn(item).getOrElse("")
  private val quitDoubleQuotesTransform: String => String = item => item.replace("\"","")
  private val toDoubleTransform: String => Option[Double] = item => try {Some(item.toDouble)} catch {case e: Exception => None}
  private val splitMoviesTRansform: String => List[String] = item => item.split("|").toList
  private val cleanTitleTransform: String => String = item => {
    val replaceNameFirstMatch: Regex = "(.*?); (.*?) .*".r
    val replaceNameSecondMatch: Regex = "(.*?) [(][0-9]{4}[)]".r
    val itemAux: String = quitDoubleQuotesTransform(item)
    if (itemAux.contains(";"))
      replaceNameFirstMatch.replaceFirstIn(itemAux,"$2 $1")
    else
      replaceNameSecondMatch.replaceFirstIn(itemAux,"$1")
  }
}
