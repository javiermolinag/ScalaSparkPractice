package rdd

import common.RDDCaseClass
import constants.Constants.{EightNumber, FalseBoolean, FiveNumber, FourDotFIveNumber, FourNumber, MinusOneNUmber, OneNumber, SevenNumber, SixNumber, ThreeNumber, TrueBoolean, TwoNumber}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.HashPartitioner

class ProcMovieLensRDD(spark: SparkSession) extends RDDCaseClass with ReadProcessRDD with Serializable{

  /*
  todo: Load data
  */
  def loadData(fileName: String): RDD[MovieLensData] = {
    val rdd: RDD[Array[String]] = ReadFromFileCSV(
      spark,
      fileName,
      ",",
      ProcMovieLensRDD.loadTransform
    )

    rdd
      .filter(line => line.length == EightNumber)
      .map(line => line.toList)
      .map(line => MovieLensData(
        ProcMovieLensRDD.quitDoubleQuotesTransform(line.head),
        ProcMovieLensRDD.quitDoubleQuotesTransform(line(OneNumber)),
        ProcMovieLensRDD.deleteSemicolonSpaceTransform(
          ProcMovieLensRDD.quitDoubleQuotesTransform(line(TwoNumber))),
        ProcMovieLensRDD.quitParenthesisTransform(
          ProcMovieLensRDD.findYearTransform(
            ProcMovieLensRDD.quitDoubleQuotesTransform(line(TwoNumber)))),
        ProcMovieLensRDD.splitMoviesTransform(line(ThreeNumber)),
        ProcMovieLensRDD.toDoubleTransform(
          ProcMovieLensRDD.quitDoubleQuotesTransform(line(FourNumber))).getOrElse(MinusOneNUmber),
        ProcMovieLensRDD.quitDoubleQuotesTransform(line(FiveNumber)),
        ProcMovieLensRDD.quitDoubleQuotesTransform(line(SixNumber)),
        ProcMovieLensRDD.quitDoubleQuotesTransform(line(SevenNumber))
      ))
  }

  /*
  todo: Rule1
    Number of movies
  */
  private def NumberOfMovies(movieLensRDD: RDD[MovieLensData]): Unit = {
    val tot = movieLensRDD
      .groupBy(rdd => rdd.movieId)
      .partitionBy(new HashPartitioner(20))
      .count()

    println(tot)
  }

  private def NumberOfMoviesFast(movieLensRDD: RDD[MovieLensData]): Unit = {
    val tot = movieLensRDD
      .map(rdd => (rdd.movieId,OneNumber))
      .partitionBy(new HashPartitioner(20))
      .reduceByKey(_ + _)
      .map(rdd => rdd._1)
      .count()

    println(tot)
  }

  /*
  todo: Rule2
    Number of movies by year
  */
  private def moviesByYear(movieLensRDD: RDD[MovieLensData]): Unit = {
    movieLensRDD
      .map(rdd => ((rdd.year, rdd.movieId, rdd.title), rdd.title))
      .groupByKey()
      .flatMapValues(values => values.toSet.toList)
      .map{case ((year, _, _),_) => (year, OneNumber)}
      .reduceByKey((a,b) => a + b)
      .coalesce(OneNumber)
      .sortBy(item => item._1,FalseBoolean)
      .foreach(println(_))
  }

  private def moviesByYearFast(movieLensRDD: RDD[MovieLensData]): Unit = {
    movieLensRDD
      .map(rdd => ((rdd.year, rdd.movieId), OneNumber))
      .reduceByKey(_ + _)
      .map{case ((year, _), _) => (year, OneNumber)}
      .reduceByKey((a,b) => a + b)
      .coalesce(OneNumber)
      .sortBy(item => item._1,FalseBoolean)
      .foreach(println(_))
  }

  /*
  todo: Rule3
    Show the best movies (avg rating > 4.5)
  */
  private def bestMovies(movieLensRDD: RDD[MovieLensData]): Unit = {
    case class Average(movieId: String, average: Double, count: Long)
    movieLensRDD
      .filter(rdd => rdd.rating != -1)
      .map(rdd => (rdd.movieId, rdd.userId, rdd.rating))
      .coalesce(FourNumber)
      .distinct()
      .map { case (movieId, _, rating) => (movieId, rating)}
      .groupBy(item => item._1)
      .map { case (movieId, ratingRDD) => Average(
        movieId,
        ratingRDD.map(item => item._2).sum/ratingRDD.size,
        ratingRDD.size)}
      .filter(item => item.average > FourDotFIveNumber)
      .coalesce(OneNumber)
      .sortBy(item => item.movieId.toLong, FalseBoolean)
      .foreach(println(_))
  }
  private def bestMoviesFast(movieLensRDD: RDD[MovieLensData]): Unit = {
    movieLensRDD
      .filter(rdd => rdd.rating != -1)
      .map(rdd => ((rdd.movieId, rdd.userId, rdd.rating), OneNumber))
      .partitionBy(new HashPartitioner(20))
      .reduceByKey(_ + _)
      .map { case ((movieId, _, rating), _) => (movieId, (OneNumber, rating))}
      .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
      .map { case (movieId, (count, sum)) => (movieId, sum/count, count)}
      .filter(item => item._2 > FourDotFIveNumber)
      .coalesce(1)
      .sortBy(item => item._1.toLong, FalseBoolean)
      .foreach(println(_))
  }

  def runProcess(): Unit = {
    val fileName: String =  "movie-lens/movieLens/*.csv"
    val movieLensRDD: RDD[MovieLensData] = loadData(fileName)
    println("Repartition size : " + movieLensRDD.partitions.length)

    // NumberOfMovies(movieLensRDD)
    // NumberOfMoviesFast(movieLensRDD)

    // moviesByYear(movieLensRDD)
    // moviesByYearFast(movieLensRDD)

    // bestMovies(movieLensRDD)
    // bestMoviesFast(movieLensRDD)

    val t0 = System.nanoTime()
    bestMoviesFast(movieLensRDD)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/10e8 + "s")

  }

}

object ProcMovieLensRDD {
  private val loadTransform: String => String =
    item => item.replaceAll(", ","; ")
  private val deleteSemicolonSpaceTransform: String => String =
    item => item.replaceAll("; ",", ")
  private val findYearTransform: String => String =
    item => "[(]([0-9]{4})[)] *$".r.findFirstIn(item).getOrElse("")
  private val quitDoubleQuotesTransform: String => String =
    item => item.replace("\"","")
  private val quitParenthesisTransform: String => String =
    item => item.replace("[() ]","")
  private val toDoubleTransform: String => Option[Double] =
    item => try {Some(item.toDouble)} catch {case e: Exception => None}
  private val splitMoviesTransform: String => List[String] =
    item => item.split("[|]").toList
}
