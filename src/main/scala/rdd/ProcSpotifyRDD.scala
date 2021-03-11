package rdd

import common.RDDCaseClass
import constants.Constants.{FalseBoolean, NineteenNUmber, TenNumber, TrueBoolean, ZeroNumber}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class ProcSpotifyRDD(spark: SparkSession) extends RDDCaseClass with ReadProcessRDD with Serializable {

  /*
    todo: Rule1
      Number of songs by year
  */
  def songByYear(rddSpotify: RDD[SpotifyData]): RDD[(String,Int)] = {
    rddSpotify
      .map(rdd => (rdd.year,1))
      .reduceByKey((a,b) => a + b)
  }

  /*
  todo: Rule2
    Most popular songs of each year
  */
  def mostPopularSongByYear(rddSpotify: RDD[SpotifyData]): RDD[(Int,Int,String,String)] = {
    rddSpotify // take most popular by year
      .map(rdd => (rdd.year.toInt,rdd))
      .reduceByKey((a,b) => {
        if (a.popularity > b.popularity) {a} else {b}
      })
      .map(rdd => (rdd._1,rdd._2.popularity,rdd._2.name,rdd._2.artists))
      .coalesce(1)
      .sortBy(a => a._1,TrueBoolean)

    rddSpotify // take 10 most popular by year
      .groupBy(rdd => rdd.year.toInt)
      .mapValues(item => item
        .toSeq
        .sortBy(a => a.popularity)(Ordering[Int].reverse)
        .take(TenNumber))
      .flatMapValues(values => values)
      .map(rdd => (rdd._1,rdd._2.popularity,rdd._2.name,rdd._2.artists))
      .coalesce(1)
      .sortBy(rdd => rdd._1)
  }

  /*
  todo: Rule3
    Number of songs for artist whose name begins with vowel
  */
  def songsNumberByFirstLetterArtist(rddSpotify: RDD[SpotifyData]): RDD[(Char,Int)] = {
    val toRemove = "\"'[]{}".toSet
    val toFilter = "^[AEIOU]$"
    rddSpotify
      .map(rdd => (rdd.artists.filterNot(toRemove).charAt(0).toUpper,1))
      .filter(rdd => rdd._1.toString.matches(toFilter))
      .reduceByKey((a,b) => a + b)
  }

  /*
  todo: Rule4
    Average of danceability by artist
  */
  case class GroupAvgDanceability(artist: String, average: Double, count: Long)

  def danceabilityByArtist(rddSpotify: RDD[SpotifyData]): RDD[GroupAvgDanceability] = {
    val toRemove = "\"'[]{}".toSet

    rddSpotify
      .map(rdd => (rdd.artists.filterNot(toRemove).toUpperCase,(rdd.danceability,1)))
      .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
      .map{
        case (a,(b,c)) => {GroupAvgDanceability(a,b/c,c)}
      }
      .filter(item => item.average > 0.9 && item.count > 5)
      .coalesce(1)
      .sortBy( item => item.average,FalseBoolean)

    rddSpotify
      .groupBy(rdd => rdd.artists.filterNot(toRemove).toUpperCase)
      .map{
        case (artist,rdd) => {GroupAvgDanceability(
          artist,
          rdd.map(item => item.danceability).sum/rdd.size,
          rdd.size
        )}
      }
      .filter(item => item.average > 0.9 && item.count > 5)
      .coalesce(1)
      .sortBy( item => item.average,FalseBoolean)
  }

  def runProcess(): Unit = {
    val fileName: String =  "data_.csv"
    val spotifyRegistry: String = "[0-9]*[.]?[0-9]*(e-[0-9]*)*,?"  // You can use a complex regex expr
    val rdd: RDD[Array[String]] = ReadFromFileCSV(spark, fileName, ",")

    val spotifyRDD: RDD[SpotifyData] = rdd
      .filter(tokens => tokens(ZeroNumber).matches(spotifyRegistry))
      .filter(tokens => tokens.length == NineteenNUmber)
      .map(tokens => new SpotifyData(tokens: _*))

    songByYear(spotifyRDD)
      // .foreach(println(_))
    mostPopularSongByYear(spotifyRDD)
      // .foreach(println(_))
    songsNumberByFirstLetterArtist(spotifyRDD)
      // .foreach(println(_))
    danceabilityByArtist(spotifyRDD)
      // .foreach(println(_))

  }

}
