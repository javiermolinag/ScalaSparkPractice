package common

import constants.Constants.TrueBoolean
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object Schemas {

  val dataSpotifySchema: StructType = StructType(Array(
    StructField("acousticness",DoubleType),
    StructField("artists",StringType),
    StructField("danceability",DoubleType),
    StructField("duration_ms",LongType),
    StructField("energy",DoubleType),
    StructField("explicit",IntegerType),
    StructField("id",StringType),
    StructField("instrumentalness",DoubleType),
    StructField("key",IntegerType),
    StructField("liveness",DoubleType),
    StructField("loudness",DoubleType),
    StructField("mode",IntegerType),
    StructField("name",StringType),
    StructField("popularity",IntegerType),
    StructField("release_date",StringType),
    StructField("speechiness",DoubleType),
    StructField("tempo",DoubleType),
    StructField("valence",DoubleType),
    StructField("year",StringType)
  ))

  val movieMovieLensSchema : StructType = StructType(Array(
    StructField("movieId",StringType,TrueBoolean),
    StructField("title",StringType,TrueBoolean),
    StructField("genres",StringType,TrueBoolean)
  ))

  val ratingMovieLensSchema : StructType = StructType(Array(
    StructField("userId",StringType,TrueBoolean),
    StructField("movieId",StringType,TrueBoolean),
    StructField("rating",StringType,TrueBoolean),
    StructField("timestamp",StringType,TrueBoolean)
  ))

  val tagMovieLensSchema : StructType = StructType(Array(
    StructField("userId",StringType,TrueBoolean),
    StructField("movieId",StringType,TrueBoolean),
    StructField("tag",StringType,TrueBoolean),
    StructField("timestamp",StringType,TrueBoolean)
  ))


}
