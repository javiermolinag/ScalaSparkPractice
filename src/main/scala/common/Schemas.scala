package common

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object Schemas {

  val dataSchema: StructType = StructType(Array(
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

}
