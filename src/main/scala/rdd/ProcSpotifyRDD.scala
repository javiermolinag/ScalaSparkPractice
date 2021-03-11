package rdd

import common.RDDCaseClass
import constants.Constants
import constants.Constants.ZeroNumber
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class ProcSpotifyRDD(spark: SparkSession) extends RDDCaseClass with ReadProcessRDD with Serializable {

  def runProcess(): Unit = {
    val fileName: String =  "data_.csv"
    val spotifyRegistry: String = "[0-9]*[.]?[0-9]*(e-[0-9]*)*,?"  // You can use a complex regex expr
    val rdd: RDD[Array[String]] = ReadFromFileCSV(spark, fileName, ",")

    val spotifyRDD: RDD[SpotifyData] = rdd
      .filter(tokens => tokens(ZeroNumber).matches(spotifyRegistry))
      .filter(tokens => tokens.length == 19)
      .map(tokens => new SpotifyData(tokens: _*))

    spotifyRDD.foreach(println(_))

  }

}
