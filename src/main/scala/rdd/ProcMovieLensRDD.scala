package rdd

import common.RDDCaseClass
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class ProcMovieLensRDD(spark: SparkSession) extends RDDCaseClass with ReadProcessRDD with Serializable{

  def runProcess(): Unit = {
    val fileName: String =  "movie-lens/movieLens/*.csv"
    val rdd: RDD[Array[String]] = ReadFromFileCSV(spark, fileName, ",")
    Unit
  }

}
