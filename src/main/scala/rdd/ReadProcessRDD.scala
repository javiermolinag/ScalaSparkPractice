package rdd

import constants.Constants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

trait ReadProcessRDD {

  def ReadFromFileCSV(
                       spark: SparkSession, name: String, separator: String, transform: String => String = line => line
                     ): RDD[Array[String]] = {
    val rdd: RDD[String] = spark.sparkContext.textFile(Constants.sourcePath + name)

    rdd
      .map(line => transform(line))
      .map(line => line.split(separator))
  }

}
