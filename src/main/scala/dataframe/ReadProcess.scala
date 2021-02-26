package dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

class ReadProcess(spark: SparkSession) {
  def readCSV(name: String, schema: StructType): DataFrame = {
    val df: DataFrame = spark
      .read
      .format("csv")
      .option("header","true")
      .option("sep",",")
      .option("mode", "failFast") // dropMalformed, permissive (default) failFast
      .option("badRecordsPath", "src/main/resources/data/deleted")
      .schema(schema)
      .load(s"src/main/resources/data/csv/$name")
    df
  }
  def readJSON(name: String, schema: StructType): DataFrame = {
    val df: DataFrame = spark
      .read
      .format("json")
      .schema(schema)
      .load(s"src/main/resources/data/json/$name")
    df
  }
  def readParquet(name: String): DataFrame = {
    val df: DataFrame = spark
      .read
      .parquet(s"src/main/resources/data/parquet/$name")
    df
  }
  def readAvro(name: String): DataFrame = {
    val df: DataFrame = spark
      .read
      .format("avro")
      .load(s"src/main/resources/data/avro/$name")
    df
  }
}
