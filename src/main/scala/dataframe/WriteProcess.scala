package dataframe

import org.apache.spark.sql.{DataFrame, SaveMode}

class WriteProcess {
  def writeJSON(df: DataFrame, name: String): Unit = {
    df.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save(s"src/main/resources/data/json/$name")
  }
  def writeParquet(df: DataFrame, name: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(s"src/main/resources/data/parquet/$name")
  }
  def writeAvro(df: DataFrame, name: String): Unit = {
    df.write
      .format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save(s"src/main/resources/data/avro/$name")
  }
}
