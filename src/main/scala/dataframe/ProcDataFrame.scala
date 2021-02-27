package dataframe

import common.Schemas.dataSchema
import constants.Constants
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, max, mean, min}

import scala.collection.mutable

class ProcDataFrame(spark: SparkSession){

  private val readSpark = new ReadProcess(spark)

  val dataFlow: mutable.Map[String, DataFrame] = mutable.Map(
    Constants.DataDF -> readSpark.readCSV(Constants.FileNameData,dataSchema)
  )

  /*
    todo: Rule1
      Get DF with the mean, max, min, and avg of "valence" column,
      Group results by "year"
  */
  def rule1(df: DataFrame): DataFrame = {
    df
      .groupBy(col(Constants.YearColumn))
      .agg(
        min(col(Constants.EnergyColumn)).as(Constants.MinValenceColumn),
        max(col(Constants.EnergyColumn)).as(Constants.MaxValenceColumn),
        mean(col(Constants.EnergyColumn)).as(Constants.MeanValenceColumn),
        avg(col(Constants.EnergyColumn)).as(Constants.AvgValenceColumn),
      )
  }

  /*
  todo: Rule2
    add a boolean column called "i_need_it" which is given by the valence column > 0.85
  */

  /*
  todo: Rule3
    find any relation between "valence", "tempo", "popularity" and "liveness" (use corr)
  */

  /*
  todo: Rule4
    make any join process
  */

  /*
  todo: Rule5
    show top 20 artists with the most popularity average songs (asc order)
  */

  /*
  todo: Rule6
    get the top 5 danceability songs for each artist
  */


  def runProcess(): Unit = {
    dataFlow(Constants.DataDF).printSchema()
    dataFlow(Constants.DataDF).show()

    dataFlow.put(Constants.Rule1,rule1(dataFlow(Constants.DataDF)))
    dataFlow(Constants.Rule1).show()


  }


}
