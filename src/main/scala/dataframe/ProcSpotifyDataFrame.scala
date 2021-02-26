package dataframe

import common.Schemas.dataSchema
import constants.Constants
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, max, min, rank, stddev, when}

import scala.collection.mutable

class ProcSpotifyDataFrame(spark: SparkSession){

  private val readSpark = new ReadProcess(spark)

  val dataFlow: mutable.Map[String, DataFrame] = mutable.Map(
    Constants.DataDF -> readSpark.readCSV(Constants.FileNameData,dataSchema)
  )

  /*
    todo: Rule1
      Get DF with the mean, max, min, stddev and avg of "valence" column,
      Group results by "year"
      Use: groupBy and agg functions
  */
  def rule1(df: DataFrame): DataFrame = {
    df
      .groupBy(col(Constants.YearColumn))
      .agg(
        min(col(Constants.EnergyColumn)).as(Constants.MinValenceColumn),
        max(col(Constants.EnergyColumn)).as(Constants.MaxValenceColumn),
        stddev(col(Constants.EnergyColumn)).as(Constants.StddevValenceColumn),
        avg(col(Constants.EnergyColumn)).as(Constants.AvgValenceColumn),
      )
  }

  /*
  todo: Rule2
    is there any relation between "valence", "tempo", "popularity" and "liveness"?
    Use: stat.corr
  */
  def rule2(df: DataFrame): DataFrame = {
    val corrSeq = Seq(
      (Constants.ValenceColumn,
        df.stat.corr(Constants.ValenceColumn,Constants.ValenceColumn),
        df.stat.corr(Constants.ValenceColumn,Constants.TempoColumn),
        df.stat.corr(Constants.ValenceColumn,Constants.PopularityColumn),
        df.stat.corr(Constants.ValenceColumn,Constants.LivenessColumn)),
      (Constants.TempoColumn,
        df.stat.corr(Constants.TempoColumn,Constants.ValenceColumn),
        df.stat.corr(Constants.TempoColumn,Constants.TempoColumn),
        df.stat.corr(Constants.TempoColumn,Constants.PopularityColumn),
        df.stat.corr(Constants.TempoColumn,Constants.LivenessColumn)),
      (Constants.PopularityColumn,
        df.stat.corr(Constants.PopularityColumn,Constants.ValenceColumn),
        df.stat.corr(Constants.PopularityColumn,Constants.TempoColumn),
        df.stat.corr(Constants.PopularityColumn,Constants.PopularityColumn),
        df.stat.corr(Constants.PopularityColumn,Constants.LivenessColumn)),
      (Constants.LivenessColumn,
        df.stat.corr(Constants.LivenessColumn,Constants.ValenceColumn),
        df.stat.corr(Constants.LivenessColumn,Constants.TempoColumn),
        df.stat.corr(Constants.LivenessColumn,Constants.PopularityColumn),
        df.stat.corr(Constants.LivenessColumn,Constants.LivenessColumn)),
    )
    val columnNames = Seq(
      "data",
      Constants.ValenceColumn,
      Constants.TempoColumn,
      Constants.PopularityColumn,
      Constants.LivenessColumn
    )
    spark.createDataFrame(corrSeq).toDF(columnNames:_*)
  }

  /*
  todo: Rule3
    add a boolean column called "i_need_it" which is given by the valence column > 0.85
    Use: when
  */
  def rule3(df: DataFrame): DataFrame = {
    val condition: Column = col(Constants.ValenceColumn) > Constants.ZeroDotEightFiveNumber
    val newCol: Column = when(condition, Constants.TrueBoolean)
      .otherwise(Constants.FalseBoolean)
      .as(Constants.INeedItColumn)
    df.select(df.columns.map(col) :+ newCol :_*)
  }

  /*
  todo: Rule4
    show top 20 artists with the most popularity average songs (asc order)
    Use: groupBy, agg functions, sort and limit
  */
  def rule4(df: DataFrame): DataFrame = {
    df
      .groupBy(col(Constants.ArtistsColumn))
      .agg(
        avg(Constants.PopularityColumn).alias(Constants.AvgPopularityColumn)
      )
      .sort(col(Constants.AvgPopularityColumn).desc_nulls_first)
      .limit(Constants.TwentyNUmber)
      .toDF()
  }

  /*
  todo: Rule5
    get the top 5 danceability songs for each artist,
    add a new column named "to_dance"
    Use: when, window, partitionBy orderBy
  */
  def rule5(df: DataFrame): DataFrame = {
    val window: WindowSpec = Window
      .partitionBy(Constants.ArtistsColumn)
      .orderBy(col(Constants.DanceabilityColumn).desc_nulls_first)
    val rankCol: Column = rank().over(window).alias(Constants.RankColumn)
    val conditionCol: Column = rankCol <= Constants.FiveNumber
    val newCol = when(conditionCol,Constants.TrueBoolean)
      .otherwise(Constants.FalseBoolean)
      .alias(Constants.ToDanceColumn)
    val dfRank = df.select(df.columns.map(col) :+ rankCol :_*)
    dfRank.select(dfRank.columns.map(col) :+ newCol :_*)
  }

  def runProcess(): Unit = {
    dataFlow(Constants.DataDF).printSchema()
    dataFlow(Constants.DataDF).show()

    dataFlow.put(Constants.Rule1,rule1(dataFlow(Constants.DataDF)))
    println("Rule 1 result")
    dataFlow(Constants.Rule1).orderBy(Constants.YearColumn).show()

    dataFlow.put(Constants.Rule2,rule2(dataFlow(Constants.DataDF)))
    println("Rule 2 result")
    dataFlow(Constants.Rule2).show()

    dataFlow.put(Constants.Rule3,rule3(dataFlow(Constants.DataDF)))
    println("Rule 3 result")
    dataFlow(Constants.Rule3).filter(col(Constants.INeedItColumn)).orderBy(Constants.YearColumn).show()

    dataFlow.put(Constants.Rule4,rule4(dataFlow(Constants.Rule3)))
    println("Rule 4 result")
    dataFlow(Constants.Rule4).show(false)

    dataFlow.put(Constants.Rule5,rule5(dataFlow(Constants.Rule3)))
    println("Rule 5 result")
    dataFlow(Constants.Rule5).filter(col(Constants.ToDanceColumn)).show()
  }


}
