package dataframe

import common.Schemas.{movieMovieLensSchema, ratingMovieLensSchema, tagMovieLensSchema}
import constants.Constants
import org.apache.spark.sql.functions.{col, explode, regexp_extract, split}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable

class ProcMovieLensDataFrame(spark: SparkSession) {

  private val readSpark = new ReadProcess(spark)

  val dataFlow: mutable.Map[String, DataFrame] = mutable.Map(
    Constants.MovieLensMovieDf -> readSpark.readCSV(Constants.MovieLensMoviePath,movieMovieLensSchema),
    Constants.MovieLensRatingDf -> readSpark.readCSV(Constants.MovieLensRatingPath,ratingMovieLensSchema),
    Constants.MovieLensTagDf -> readSpark.readCSV(Constants.MovieLensTagPath,tagMovieLensSchema)
  )

  /*
    todo: Rule1
      Add the "year" column, which is extracted from the "title" column
      Use: regexp_extract
  */
  def rule1(df: DataFrame): DataFrame = {
    val newCol: Column = regexp_extract(col(Constants.TitleColumn),"([0-9]{4})",1)
      .alias(Constants.YearColumn)
    df.select(df.columns.map(col) :+ newCol :_*)
  }

  /*
  todo: Rule1.1
    Delete the year expression in the "title" column
  */

  /*
  todo: Rule2
    Convert the "genres" column to list
    Use: split
  */
  def rule2(df: DataFrame): DataFrame = {
    val newCol: Column = split(col(Constants.GenresColumn),"\\|")
      .alias(Constants.GenresColumn)
    df.select(df.columns.map( columnName => {
      columnName match {
        case Constants.GenresColumn => newCol
        case _ => col(columnName)
      }
    }) :_*)
  }

  /*
  todo: Rule3
    generates a new dataframe with columns: "title", genres (decompose all different genres in columns),
    example of columns: "title", "Action", "Adventure", "Animation", etc.
    For each title show the count() of genres
    Use: explode, groupBy, pivot, count
  */
  def rule3(df: DataFrame): DataFrame = {
    val genresCol: Column = explode(col(Constants.GenresColumn))
      .alias(Constants.GenresColumn)
    val dfWithGenres: DataFrame = df
      .select(df.columns.map(colName =>{
        colName match {
          case Constants.GenresColumn => genresCol
          case _ => col(colName)
        }
      } ) :_*)
    dfWithGenres
      .groupBy(col(Constants.MovieIdColumn))
      .pivot(col(Constants.GenresColumn))
      .count()
      .na
      .fill(Constants.ZeroNumber)
  }

  /*
  todo: Rule4
    Generate a dataframe with your favourite movies, add a "title" and a "rating" columns
  */
  def rule4(df: DataFrame): DataFrame = {
    df
  }

  /*
  todo: Rule5
    Add the "id" to your favourite movies - rating given the original movie table
  */
  def rule5(df: DataFrame): DataFrame = {
    df
  }



  /*
  todo: Rule n1
    how many movies have seen each user?
  */

  /*
  todo: Rule n2
    show the avg, stddev, max and min "rating" (given by all users) of each movie,
  */

  /*
  todo: Rule n3
    list the top 50 movies using the avg of "rating"
  */

  /*
  todo: Rule n4
    Show all different tags (list of strings) for each movie in a column called "tags"
  */

  def runProcess(): Unit = {
    dataFlow(Constants.MovieLensTagDf).printSchema()
    dataFlow(Constants.MovieLensTagDf).show(5)

    dataFlow(Constants.MovieLensRatingDf).printSchema()
    dataFlow(Constants.MovieLensRatingDf).show(5)

    dataFlow(Constants.MovieLensMovieDf).printSchema()
    dataFlow(Constants.MovieLensMovieDf).show(5)


    dataFlow.put(Constants.Rule1,rule1(dataFlow(Constants.MovieLensMovieDf)))
    dataFlow(Constants.Rule1).show()

    dataFlow.put(Constants.Rule2,rule2(dataFlow(Constants.Rule1)))
    println(dataFlow(Constants.Rule2).count())
    dataFlow(Constants.Rule2).show()
    dataFlow(Constants.Rule2).printSchema()

    dataFlow.put(Constants.Rule3,rule3(dataFlow(Constants.Rule2)))
    println(dataFlow(Constants.Rule3).count())
    dataFlow(Constants.Rule3).show()
    dataFlow(Constants.Rule3).printSchema()

  }
}
