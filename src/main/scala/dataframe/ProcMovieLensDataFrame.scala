package dataframe

import common.Schemas.{movieMovieLensSchema, ratingMovieLensSchema, tagMovieLensSchema}
import constants.Constants
import org.apache.spark.sql.functions.{broadcast, col, explode, lit, ltrim, regexp_extract, rtrim, split, substring_index, sum}
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
  todo: Rule2
    Delete the year expression in the "title" column
    Use: substring_index, rtrim, ltrim
  */
  def rule2(df: DataFrame): DataFrame = {
    val newCol: Column = substring_index(col(Constants.TitleColumn),"(",1)
      .alias(Constants.TitleColumn)
    df.select(df.columns.map(colName => {
      colName match {
        case Constants.TitleColumn => ltrim(rtrim(newCol)).alias(Constants.TitleColumn)
        case _ => col(colName)
      }
    }) :_*)
  }

  /*
  todo: Rule3
    Convert the "genres" column to list
    Use: split
  */
  def rule3(df: DataFrame): DataFrame = {
    val newCol: Column = split(col(Constants.GenresColumn),"\\|")
      .alias(Constants.GenresColumn)
    df.select(df.columns.map( colName => {
      colName match {
        case Constants.GenresColumn => newCol
        case _ => col(colName)
      }
    }) :_*)
  }

  /*
  todo: Rule4
    generates a new dataframe (genres count df) with columns: "title", genres (decompose all different genres in columns),
    example of columns: "title", "Action", "Adventure", "Animation", etc.
    For each title show the count() of genres
    Use: explode, groupBy, pivot, count
  */
  def rule4(df: DataFrame): DataFrame = {
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
  todo: Rule5
    Generate a dataframe with your favourite movies, add the "title" and "rating" columns
  */
  def rule5(): DataFrame = {
    val myMovies = Seq(
      ("Toy Story (1995)", 5.0),
      ("Jumanji (1995)", 4.0),
      ("Grumpier Old Men (1995)", 2.0),
      ("Waiting to Exhale (1995)", 2.0)
    )
    import spark.implicits._
    myMovies.toDF(Constants.TitleColumn,Constants.RatingColumn)
  }

  /*
  todo: Rule6
    Add the "id" to your (movies, rating) data frame given the original movie table
    Use: join
  */
  def rule6(df1: DataFrame, df2: DataFrame): DataFrame = {
    df2.join(
      df1.select(Constants.MovieIdColumn, Constants.TitleColumn),
      Seq(Constants.TitleColumn),
      Constants.InnerJoin
    )
  }

  /*
  todo: Rule7
    Filter the "genres count" df (given in Rule 4) by the "movies, rating" df (given in Rule 6)
    after multiplies each rating with its corresponding genres and sum the result for the same genres,
    where a single row is obtained.
    Obtained row should contains the weight of each genre according the rating give to each movie
    Final data frame should contains the "rating" column and the genres columns (Adventure, Animation, etc.)
    Use: filter, join, drop, sum
  */
  def rule7(df1: DataFrame, df2: DataFrame): DataFrame = {
    val IdsList = df2
      .select(col(Constants.MovieIdColumn))
      .collect()
      .map(row => row.getString(0))
      .toList
    val df = df1
      .filter(col(Constants.MovieIdColumn).isin(IdsList :_*))
      .join(
        df2.select(Constants.RatingColumn, Constants.MovieIdColumn),
        Seq(Constants.MovieIdColumn),
        Constants.InnerJoin
      )
      .drop(Constants.MovieIdColumn)

    df
      .select(df.columns.map(colName => {
        colName match {
          case Constants.RatingColumn => col(Constants.RatingColumn)
          case _ => (col(colName) * col(Constants.RatingColumn)).alias(colName)
        }
      }):_*)
      .select(df.columns.map(colName => sum(col(colName)).alias(colName)):_*)
  }

  /*
  todo: Rule8
    Obtain the top recommended movies for the dataframe generated in Rule 5.
    In order to make this, multiplies the "genres count" (obtained in Rule 4) and the "weight of each genre" obtained in Rule 7,
    after that, sum all genres columns and divide the result by the rating column, resulted column will be called "recommendation_value".
    Finally, filter the top 50 recommendations, the final data frame should contains the columns "title" and "recommendation"
    Use: Broadcast
  */
  def rule8(df1: DataFrame, df2: DataFrame, df3: DataFrame): DataFrame = {
    // def singleWeight(colName: String):Double = broadcast(df2).select(col(colName)).collect()(0).getDouble(0)
    val mapWeight = spark.sparkContext.broadcast(   // FASTEST !!!!!
      df2
        .collect()(0)
        .getValuesMap[Double](df2.schema.fieldNames)
    )
    def singleWeight(colName: String):Double = mapWeight.value(colName)

    val df = df1
      .select(df1.columns.map(colName => {
        colName match {
          case Constants.MovieIdColumn => col(Constants.MovieIdColumn)
          case _ => (df1.col(colName) * singleWeight(colName)).alias(colName)
        }
      }) :_*)
      .select(
        col(Constants.MovieIdColumn),
        df1
          .columns
          .filter(colName => colName != Constants.MovieIdColumn)
          .map(col)
          .reduce((col1,col2) => col1 + col2)
          .alias(Constants.SumColumn)
      )
      .select(
        col(Constants.MovieIdColumn),
        (col(Constants.SumColumn) / singleWeight(Constants.RatingColumn))
          .alias(Constants.RecommendationColumn)
      )
    df
      .join(df3,Seq(Constants.MovieIdColumn),Constants.InnerJoin)
      .drop(col(Constants.MovieIdColumn))
      .orderBy(col(Constants.RecommendationColumn).desc_nulls_first)
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

    // Due to some movies have the same name and only differs by the year (for example "sabrina"),
    // we do not use this rule
    // dataFlow.put(Constants.Rule2,rule2(dataFlow(Constants.Rule1)))

    dataFlow.put(Constants.Rule3,rule3(dataFlow(Constants.Rule1)))
    println(dataFlow(Constants.Rule3).count())
    dataFlow(Constants.Rule3).printSchema()

    dataFlow.put(Constants.Rule4,rule4(dataFlow(Constants.Rule3)))
    dataFlow(Constants.Rule4).show()

    dataFlow.put(Constants.Rule5,rule5())

    dataFlow.put(Constants.Rule6,rule6(dataFlow(Constants.Rule3), dataFlow(Constants.Rule5)))
    dataFlow(Constants.Rule6).show()

    dataFlow.put(Constants.Rule7,rule7(dataFlow(Constants.Rule4), dataFlow(Constants.Rule6)))
    dataFlow(Constants.Rule7).show()

    val t0 = System.nanoTime()
    dataFlow.put(Constants.Rule8,rule8(dataFlow(Constants.Rule4), dataFlow(Constants.Rule7), dataFlow(Constants.MovieLensMovieDf)))
    dataFlow(Constants.Rule8).show(30,false)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/10e8 + "s")

  }
}
