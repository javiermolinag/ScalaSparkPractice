package dataframe

import common.Schemas.{movieMovieLensSchema, ratingMovieLensSchema, tagMovieLensSchema}
import constants.Constants
import org.apache.spark.sql.functions.{avg, col, collect_set, count, explode, lit, lower, ltrim, regexp_extract, regexp_replace, rtrim, split, substring_index, sum}
import org.apache.spark.sql.types.{LongType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable

class ProcMovieLensDataFrame(spark: SparkSession) {

  private val readSpark = new ReadProcess(spark)
  private val writeSpark = new WriteProcess

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
    val newCol: Column =
      regexp_replace(
        regexp_extract(
          col(Constants.TitleColumn),
          "[(][0-9]{4}[)] *$",
          Constants.ZeroNumber),
        "[() ]",
        "")
      .alias(Constants.YearColumn)
    df.select(df.columns.map(col) :+ newCol :_*)
  }

  /*
  todo: Rule2
    Delete the year expression in the "title" column
    Use: substring_index, rtrim, ltrim
  */
  def rule2(df: DataFrame): DataFrame = {
    val newCol: Column = substring_index(col(Constants.TitleColumn),"(",Constants.OneNumber)
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
      .map(row => row.getString(Constants.ZeroNumber))
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
    Use: Broadcast, groupBy, agg, filter, map, reduce, join, na, drop
  */
  def rule8(df1: DataFrame, df2: DataFrame, df3: DataFrame, df4: DataFrame): DataFrame = {
    // https://www.kaggle.com/rabiaakt/recommender-systems
    // def singleWeight(colName: String):Double = broadcast(df2).select(col(colName)).collect()(0).getDouble(0)
    val mapWeight = spark.sparkContext.broadcast(   // FASTEST !!!!!
      df2
        .collect()(Constants.ZeroNumber)
        .getValuesMap[Double](df2.schema.fieldNames)
    )
    def singleWeight(colName: String):Double = mapWeight.value(colName)

    val ratingDf = df4
      .groupBy(col(Constants.MovieIdColumn))
      .agg(
        avg(col(Constants.RatingColumn)).as(Constants.AvgRatingColumn)
      )

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
      .join(ratingDf,Seq(Constants.MovieIdColumn),Constants.OuterJoin)
      .na
      .fill(Map(
        Constants.AvgRatingColumn -> Constants.ZeroNumber
      ))
      .drop(col(Constants.MovieIdColumn))
      .orderBy(
        col(Constants.RecommendationColumn).desc_nulls_first,
        col(Constants.AvgRatingColumn).desc_nulls_first
      )
  }

  /*
  todo: Rule n1
    how many movies have seen each user?
    Use: groupBy, agg, orderBy
  */
  def ruleN1(df: DataFrame): DataFrame = {
    df
      .groupBy(Constants.UserIdColumn)
      .agg(
        count(col(Constants.RatingColumn)).as(Constants.CountMovies)
      )
      .orderBy(col(Constants.CountMovies).desc_nulls_first)
  }

  /*
  todo: Rule n2
    show the avg "rating" (given by all users) of each movie
    Use: groupBy, agg, join, na, orderBy
  */
  def ruleN2(df1: DataFrame,df2: DataFrame): DataFrame = {
    df1
      .groupBy(Constants.MovieIdColumn)
      .agg(
        avg(col(Constants.RatingColumn)).as(Constants.AvgRatingColumn),
        count(col(Constants.RatingColumn)).as(Constants.CountUsers)
      )
      .join(df2, Seq(Constants.MovieIdColumn),Constants.OuterJoin)
      .na
      .fill(Map(
        Constants.AvgRatingColumn -> Constants.ZeroNumber
      ))
      .orderBy(
        col(Constants.AvgRatingColumn).desc_nulls_first,
        col(Constants.CountUsers).desc_nulls_first
      )
  }

  /*
  todo: Rule n3
    Show all different tags (array of strings) for each movie in a column called "tag"
    Use: groupBy, agg, collect_set, lower, join
  */
  def ruleN3(df1: DataFrame, df2: DataFrame): DataFrame = {
    df1
      .groupBy(Constants.MovieIdColumn)
      .agg(
        collect_set(lower(col(Constants.TagColumn))).as(Constants.TagColumn)
      )
      .join(df2,Seq(Constants.MovieIdColumn),Constants.OuterJoin)
  }

  /*
  todo: Join and save a single CSV
    Join the tag.csv, movie.csv and rating.csv and save it as movieLens.csv
  */
  def JoinAndWriteAll(): Unit = {
    val moviesDf: DataFrame = dataFlow(Constants.MovieLensMovieDf)
    val ratingDf: DataFrame = dataFlow(Constants.MovieLensRatingDf)
    val tagDf: DataFrame = dataFlow(Constants.MovieLensTagDf)
    val renameTimestamp = (df: DataFrame, newColName: String) => {
      df.select(df.columns.map(colName => {
        colName match {
          case Constants.TimestampColumn => col(Constants.TimestampColumn).alias(newColName)
          case _ => col(colName)
        }
      }):_*)
    }

    writeSpark.writeCSV(
      renameTimestamp(ratingDf,Constants.TimestampRatingColumn)
        .join(
          renameTimestamp(tagDf,Constants.TimestampTagColumn),
          Seq(Constants.UserIdColumn, Constants.MovieIdColumn),
          Constants.OuterJoin)
        .join(
          moviesDf,
          Seq(Constants.MovieIdColumn),
          Constants.OuterJoin)
      .select(
        Constants.UserIdColumn,
        Constants.MovieIdColumn,
        Constants.TitleColumn,
        Constants.GenresColumn,
        Constants.RatingColumn,
        Constants.TimestampRatingColumn,
        Constants.TagColumn,
        Constants.TimestampTagColumn
      ),
      Constants.MovieLensString
    )
  }

  /*
  todo: Check the RDD exercises
  */
  def checkRDDProcess(): Unit = {
    def moviesByYearCheck(df: DataFrame): Unit = {
      df
        .groupBy(Constants.YearColumn)
        .count()
        .orderBy(col(Constants.YearColumn).desc_nulls_first)
        // .show(1000)
    }
    def NumberOfMovies(df: DataFrame): Unit = {
      println(
        df
          .dropDuplicates(Seq(Constants.MovieIdColumn))
          .count()
      )
    }
    def bestMovies(df: DataFrame): Unit = {
      df
        .groupBy(col(Constants.MovieIdColumn))
        .agg(
          avg(col(Constants.RatingColumn))
            .alias(Constants.AvgRatingColumn),
          count(col(Constants.RatingColumn))
            .alias(Constants.CountMovies)
        )
        .filter(col(Constants.AvgRatingColumn) > Constants.FourDotFIveNumber)
        .sort(col(Constants.MovieIdColumn).cast(LongType).desc_nulls_first)
        //.show(200,Constants.FalseBoolean)
    }
    val explodeGenres: DataFrame => DataFrame = {
      df =>
        val newCol: Column = explode(split(col(Constants.GenresColumn),"\\|"))
          .alias(Constants.GenresColumn)
        df.select(df.columns.map( colName => {
          colName match {
            case Constants.GenresColumn => newCol
            case _ => col(colName)
          }
        }) :_*)
    }
    def bestMoviesByGenre(df1: DataFrame, df2: DataFrame): Unit = {
      df1
        .join(df2,Seq(Constants.MovieIdColumn))
        .transform(explodeGenres)
        .groupBy(col(Constants.MovieIdColumn),col(Constants.GenresColumn))
        .agg(
          avg(col(Constants.RatingColumn))
            .alias(Constants.AvgRatingColumn),
          count(col(Constants.RatingColumn))
            .alias(Constants.CountMovies)
        )
        .filter(col(Constants.AvgRatingColumn) > Constants.FourDotFIveNumber)
        .sort(col(Constants.MovieIdColumn).cast(LongType).desc_nulls_first)
        .show(200,Constants.FalseBoolean)
    }

    // Number of movies
    NumberOfMovies(dataFlow(Constants.MovieLensMovieDf))
    // Movies by year
    moviesByYearCheck(rule1(dataFlow(Constants.MovieLensMovieDf)))
    // best movies
    bestMovies(dataFlow(Constants.MovieLensRatingDf))
    // best movies by gene
    bestMoviesByGenre(dataFlow(Constants.MovieLensRatingDf),dataFlow(Constants.MovieLensMovieDf))
  }

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
    // println(dataFlow(Constants.Rule3).count())
    // dataFlow(Constants.Rule3).printSchema()
    dataFlow.put(Constants.Rule4,rule4(dataFlow(Constants.Rule3)))
    // dataFlow(Constants.Rule4).show()
    dataFlow.put(Constants.Rule5,rule5())
    dataFlow.put(Constants.Rule6,rule6(dataFlow(Constants.Rule3), dataFlow(Constants.Rule5)))
    // dataFlow(Constants.Rule6).show()
    dataFlow.put(Constants.Rule7,rule7(dataFlow(Constants.Rule4), dataFlow(Constants.Rule6)))
    // dataFlow(Constants.Rule7).show()
    val t0 = System.nanoTime()
    dataFlow.put(Constants.Rule8,rule8(dataFlow(Constants.Rule4), dataFlow(Constants.Rule7), dataFlow(Constants.MovieLensMovieDf), dataFlow(Constants.MovieLensRatingDf)))
    dataFlow(Constants.Rule8).show(30,false)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/10e8 + "s")

    dataFlow.put(Constants.RuleN1,ruleN1(dataFlow(Constants.MovieLensRatingDf)))
    dataFlow(Constants.RuleN1).show()
    dataFlow.put(Constants.RuleN2,ruleN2(dataFlow(Constants.MovieLensRatingDf),dataFlow(Constants.MovieLensMovieDf)))
    dataFlow(Constants.RuleN2).show()
    dataFlow.put(Constants.RuleN3,ruleN3(dataFlow(Constants.MovieLensTagDf),dataFlow(Constants.MovieLensMovieDf)))
    dataFlow(Constants.RuleN3).printSchema()
    dataFlow(Constants.RuleN3).show(false)

    JoinAndWriteAll()
  }
}
