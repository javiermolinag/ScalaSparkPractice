import rdd.{ProcMovieLensRDD, ProcSpotifyRDD}
import utils.initSpark

object MainRDD extends initSpark{

  // val procRDD: ProcSpotifyRDD = new ProcSpotifyRDD(spark)
  // procRDD.runProcess()

  val procRDD: ProcMovieLensRDD = new ProcMovieLensRDD(spark)
  procRDD.runProcess()
}