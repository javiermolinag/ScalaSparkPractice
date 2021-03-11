import rdd.ProcSpotifyRDD
import utils.initSpark

object MainRDD extends initSpark{

  val procRDD: ProcSpotifyRDD = new ProcSpotifyRDD(spark)
  procRDD.runProcess()

}