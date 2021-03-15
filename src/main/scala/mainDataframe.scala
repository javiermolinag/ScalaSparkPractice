import dataframe.{ProcMovieLensDataFrame, ProcSpotifyDataFrame}
import utils.initSpark

object mainDataframe extends initSpark{
  // val procDataFrame: ProcSpotifyDataFrame = new ProcSpotifyDataFrame(spark)
  // procDataFrame.runProcess()

  val procDataFrame: ProcMovieLensDataFrame = new ProcMovieLensDataFrame(spark)
  // procDataFrame.runProcess()
  procDataFrame.checkRDDProcess()
}
