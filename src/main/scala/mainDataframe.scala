import dataframe.ProcSpotifyDataFrame
import utils.initSpark

object mainDataframe extends initSpark{
  val procDataFrame: ProcSpotifyDataFrame = new ProcSpotifyDataFrame(spark)
  procDataFrame.runProcess()
}
