import dataframe.ProcDataFrame
import utils.initSpark

object mainDataframe extends initSpark{
  val procDataFrame: ProcDataFrame = new ProcDataFrame(spark)
  procDataFrame.runProcess()
}
