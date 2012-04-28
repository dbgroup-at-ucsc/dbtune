package edu.ucsc.dbtune.cli

import edu.ucsc.dbtune.advisor.Advisor
import edu.ucsc.dbtune.viz.TotalWorkPlotter

object Plotter
{
  val twPlotter = new TotalWorkPlotter

  /** Plots
    */
  def plotTotalWork(advisor:Advisor) = {
    twPlotter.plot(advisor.getRecommendationStatistics)
  }
}
