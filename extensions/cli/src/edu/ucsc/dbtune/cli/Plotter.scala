package edu.ucsc.dbtune.cli

import java.util.Set

import edu.ucsc.dbtune.advisor.Advisor
import edu.ucsc.dbtune.advisor.RecommendationStatistics
import edu.ucsc.dbtune.metadata.Index
import edu.ucsc.dbtune.viz.IndexSetPartitionTable
import edu.ucsc.dbtune.viz.TotalWorkPlotter

object Plotter
{
  val twPlotter = new TotalWorkPlotter
  val partitionTable = new IndexSetPartitionTable

  /** Plots total work
    */
  def plotTotalWork(advisor:Advisor) = {
    twPlotter.plot(advisor.getRecommendationStatistics)
  }

  /** Plots total work
    */
  def plotTotalWork(stats:RecommendationStatistics) = {
    twPlotter.plot(stats)
  }

  /** Plots total work
    */
  def plotTotalWork(stats:RecommendationStatistics*) = {
    twPlotter.plot(stats.toList : _*)
  }

  /** Displays the partition table
    */
  def showPartitionTable(partition:Set[Set[Index]]) = {
    partitionTable.setPartition(partition)
  }
}
