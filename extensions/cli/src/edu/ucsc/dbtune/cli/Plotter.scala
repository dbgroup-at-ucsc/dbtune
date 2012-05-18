package edu.ucsc.dbtune.cli

import java.util.Set

import edu.ucsc.dbtune.advisor.Advisor
import edu.ucsc.dbtune.advisor.RecommendationStatistics
import edu.ucsc.dbtune.metadata.Index
import edu.ucsc.dbtune.viz.IndexSetPartitionTable
import edu.ucsc.dbtune.viz.TotalWorkPlotter
import edu.ucsc.dbtune.viz.WFITStatisticsTable
import edu.ucsc.dbtune.viz.WFITIndexSetFeedbackTable

/** CLI interface to the {@code edu.ucsc.dbutne.viz} package
 */
object Plotter
{
  val twPlotter = new TotalWorkPlotter
  val partitionTable = new IndexSetPartitionTable
  val wfitTable = new WFITStatisticsTable
  val feedbackTable = new WFITIndexSetFeedbackTable

  /** Plots the total work for the given workload and list of statistics. If the plot hadn't been 
   * registered in the workload, it does so.
   *
   * @param wl
   *    workload the given set of statistics corresponds to
   * @param stats
   *    statistics from which the total work is obtained
   */
  def plotTotalWork(wl: WorkloadStream, stats: RecommendationStatistics*) = {
    if (!wl.isRegistered(twPlotter)) {
      twPlotter.setStatistics(stats.toList : _*)
      wl.register(twPlotter)
    }
    twPlotter.refresh
  }

  /** Displays wfit-specific table. If the table hadn't been registered in the workload, it does so.
   *
   * @param wl
   *    workload the given set of statistics corresponds to
   * @param stats
   *    statistics from which the candidate set partitioning is obtained
   */
  def showWFITTable(wl: WorkloadStream, stats:RecommendationStatistics*) = {
    if (!wl.isRegistered(wfitTable)) {
      wfitTable.setStatistics(stats.toList : _*)
      wl.register(wfitTable)
    }
    wfitTable.refresh
  }

  /** Displays the partitioning of the candidate set that a given algorithm (through its 
   * recommendation statistics object) does. If the table hadn't been registered in the workload, it 
   * does so.
   *
   * @param wl
   *    workload the given set of statistics corresponds to
   * @param stats
   *    statistics from which the candidate set partitioning is obtained
   */
  def showPartitionTable(wl: WorkloadStream, stats:RecommendationStatistics*) = {
    if (!wl.isRegistered(partitionTable)) {
      partitionTable.setStatistics(stats.toList : _*)
      wl.register(partitionTable)
    }
    partitionTable.refresh
  }

  /** Displays the feedback table. If the table hadn't been registered in the workload, it does so.
   *
   * @param wl
   *    workload the given set of statistics corresponds to
   * @param stats
   *    statistics from which the candidate set partitioning is obtained
   */
  def showFeedbackTable(wl: WorkloadStream, stats:RecommendationStatistics*) = {
    if (!wl.isRegistered(feedbackTable)) {
      feedbackTable.setStatistics(stats.toList : _*)
      wl.register(feedbackTable)
    }
    feedbackTable.refresh
  }
}
