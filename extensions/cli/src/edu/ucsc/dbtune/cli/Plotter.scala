package edu.ucsc.dbtune.cli

import java.util.Set

import edu.ucsc.dbtune.advisor.Advisor
import edu.ucsc.dbtune.advisor.RecommendationStatistics
import edu.ucsc.dbtune.metadata.Index
import edu.ucsc.dbtune.viz.AbstractVisualizer
import edu.ucsc.dbtune.viz.IndexSetPartitionTable
import edu.ucsc.dbtune.viz.TotalWorkPlotter
import edu.ucsc.dbtune.viz.WFITStatisticsTable
import edu.ucsc.dbtune.viz.WFITIndexSetFeedbackTable

/** CLI interface to the {@code edu.ucsc.dbutne.viz} package
 */
object Plotter
{
  var twPlotter = new TotalWorkPlotter
  var partitionTable = new IndexSetPartitionTable
  var wfitTable = new WFITStatisticsTable
  var feedbackTable = new WFITIndexSetFeedbackTable

  def resetUI = {
    twPlotter.hide
    partitionTable.hide
    wfitTable.hide
    feedbackTable.hide
    twPlotter = new TotalWorkPlotter
    partitionTable = new IndexSetPartitionTable
    wfitTable = new WFITStatisticsTable
    feedbackTable = new WFITIndexSetFeedbackTable
  }

  /** Plots the total work for the given workload and list of statistics. If the plot hadn't been 
   * registered in the workload, it does so.
   *
   * @param wl
   *    workload the given set of statistics corresponds to
   * @param stats
   *    statistics from which the total work is obtained
   */
  def plotTotalWork(wl: WorkloadStream, stats: RecommendationStatistics*) =
    show(twPlotter, wl, stats.toList : _*)

  /** Displays wfit-specific table. If the table hadn't been registered in the workload, it does so.
   *
   * @param wl
   *    workload the given set of statistics corresponds to
   * @param stats
   *    statistics from which the candidate set partitioning is obtained
   */
  def showWFITTable(wl: WorkloadStream, stats: RecommendationStatistics) =
    show(wfitTable, wl, stats)

  /** Displays the partitioning of the candidate set that a given algorithm (through its 
   * recommendation statistics object) does. If the table hadn't been registered in the workload, it 
   * does so.
   *
   * @param wl
   *    workload the given set of statistics corresponds to
   * @param stats
   *    statistics from which the candidate set partitioning is obtained
   */
  def showPartitionTable(wl: WorkloadStream, stats: RecommendationStatistics) =
    show(partitionTable, wl, stats)

  /** Displays the feedback table. If the table hadn't been registered in the workload, it does so.
   *
   * @param wl
   *    workload the given set of statistics corresponds to
   * @param stats
   *    statistics from which the candidate set partitioning is obtained
   */
  def showFeedbackTable(wl: WorkloadStream, stats: RecommendationStatistics) =
    show(feedbackTable, wl, stats)

  def show(vis: AbstractVisualizer, wl: WorkloadStream, stats: RecommendationStatistics*) = {
    wl.register(vis)
    vis.setStatistics(stats.toList : _*)
    vis.show
  }
}
