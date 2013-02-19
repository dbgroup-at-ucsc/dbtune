package edu.ucsc.dbtune.viz;

import edu.ucsc.dbtune.advisor.WorkloadObserverAdvisor;
import edu.ucsc.dbtune.advisor.wfit.WFIT;

/**
 * Instantiates visualizations.
 *
 * @author Ivo Jimenez
 */
public final class VisualizationFactory
{
    /**
     * utility class.
     */
    private VisualizationFactory()
    {
    }

    /**
     * creates a new visualizer based on the advisor given.
     *
     * @param advisor
     *      the advisor for which a visualizer is being built
     * @return
     *      a visualizer
     * @throws Exception
     *      if no known visualizer can be instantiated for the given advisor
     */
    public static AdvisorVisualizer newVisualizer(WorkloadObserverAdvisor advisor) throws Exception
    {
        AdvisorVisualizer viz = null;

        if (advisor instanceof WFIT) {
            WFIT wfitAdvisor = (WFIT) advisor;

            TabbedSwingVisualizer tViz = new TabbedSwingVisualizer(advisor);

            WorkloadTableWithWindow workloadTable = new WorkloadTableWithWindow(advisor);
            IndexSetTable currentRec = new IndexSetTable(advisor);
            WFITRecommendationRankTable recRank = new WFITRecommendationRankTable(wfitAdvisor);
            TotalWorkPlotter twPlotter = new TotalWorkPlotter(advisor);
            VoteableCandidateSetPartitionTable partitionTable =
                new VoteableCandidateSetPartitionTable(advisor);

            tViz.add("Workload", workloadTable);
            tViz.add("Current Recommendation", currentRec);
            tViz.add("Recommendation Rank", recRank);
            tViz.add("Candidate Partitioning", partitionTable);
            tViz.add("Total Work", twPlotter);

            viz = tViz;
        } else {
            throw new Exception("Unknown advisor type " + advisor);
        }

        return viz;
    }
}
