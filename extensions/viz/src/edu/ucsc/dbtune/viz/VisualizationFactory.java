package edu.ucsc.dbtune.viz;

import edu.ucsc.dbtune.advisor.Advisor;
import edu.ucsc.dbtune.advisor.wfit.WFIT;

/**
 * Instantiates visualizations.
 *
 * @author Ivo Jimenez
 */
public class VisualizationFactory
{
    /**
     */
    public static AdvisorVisualizer newVisualizer(Advisor advisor) throws Exception
    {
        AdvisorVisualizer viz = null;

        if (advisor instanceof WFIT) {
            TabbedSwingVisualizer tViz = new TabbedSwingVisualizer();

            WorkloadTable workloadTable = new WorkloadTable();
            TotalWorkPlotter twPlotter = new TotalWorkPlotter();
            IndexSetPartitionTable partitionTable = new IndexSetPartitionTable();

            tViz.add(workloadTable);
            tViz.add(twPlotter);
            tViz.add(partitionTable);

            viz = tViz;
        } else {
            throw new Exception("Unknown advisor type " + advisor);
        }

        return viz;
    }
}
