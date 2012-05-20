package edu.ucsc.dbtune.viz;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;

/**
 * @author Ivo Jimenez
 */
public abstract class AbstractVisualizer implements Visualizer
{
    protected List<RecommendationStatistics> stats;

    /**
     * Sets the stats for this instance.
     *
     * @param stats The stats.
     */
    public void setStatistics(RecommendationStatistics... stats)
    {
        this.stats = new ArrayList<RecommendationStatistics>();

        for (RecommendationStatistics stat : stats)
            this.stats.add(stat);

        refresh();
    }

    /**
     * Sets the stats for this instance.
     *
     * @param stats The stats.
     */
    public void setStatistics(List<RecommendationStatistics> stats)
    {
        setStatistics(stats.toArray(new RecommendationStatistics[0]));
    }
}
