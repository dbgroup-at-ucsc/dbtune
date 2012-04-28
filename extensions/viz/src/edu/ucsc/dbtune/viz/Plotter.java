package edu.ucsc.dbtune.viz;

import java.util.List;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;

/**
 * Interface for plotters.
 * 
 * @author Ivo Jimenez
 */
public interface Plotter
{
    /**
     * @param recommendationStats
     *      statistics about a recommendation phase from several algorithms
     */
    void plot(List<RecommendationStatistics> recommendationStats);

    /**
     * @param recommendationStats
     *      statistics about a recommendation phase
     */
    void plot(RecommendationStatistics recommendationStats);
}
