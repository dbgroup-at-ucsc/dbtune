package edu.ucsc.dbtune.viz;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;

import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

/**
 * Interface for plotters.
 * 
 * @author Ivo Jimenez
 */
public class TotalWorkPlotter extends AbstractVisualizer
{
    private XYChart appFrame;

    /**
     * Creates a total work plotter.
     */
    public TotalWorkPlotter()
    {
        XYSeriesCollection c = new XYSeriesCollection();
        appFrame = VizUtils.createXYChart("DBTune", c, "Performance", "Queries", "Total Work");
        frame = appFrame;
        stats = new ArrayList<RecommendationStatistics>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateContent()
    {
        plot(stats);
    }

    /**
     * @param recommendationStats
     *      list of statistics about recommendation phases
     */
    public void plot(RecommendationStatistics... recommendationStats)
    {
        XYSeriesCollection c = new XYSeriesCollection();

        for (RecommendationStatistics rs : recommendationStats) {
            XYSeries xyseries = new XYSeries(rs.getAlgorithmName());
            int i = 1;
            for (RecommendationStatistics.Entry e : rs)
                xyseries.add(i++, e.getTotalWork());
            c.addSeries(xyseries);
        }

        appFrame.updateDataSet(c);
    }

    /**
     * @param recommendationStats
     *      list of statistics about recommendation phases
     */
    public void plot(List<RecommendationStatistics> recommendationStats)
    {
        plot(recommendationStats.toArray(new RecommendationStatistics[0]));
    }
}
