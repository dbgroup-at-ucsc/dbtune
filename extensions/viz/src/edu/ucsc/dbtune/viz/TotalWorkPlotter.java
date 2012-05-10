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
public class TotalWorkPlotter implements Plotter
{
    private XYChart appFrame;

    /**
     * Creates a total work plotter.
     */
    public TotalWorkPlotter()
    {
        XYSeriesCollection c = new XYSeriesCollection();
        appFrame = VizUtils.createXYChart("DBTune", c, "Performance", "Queries", "Total Work");
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void plot(RecommendationStatistics recommendationStats)
    {
        List<RecommendationStatistics> rsList = new ArrayList<RecommendationStatistics>();

        rsList.add(recommendationStats);

        plot(rsList);
    }

    /**
     * {@inheritDoc}
     */
    @Override
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
        appFrame.setVisible(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void plot(List<RecommendationStatistics> recommendationStats)
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
        appFrame.setVisible(true);
    }
}
