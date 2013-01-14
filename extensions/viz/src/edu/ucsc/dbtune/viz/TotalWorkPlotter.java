package edu.ucsc.dbtune.viz;

import java.util.ArrayList;
import java.util.List;

import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;

/**
 * Interface for plotters.
 *
 * @author Ivo Jimenez
 */
public class TotalWorkPlotter extends SwingVisualizer
{
    private XYChart xyChart;
    static final long serialVersionUID = 0;

    /**
     * Creates a total work plotter.
     */
    public TotalWorkPlotter()
    {
        XYSeriesCollection c = new XYSeriesCollection();
        xyChart = VizUtils.createXYChart("Performance", c, "", "Queries", "Total Work");
        setContentPane(xyChart.getPanel());
        stats = new ArrayList<RecommendationStatistics>();
        setLocation(513, 375);
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

        xyChart.updateDataSet(c);
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
