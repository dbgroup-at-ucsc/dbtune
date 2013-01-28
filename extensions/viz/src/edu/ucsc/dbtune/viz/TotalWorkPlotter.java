package edu.ucsc.dbtune.viz;

import java.util.List;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;
import edu.ucsc.dbtune.advisor.WorkloadObserverAdvisor;

import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

/**
 * Interface for plotters.
 *
 * @author Ivo Jimenez
 */
public class TotalWorkPlotter extends SwingVisualizer
{
    static final long serialVersionUID = 0;
    private XYChart xyChart;

    /**
     * Creates a total work plotter.
     *
     * @param advisor
     *      the advisor being observed
     */
    public TotalWorkPlotter(WorkloadObserverAdvisor advisor)
    {
        super(advisor);
        XYSeriesCollection c = new XYSeriesCollection();
        xyChart = VizUtils.createXYChart("Performance", c, "", "Queries", "Total Work");
        setContentPane(xyChart.getPanel());
        setLocation(513, 375);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateContent() throws Exception
    {
        plot(advisor.getRecommendationStatistics());
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
