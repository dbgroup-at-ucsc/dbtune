package edu.ucsc.dbtune.viz;

import org.jfree.data.xy.XYDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

/**
 * @author Ivo Jimenez
 */
public final class VizUtils
{
    /**
     * utility class.
     */
    private VizUtils()
    {
    }

    /**
     * Creates an XY chart with the given XY dataset.
     *
     * @param windowTitle
     *      the title of the window
     * @param xyDataSet
     *      dataset that is plotted
     * @param chartTitle
     *      the title of the chart
     * @param xLabel
     *      label to display for the X-axis
     * @param yLabel
     *      label to display for the y-axis
     * @return
     *      the application frame containing the chart
     */
    public static ApplicationFrame createXYChart(
            String windowTitle,
            XYDataset xyDataSet,
            String chartTitle,
            String xLabel,
            String yLabel)
    {
        XYChart chart = new XYChart(windowTitle, xyDataSet, chartTitle, xLabel, yLabel);
        chart.pack();
        RefineryUtilities.centerFrameOnScreen(chart);
        chart.setVisible(true);
        return chart;
    }
}
