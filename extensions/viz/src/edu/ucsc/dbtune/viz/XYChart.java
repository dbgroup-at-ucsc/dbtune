package edu.ucsc.dbtune.viz;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;

import javax.swing.JFrame;
import javax.swing.JPanel;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.StandardChartTheme;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYDataset;

/**
 * @author Ivo Jimenez
 */
public class XYChart extends JFrame
{
    /** stuff. */
    public static final long serialVersionUID = 0;
    private JFreeChart chart;

    /**
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
     */
    public XYChart(
            String windowTitle,
            XYDataset xyDataSet,
            String chartTitle,
            String xLabel,
            String yLabel)
    {
        super(windowTitle);
        JPanel jpanel = createPanel(xyDataSet, chartTitle, xLabel, yLabel);
        jpanel.setPreferredSize(new Dimension(511, 350));
        setContentPane(jpanel);
        setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        chart = ((ChartPanel) jpanel).getChart();
    }

    /**
     * Creates a chart panel containing the given dataset.
     *
     * @param xyDataSet
     *      dataset to be included in the panel
     * @param chartTitle
     *      the title of the chart
     * @param xLabel
     *      label to display for the X-axis
     * @param yLabel
     *      label to display for the y-axis
     * @return
     *      the panel
     */
    public static JPanel createPanel(
            XYDataset xyDataSet,
            String chartTitle,
            String xLabel,
            String yLabel)
    {
        JFreeChart jfreechart = createChart(xyDataSet, chartTitle, xLabel, yLabel);
        ChartPanel chartpanel = new ChartPanel(jfreechart);
        chartpanel.setMouseWheelEnabled(true);
        return chartpanel;
    }

    /**
     * Creates a chart panel containing the given dataset.
     *
     * @param xyDataSet
     *      dataset to be included in the panel
     */
    public void updateDataSet(XYDataset xyDataSet)
    {
        chart.getXYPlot().setDataset(xyDataSet);
    }

    /**
     * Creates a chart containing the given dataset.
     *
     * @param xyDataSet
     *      dataset to be plotted in the chart
     * @param chartTitle
     *      the title of the chart
     * @param xLabel
     *      label to display for the X-axis
     * @param yLabel
     *      label to display for the y-axis
     * @return
     *      the chart
     */
    private static JFreeChart createChart(
            XYDataset xyDataSet,
            String chartTitle,
            String xLabel,
            String yLabel)
    {
        JFreeChart jfreechart =
            ChartFactory.createXYLineChart(
                chartTitle, xLabel, yLabel, xyDataSet, PlotOrientation.VERTICAL, true, true, false);

        // font
        StandardChartTheme chartTheme = (StandardChartTheme) StandardChartTheme.createJFreeTheme();
        chartTheme.setExtraLargeFont(new Font("Droid Sans", Font.PLAIN, 32));
        chartTheme.setLargeFont(new Font("Droid Sans", Font.PLAIN, 26));
        chartTheme.setRegularFont(new Font("Droid Sans", Font.BOLD, 20));
        chartTheme.setSmallFont(new Font("Droid Sans", Font.PLAIN, 8));

        chartTheme.apply(jfreechart);

        // color
        XYPlot xyplot = jfreechart.getXYPlot();

        xyplot.setForegroundAlpha(0.65f);
        xyplot.setRangeGridlinePaint(Color.black);
        xyplot.setDomainGridlinePaint(Color.black);
        xyplot.setBackgroundPaint(Color.decode("#FFFFFF"));

        XYLineAndShapeRenderer xyrenderer = (XYLineAndShapeRenderer) xyplot.getRenderer();

        //((NumberAxis) xyplot.getRangeAxis()).setRange(0.66,1.04);
        ((NumberAxis) xyplot.getRangeAxis()).setAutoRangeIncludesZero(false);
        ((NumberAxis) xyplot.getDomainAxis()).
            setStandardTickUnits(NumberAxis.createIntegerTickUnits());

        xyrenderer.setSeriesPaint(0, Color.decode("#9E9E9E"));
        xyrenderer.setSeriesPaint(1, Color.decode("#196DFF"));
        xyrenderer.setSeriesPaint(2, Color.decode("#C11515"));
        xyrenderer.setSeriesPaint(3, Color.decode("#FFAB19"));
        xyrenderer.setBaseShapesVisible(true);
        xyrenderer.setSeriesFillPaint(0, Color.decode("#9E9E9E"));
        xyrenderer.setSeriesFillPaint(1, Color.decode("#196DFF"));
        xyrenderer.setSeriesFillPaint(2, Color.decode("#FFAB19"));
        xyrenderer.setSeriesFillPaint(3, Color.decode("#C11515"));
        xyrenderer.setUseFillPaint(true);
        xyrenderer.setSeriesStroke(0, new BasicStroke(4f));
        xyrenderer.setSeriesStroke(1, new BasicStroke(4f));
        xyrenderer.setSeriesStroke(2, new BasicStroke(4f));
        xyrenderer.setSeriesStroke(3, new BasicStroke(4f));

        return jfreechart;
    }
}
