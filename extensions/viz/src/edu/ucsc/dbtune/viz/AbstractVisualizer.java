package edu.ucsc.dbtune.viz;

import java.util.ArrayList;
import java.util.List;

import javax.swing.JFrame;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;

import edu.ucsc.dbtune.workload.Workload;

/**
 * @author Ivo Jimenez
 */
public abstract class AbstractVisualizer implements Visualizer
{
    protected List<RecommendationStatistics> stats;
    protected JFrame frame;

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
    /**
     * {@inheritDoc}
     */
    @Override
    public final void hide()
    {
        frame.dispose();
        frame.setVisible(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setWorkload(Workload w)
    {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void show()
    {
        frame.setAlwaysOnTop(true);
        frame.setAlwaysOnTop(false);
        frame.setVisible(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void refresh()
    {
        updateContent();

        frame.invalidate();
        frame.validate();
        frame.repaint();
    }

    /**
     * updates the content.
     */
    public abstract void updateContent();
}
