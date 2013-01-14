package edu.ucsc.dbtune.viz;

import java.util.ArrayList;
import java.util.List;

import javax.swing.JFrame;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;
import edu.ucsc.dbtune.workload.Workload;

/**
 * @author Ivo Jimenez
 */
public abstract class SwingVisualizer extends JFrame implements AdvisorVisualizer
{
    static final long serialVersionUID = 0;

    protected List<RecommendationStatistics> stats;

    protected Workload wl;

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
        dispose();
        setVisible(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void showit()
    {
        setAlwaysOnTop(true);
        setAlwaysOnTop(false);
        setVisible(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void refresh()
    {
        updateContent();

        invalidate();
        validate();
        repaint();
    }

    @Override
    public void setWorkload(Workload wl)
    {
        this.wl = wl;
    }

    /**
     * updates the content.
     */
    public abstract void updateContent();
}
