package edu.ucsc.dbtune.viz;

import javax.swing.JFrame;

import com.google.common.eventbus.Subscribe;

import edu.ucsc.dbtune.advisor.WorkloadObserverAdvisor;

import edu.ucsc.dbtune.util.EventBusFactory;

import edu.ucsc.dbtune.workload.Workload;

/**
 * @author Ivo Jimenez
 */
public abstract class SwingVisualizer extends JFrame implements AdvisorVisualizer
{
    static final long serialVersionUID = 0;

    protected WorkloadObserverAdvisor advisor;
    protected Workload workload;

    /**
     * @param advisor
     *      the advisor being observed by the visualizer
     */
    public SwingVisualizer(WorkloadObserverAdvisor advisor)
    {
        this.advisor = advisor;
        this.workload = advisor.getWorkload();

        EventBusFactory.getEventBusInstance().register(this);
    }

    /**
     * Sets the stats for this instance.
     *
     * @param eventId
     *      a string identifying the event that this visualizer is waiting for
     * @throws Exception
     *      if an error occurs while refreshing the content
     */
    @Subscribe
    public void handleUpdateToAdvisor(String eventId) throws Exception
    {
        if (eventId.equals(advisor.hashCode() + "_" + workload.hashCode()))
            refreshit();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void hideit()
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
    public final void refreshit() throws Exception
    {
        updateContent();

        invalidate();
        validate();
        repaint();
    }

    /**
     * updates the content.
     *
     * @throws Exception
     *      if an error occurs while updating the content
     */
    public abstract void updateContent() throws Exception;
}
