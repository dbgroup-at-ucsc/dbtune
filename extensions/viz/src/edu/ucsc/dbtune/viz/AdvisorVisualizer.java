package edu.ucsc.dbtune.viz;

import edu.ucsc.dbtune.workload.Workload;

/**
 * A visualizer of recommendation algorithms.
 *
 * @author Ivo Jimenez
 */
public interface AdvisorVisualizer
{
    /**
     */
    void refresh();

    /**
     */
    void showit();

    /**
     */
    void hide();

    /**
     */
    void setWorkload(Workload wl);
}
