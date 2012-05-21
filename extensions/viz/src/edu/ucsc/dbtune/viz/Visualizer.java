package edu.ucsc.dbtune.viz;

import edu.ucsc.dbtune.workload.Workload;

/**
 * @author Ivo Jimenez
 */
public interface Visualizer
{
    /**
     */
    void refresh();

    /**
     */
    void show();

    /**
     */
    void hide();

    /**
     * @param w
     *      the workload this visualizer is watching
     */
    void setWorkload(Workload w);
}
