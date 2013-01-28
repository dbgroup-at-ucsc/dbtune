package edu.ucsc.dbtune.viz;

/**
 * A visualizer of recommendation algorithms.
 *
 * @author Ivo Jimenez
 */
public interface AdvisorVisualizer
{
    /**
     * @throws Exception
     *      if an error occurs while refreshing the visualizer
     */
    void refreshit() throws Exception;

    /**
     */
    void showit();

    /**
     */
    void hideit();
}
