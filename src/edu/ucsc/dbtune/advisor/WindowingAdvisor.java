package edu.ucsc.dbtune.advisor;

import java.util.List;

import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * An advisor that processes statements through a windowing-based approach.
 *
 * @author Ivo Jimenez
 */
public interface WindowingAdvisor
{
    /**
     * Returns the last window.
     *
     * @return
     *      the window
     */
    List<SQLStatement> getWindow();
}
