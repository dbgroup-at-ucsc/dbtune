package edu.ucsc.dbtune.workload;

/**
 * Reader of SQLStatement objects
 *
 * @author Ivo Jimenez
 */
public interface WorkloadReader extends Iterable<SQLStatement>
{
    /**
     * Gets the name of the workload, if given during construction.
     *
     * @return The name.
     */
    String getName();
}
