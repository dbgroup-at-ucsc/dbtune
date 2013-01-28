package edu.ucsc.dbtune.workload;

/**
 * Reader of SQLStatement objects.
 *
 * @author Ivo Jimenez
 */
public interface WorkloadReader extends Iterable<SQLStatement>
{
    /**
     * The workload that this reader is reading.
     *
     * @return
     *      name of the workload that it's being read.
     */
    Workload getWorkload();
}
