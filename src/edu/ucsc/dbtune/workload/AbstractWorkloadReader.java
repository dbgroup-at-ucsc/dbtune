package edu.ucsc.dbtune.workload;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Reader for SQLStatement object.
 *
 * @author Ivo Jimenez
 */
public abstract class AbstractWorkloadReader implements WorkloadReader
{
    protected List<SQLStatement> sqls;

    protected Workload workload;

    /**
     * Creates an abstract reader that is associated to the given workload object. Every child of 
     * this abstract class has to provide the workload at construction time, since this is the 
     * object that it's associated to every new {@link SQLStatement} instantiated by the reader.
     *
     * @param workload
     *      workload that the reader is reading
     */
    protected AbstractWorkloadReader(Workload workload)
    {
        this.workload = workload;
    }

    /**
     * List of statements visible to members of the package only.
     *
     * @return
     *      a copy of the internal list.
     */
    List<SQLStatement> getStatements()
    {
        return new ArrayList<SQLStatement>(sqls);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<SQLStatement> iterator()
    {
        return sqls.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Workload getWorkload()
    {
        return workload;
    }
}
