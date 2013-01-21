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
    protected String name;
    protected List<SQLStatement> sqls;

    /**
     * {@inheritDoc}
     */
    public String getName()
    {
        return name;
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
}
