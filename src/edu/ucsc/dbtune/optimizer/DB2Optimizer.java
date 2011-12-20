package edu.ucsc.dbtune.optimizer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Interface to the DB2 optimizer.
 *
 * @author Karl Schnaitter
 * @author Huascar Sanchez
 * @author Ivo Jimenez
 */
public class DB2Optimizer extends AbstractOptimizer
{
    /**
     * Creates a DB2 optimizer with the given information.
     *
     * @param connection
     *     a live connection to DB2
     */
    public DB2Optimizer(Connection connection)
    {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExplainedSQLStatement explain(SQLStatement sql, Set<Index> indexes)
        throws SQLException
    {
        throw new SQLException("not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> recommendIndexes(SQLStatement sql)
        throws SQLException
    {
        throw new SQLException("Not implemented yet");
    }

}
