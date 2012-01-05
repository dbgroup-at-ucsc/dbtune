package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * A convenience class for a no-op implementation of the {@link PreparedSQLStatement} interface.
 * The {@link PreparedSQLStatement#explain(Configuration)} method simply calls {@link 
 * Optimizer#explain(SQLStatement, Configuration)}. 
 * 
 * @author Alkis Polyzotis
 * @author Ivo Jimenez
 */
public class DefaultPreparedSQLStatement implements PreparedSQLStatement
{
    /** The optimizer that created this statement. */
    protected final Optimizer optimizer;
    
    /** The SQL statement corresponding to the prepared statement. */
    protected final SQLStatement sql;

    /**
     * Constructs a default prepared statement.
     *
     * @param optimizer
     *      the optimizer that created this statement
     * @param sql
     *      the sql statement
     */
    public DefaultPreparedSQLStatement(Optimizer optimizer, SQLStatement sql)
    {
        this.optimizer = optimizer;
        this.sql       = sql;
    }
    
    /**
     * Constructs a {@link DefaultPreparedSQLStatement} out af another {@link PreparedSQLStatement}.
     *
     * @param other
     *      the existing {@link PreparedSQLStatement}
     */
    public DefaultPreparedSQLStatement(PreparedSQLStatement other)
    {
        this.optimizer = other.getOptimizer();
        this.sql       = other.getSQLStatement();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optimizer getOptimizer()
    {
        return optimizer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLStatement getSQLStatement()
    {
        return sql;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExplainedSQLStatement explain(Set<Index> configuration)
        throws SQLException
    {
        return optimizer.explain(sql, configuration);
    }
}
