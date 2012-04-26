package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;

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
     * For UPDATE statements, this is the cost, from the internal plan cost, that corresponds to 
     * doing the update.
     */
    protected double baseTableUpdateCost;

    /**
     * For UPDATE statements, this is the table being updated.
     */
    protected Table updatedTable;

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
     * Returns the table that the update operates on.
     *
     * @return
     *     the updated base table. If the statement is a {@link SQLCategory#SELECT} statement, the 
     *     value returned is {@code null}.
     */
    public Table getUpdatedTable()
    {
        return updatedTable;
    }

    /**
     * Returns the update cost associated to the cost of updating the base table.
     *
     * @param indexes
     *      indexes for which the update cost is obtained
     * @return
     *      a Map of incurred update costs, where each element corresponds to an index contained in 
     *      {@link #getUpdatedConfiguration}
     */
    public Map<Index, Double> getIndexUpdateCosts(Set<Index> indexes)
    {
        Map<Index, Double> indexUpdateCosts = new HashMap<Index, Double>();

        if (sql.getSQLCategory().isSame(NOT_SELECT))
            for (Index i : indexes)
                // we're approximating the index update cost, by assuming that the cost of updating 
                // an index isn't higher than that of updating the base table
                if (i.getTable().equals(getUpdatedTable()))
                    indexUpdateCosts.put(i, getBaseTableUpdateCost());

        return indexUpdateCosts;
    }

    /**
     * Returns the update cost associated to the cost of updating the base table.
     *
     * @return
     *     the update cost of the base table. If the statement is a {@link SQLCategory#SELECT} 
     *     statement, the value returned is zero.
     */
    public double getBaseTableUpdateCost()
    {
        return baseTableUpdateCost;
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
