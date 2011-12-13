package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.sql.SQLException;

/**
 * Common base class for optimizers.
 * @author alkis
 */
public abstract class AbstractOptimizer implements Optimizer
{
    protected Catalog catalog;

    /**
     * Counter for the actual number of what-if calls
     */
    protected int whatIfCount=0;
    
    /**
     * perform an optimization call for a SQL statement.
     *
     * @param sql
     *      SQL statement
     * @return
     *      an {@link ExplainedSQLStatement} object describing the results of an optimization call.
     * @throws SQLException
     *      if an error occurs while retrieving the plan
     */
    public ExplainedSQLStatement explain(String sql) throws SQLException
    {
        return explain(new SQLStatement(sql), new Configuration("empty"));
    }

    /**
     * perform an optimization call for a SQL statement.
     *
     * @param sql
     *      SQL statement
     * @return
     *      an {@link ExplainedSQLStatement} object describing the results of an optimization call.
     * @throws SQLException
     *      if an error occurs while retrieving the plan
     */
    public ExplainedSQLStatement explain(SQLStatement sql) throws SQLException
    {
        return explain(sql, new Configuration("empty"));
    }

    /**
     * estimate what-if optimization plan of a statement using the given configuration.
     *
     * @param sql
     *     sql statement
     * @param configuration
     *     physical configuration the optimizer should consider when preparing the statement
     * @return
     *     an {@link ExplainedSQLStatement} object describing the results of a what-if optimization 
     *     call.
     * @throws java.sql.SQLException
     *     unable to estimate cost for the stated reasons.
     */
    public ExplainedSQLStatement explain(String sql, Configuration configuration)
        throws SQLException
    {
        return explain(new SQLStatement(sql), configuration);
    }
    
    /**
     * estimate what-if optimization plan of a statement using the given configuration.
     *
     * @param sql
     *     sql statement
     * @param configuration
     *     physical configuration the optimizer should consider when preparing the statement
     * @return
     *     an {@link ExplainedSQLStatement} object describing the results of a what-if optimization 
     *     call.
     * @throws java.sql.SQLException
     *     unable to estimate cost for the stated reasons.
     */
    public abstract ExplainedSQLStatement explain(SQLStatement sql, Configuration configuration)
        throws SQLException;
    
    /**
     * Given a sql statement, it recommends indexes to make it run faster.
     *
     * @param sql
     *      SQL statement
     * @return
     *      a list of indexes that would improve the statement's performance
     * @throws SQLException
     *      if an error occurs while retrieving the plan
     */
    public Configuration recommendIndexes(String sql) throws SQLException
    {
        return recommendIndexes(new SQLStatement(sql));
    }

    /**
     * Given a sql statement, it recommends indexes to make it run faster.
     *
     * @param sql
     *      SQL statement
     * @return
     *      a list of indexes that would improve the statement's performance
     * @throws SQLException
     *      if an error occurs while retrieving the plan
     */
    public abstract Configuration recommendIndexes(SQLStatement sql) throws SQLException;

    /**
     * Assigns the catalog that should be used to bind metadata to prepared statements.
     *
     * @param catalog
     *     metadata to be used when binding database objects.
     */
    public void setCatalog(Catalog catalog)
    {
        this.catalog = catalog;
    }
    
    public int getWhatIfCount() 
    {
    	return whatIfCount;
    }
    
    @Override
    public PreparedSQLStatement prepareExplain(SQLStatement sql)
    	throws SQLException
    {
    	return new DefaultPreparedSQLStatement(this,sql);
    }
}
