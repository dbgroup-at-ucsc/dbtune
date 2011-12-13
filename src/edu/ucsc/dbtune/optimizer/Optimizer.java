package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * The interface for a query what-if optimizer.
 * 
 * @author alkis
 *
 */
public interface Optimizer
{
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
    public ExplainedSQLStatement explain(String sql) throws SQLException;

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
    public ExplainedSQLStatement explain(SQLStatement sql) throws SQLException;
    
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
        throws SQLException;
    
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
    public ExplainedSQLStatement explain(SQLStatement sql, Configuration configuration)
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
    public Configuration recommendIndexes(String sql) throws SQLException;

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
    public Configuration recommendIndexes(SQLStatement sql) throws SQLException;
    
    /**
     * Return the count of actual what-if calls made to the DBMS optimizer.
     * 
     * @return
     */
    public int getWhatIfCount();
    
    /**
     * Set the catalog for this optimizer.
     * @param catalog
     */
    public void setCatalog(Catalog catalog);

	PreparedSQLStatement prepareExplain(SQLStatement sql)
			throws SQLException;
}
