package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * The interface for a what-if optimizer.
 * 
 * @author Ivo Jimenez
 * @author Alkis Polyzotis
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
    ExplainedSQLStatement explain(String sql) throws SQLException;

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
    ExplainedSQLStatement explain(SQLStatement sql) throws SQLException;
    
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
    ExplainedSQLStatement explain(String sql, Set<Index> configuration) throws SQLException;
    
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
    ExplainedSQLStatement explain(SQLStatement sql, Set<Index> configuration) throws SQLException;
    
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
    Set<Index> recommendIndexes(String sql) throws SQLException;

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
    Set<Index> recommendIndexes(SQLStatement sql) throws SQLException;
    
    /**
     * Return the count of actual what-if calls made to the DBMS optimizer.
     * 
     * @return
     *      the number of what-if calls to the DBMS optimizer
     */
    int getWhatIfCount();
    
    /**
     * Set the catalog for this optimizer.
     *
     * @param catalog
     *      the catalog from which the optimizer obtains metadata.
     */
    void setCatalog(Catalog catalog);

    /**
     * Generates a {@link PreparedSQLStatement}, given a {@code sql} object.
     *
     * @param sql
     *      the statement being explained
     * @throws SQLException
     *      if an error occurs while explaining the statement
     * @return
     *      the prepared statement object
     */
    PreparedSQLStatement prepareExplain(SQLStatement sql) throws SQLException;
}
