package edu.ucsc.dbtune.advisor;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Represents an advisor that recommends physical design modifications.
 *
 * @author Ivo Jimenez
 */
public abstract class AbstractAdvisor implements Advisor
{
    /**
     * Indicates the advisor that it should process a new statement.
     *
     * @param sql
     *      sql statement
     * @throws SQLException
     *      if the given statement can't be processed
     */
    protected abstract void processNewStatement(SQLStatement sql) throws SQLException;

    /**
     * {@inheritDoc}
     */
    public void process(String sql) throws SQLException
    {
        process(new SQLStatement(sql));
    }

    /**
     * {@inheritDoc}
     */
    public void process(SQLStatement sql) throws SQLException
    {
        processNewStatement(sql);
    }

    /**
     * {@inheritDoc}
     */
    public abstract Set<Index> getRecommendation() throws SQLException;

    /**
     * {@inheritDoc}
     */
    public abstract RecommendationStatistics getRecommendationStatistics()
        throws SQLException;
}
