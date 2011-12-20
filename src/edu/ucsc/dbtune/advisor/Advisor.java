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
public abstract class Advisor
{
    /**
     * Adds a query to the set of queries that are considered for recommendation.
     *
     * @param sql
     *      sql statement
     * @throws SQLException
     *      if the given statement can't be processed
     */
    public abstract void process(SQLStatement sql) throws SQLException;

    /**
     * Returns the configuration obtained by the Advisor.
     *
     * @return
     *      a {@code Configuration} object containing the information related to
     *      the recommendation produced by the advisor.
     * @throws SQLException
     *      if the given statement can't be processed
     */
    public abstract Set<Index> getRecommendation() throws SQLException;
}
