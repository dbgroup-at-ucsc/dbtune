package edu.ucsc.dbtune.advisor;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

/**
 * Represents an advisor that recommends physical design modifications.
 *
 * @author Ivo Jimenez
 */
public abstract class Advisor
{
    /**
     * Adds the statements contained in a workload to the set of statements that are considered for 
     * recommendation.
     *
     * @param workload
     *      sql statements
     * @throws SQLException
     *      if the given workload can't be processed
     */
    public void process(Workload workload) throws SQLException
    {
        for (SQLStatement sql : workload)
            process(sql);
    }

    /**
     * Adds an element to the set of statements that are considered for recommendation.
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
     *      a set of indexes recommended for the statements given so far. This might differ from 
     *      invocation to invocation, depending on what the {@link #process} method is internally 
     *      doing.
     * @throws SQLException
     *      if the recommendation can't be retrieved
     */
    public abstract Set<Index> getRecommendation() throws SQLException;

    /**
     * Returns statistics about the recommendation done by the advisor.
     *
     * @return
     *      a POJO containing the information related to the recommendation produced by the advisor.
     * @throws SQLException
     *      if the given statement can't be processed
     */
    public abstract RecommendationStatistics getRecommendationStatistics() throws SQLException;
}
