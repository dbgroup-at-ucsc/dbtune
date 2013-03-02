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
public interface Advisor
{
    /**
     * Adds an element to the set of statements that are considered for recommendation.
     *
     * @param sql
     *      sql statement
     * @throws SQLException
     *      if the given statement can't be processed
     */
    void process(String sql) throws SQLException;

    /**
     * Adds an element to the set of statements that are considered for recommendation.
     *
     * @param sql
     *      sql statement
     * @throws SQLException
     *      if the given statement can't be processed
     */
    void process(SQLStatement sql) throws SQLException;

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
    Set<Index> getRecommendation() throws SQLException;

    /**
     * Returns statistics about the recommendation done by the advisor.
     *
     * @return
     *      a POJO containing the information related to the recommendation produced by the advisor.
     * @throws SQLException
     *      if the given statement can't be processed
     */
    RecommendationStatistics getRecommendationStatistics() throws SQLException;

    /**
     * The statistics corresponding to the idealized {@code OPT} algorithm.
     *
     * @return
     *      recommendation statistics for {@code OPT}
     * @throws SQLException
     *      if the candidate set wasn't specified from the beginning
     */
    RecommendationStatistics getOptimalRecommendationStatistics() throws SQLException;

    /**
     * @return the isCandidateSetFixed
     */
    boolean isCandidateSetFixed();
}
