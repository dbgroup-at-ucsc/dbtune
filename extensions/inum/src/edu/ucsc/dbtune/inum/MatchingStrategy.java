package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;

/**
 * This represents the matching logic that determines the optimality of reused plans. Given a set of 
 * strict rules, this logic will efficiently assign, a given set of template plans and a 
 * configuration, the corresponding optimal plan, without going to the optimizer, simply by adding 
 * cached costs to access the tables referenced in the statement.
 *
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public interface MatchingStrategy
{
    /**
     * It matches the input configuration to its corresponding optimal plan. If the input matches
     * more than one optimal plan, then pick the one with minimum cost.
     *
     * @param inumSpace
     *    the space containing all cached optimal plans.
     * @param configuration
     *    an input configuration for which we will find its optimal plan.
     * @return
     *    the matching result
     * @throws SQLException
     *    if the given configuration can't be used to obtain the best template plan
     */
    Result match(Set<InumPlan> inumSpace, Set<Index> configuration)
        throws SQLException;

    /**
     * The result of a matching operation.
     *
     * @author Ivo Jimenez
     */
    public static class Result
    {
        private double bestCost;
        private InumPlan bestPlan;
        private Set<Index> bestConfiguration;

        /**
         * construct a result with the given arguments.
         *
         * @param bestPlan
         *    the template plan with the best cost for the configuration
         * @param bestConfiguration
         *    configuration corresponding to the best plan
         * @param bestCost
         *    cost associated to the plan
         */
        public Result(InumPlan bestPlan, Set<Index> bestConfiguration, double bestCost)
        {
            this.bestPlan = bestPlan;
            this.bestConfiguration = bestConfiguration;
            this.bestCost = bestCost;
        }

        /**
         * Gets the bestCost for this instance.
         *
         * @return The bestCost.
         */
        public double getBestCost()
        {
            return this.bestCost;
        }

        /**
         * Gets the bestPlan for this instance.
         *
         * @return The bestPlan.
         */
        public InumPlan getBestPlan()
        {
            return this.bestPlan;
        }

        /**
         * Gets the bestConfiguration for this instance.
         *
         * @return The bestConfiguration.
         */
        public Set<Index> getBestConfiguration()
        {
            return this.bestConfiguration;
        }
    }
}
