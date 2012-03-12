package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.util.Environment;

import static edu.ucsc.dbtune.util.EnvironmentProperties.EXHAUSTIVE;
import static edu.ucsc.dbtune.util.EnvironmentProperties.GREEDY;

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
     * Matches the input configuration to its corresponding optimal plan. If the input matches
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
     */
    public abstract class Factory
    {
        /**
         * utility class.
         */
        private Factory()
        {
        }

        /**
         * Creates a matching strategy.
         *
         * @param env
         *      used to access the type of matching strategy to instantiate.
         * @return
         *      a new matching strategy
         * @throws InstantiationException
         *      if throws an exception
         */
        public static MatchingStrategy newMatchingStrategy(Environment env)
            throws InstantiationException
        {
            if (env.getInumMatchingStrategy().equals(GREEDY))
                return new GreedyMatchingStrategy();
            else if (env.getInumMatchingStrategy().equals(EXHAUSTIVE))
                return new ExhaustiveMatchingStrategy();

            throw new InstantiationException(
                    "Unknown matching strategy option " + env.getInumMatchingStrategy());
        }
    }

    /**
     * The result of a matching operation.
     *
     * @author Ivo Jimenez
     */
    public static class Result
    {
        private double bestCost;
        private InumPlan bestTemplate;
        private Set<Index> bestConfiguration;
        private SQLStatementPlan instantiatedPlan;

        /**
         * construct a result with the given arguments.
         *
         * @param instantiatedPlan
         *    plan that results from instantiating {@code bestTemplate} with {@code 
         *    bestConfiguration}
         * @param bestTemplate
         *    the template plan with the best cost for the configuration
         * @param bestConfiguration
         *    configuration corresponding to the best plan
         * @param bestCost
         *    cost associated to the plan
         */
        public Result(
                SQLStatementPlan instantiatedPlan,
                InumPlan bestTemplate,
                Set<Index> bestConfiguration,
                double bestCost)
        {
            this.instantiatedPlan = instantiatedPlan;
            this.bestTemplate = bestTemplate;
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
        public InumPlan getBestTemplate()
        {
            return this.bestTemplate;
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

        /**
         * Gets the instantiatedPlan for this instance.
         *
         * @return The instantiatedPlan.
         */
        public SQLStatementPlan getInstantiatedPlan()
        {
            return this.instantiatedPlan;
        }

        @Override
        public String toString()
        {
            String str = "";

            str += "cost: " + bestCost + "\n";
            str += "bestTemplate: " + bestTemplate;
            str += "instance: " + instantiatedPlan + "\n";
            str += "bestConf: " + bestConfiguration;

            return str;
        }
    }
}
