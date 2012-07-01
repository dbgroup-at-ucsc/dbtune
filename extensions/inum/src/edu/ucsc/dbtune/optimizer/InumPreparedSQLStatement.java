package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.inum.MatchingStrategy;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;

/**
 * An INUM-based prepared statement. This object has an INUM space associated to it (a set of {@link 
 * edu.ucsc.dbtune.inum.DATInumPlan} objects) that gets populated the first time that the {@link 
 * #explain} method is invoked (by calling the {@link InumOptimizer#computeInumSpace}).
 * <p>
 * After the INUM space has been populated, a {@link MatchingStrategy} is used to determine the 
 * optimal plan to given {@link #explain}.
 *
 * @author Ivo Jimenez
 * @see <a href="http://portal.acm.org/citation.cfm?id=1325974"?>
 *         Efficient use of the query optimizer for automated physical design
 *      </a>
 */
public class InumPreparedSQLStatement extends DefaultPreparedSQLStatement
{
    private Set<InumPlan> inumSpace;
    private MatchingStrategy matchingStrategy;

    /**
     * Constructcs a prepared statement.
     *  
     * @param optimizer
     *      optimizer that created this statement
     * @param sql
     *      statement that corresponds to this prepared statement
     * @param matchingStrategy
     *      strategy that should be used when searching for the optimal template for a given 
     *      configuration.
     * @throws SQLException
     *      if an error occurs while computing the inum space
     */
    InumPreparedSQLStatement(
            InumOptimizer optimizer,
            SQLStatement sql,
            MatchingStrategy matchingStrategy)
        throws SQLException
    {
        super(optimizer, sql);

        this.matchingStrategy = matchingStrategy;

        inumSpace = optimizer.computeInumSpace(sql);
    }

    /**
     * Uses the {@link MatchingStrategy} to determine the optimal plan for the given {@code 
     * configuration}. If the INUM space hasn't been populated, it invokes {@link 
     * InumOptimizer#computeInumSpace} method in order to do so.
     *
     * @param configuration
     *      the set of indexes considered to estimate the cost of the new statement.
     * @return
     *      a new statement
     * @throws SQLException
     *      if it's not possible to do what-if optimization on the given configuration
     */
    @Override
    public ExplainedSQLStatement explain(Set<Index> configuration) throws SQLException
    {
        MatchingStrategy.Result result = matchingStrategy.match(inumSpace, configuration);

        Map<Index, Double> indexUpdateCosts = new HashMap<Index, Double>();

        if (sql.getSQLCategory().isSame(NOT_SELECT))
            for (Index i : configuration)
                // we're approximating the index update cost, by assuming that the cost of updating 
                // an index isn't higher than that of updating the base table
                if (i.getTable().equals(result.getBestTemplate().getUpdatedTable()))
                    indexUpdateCosts.put(i, result.getBestTemplate().getBaseTableUpdateCost());

        return new ExplainedSQLStatement(
            sql,
            result.getInstantiatedPlan(),
            getOptimizer(),
            result.getBestCost() - result.getBestTemplate().getBaseTableUpdateCost(),
            result.getBestTemplate().getUpdatedTable(),
            result.getBestTemplate().getBaseTableUpdateCost(),
            indexUpdateCosts,
            configuration,
            new HashSet<Index>(result.getInstantiatedPlan().getIndexes()),
            0);
        // XXX: count of zero is because we assume a warm cache. If needed, it can be improved
    }
    
    /**
     * Returns the set of template plans contained in the INUM space.
     *
     * @return
     *      set of template plans
     */
    public Set<InumPlan> getTemplatePlans()
    {
        return inumSpace;
    }
}
