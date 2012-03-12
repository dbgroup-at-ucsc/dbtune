package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.inum.InumSpaceComputation;
import edu.ucsc.dbtune.inum.MatchingStrategy;

import edu.ucsc.dbtune.optimizer.plan.InumPlan;

import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.InumPlanSetWithCache;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Implements an INUM-based optimizer.
 * <p>
 * The passed delegate optimizer should be able to produce plans, i.e. the {@link 
 * edu.ucsc.dbtune.optimizer.ExplainedSQLStatement} objects that get generated by it should return a 
 * non-null instance when calling {@link edu.ucsc.dbtune.optimizer.ExplainedSQLStatement#getPlan}.
 *
 * @author Ivo Jimenez
 */
public class InumOptimizer extends AbstractOptimizerWithDelegate
{
    private boolean useInumCache;
    private InumSpaceComputation inumSpaceComputation;
    private MatchingStrategy matchingStrategy;

    /**
     * Constructs an {@code InumOptimizer}. Relies on the given {@code optimizer} to execute actual 
     * optimization calls.
     *
     * @param optimizer
     *      a DBMS-specific implementation of an {@link Optimizer} type.
     * @param env
     *      to get properties about the environment where the optimizer is running
     * @throws SQLException
     *      if the underlying {@link InumSpaceComputation} specified by the {@code env} object can't 
     *      be instantiated.
     */
    public InumOptimizer(Optimizer optimizer, Environment env) throws SQLException
    {
        this.delegate = optimizer;

        if (env.getInumSlotCache())
            useInumCache = true;
        else
            useInumCache = false;

        try {
            inumSpaceComputation = InumSpaceComputation.Factory.newInumSpaceComputation(env);
            matchingStrategy = MatchingStrategy.Factory.newMatchingStrategy(env);
        } catch (InstantiationException ex) {
            throw new SQLException(ex);
        }
    }

    /**
     * Build an {@link Inum} object corresponding to a specific {@link SQLStatement}.
     *
     * @param sql
     *      the statement
     * @return
     *      the index usage model for the given statement
     * @throws SQLException
     *      if there's an while building the Inum object
     */
    Set<InumPlan> computeInumSpace(SQLStatement sql) throws SQLException
    {
        Set<InumPlan> inumSpace;

        if (useInumCache)
            inumSpace = new InumPlanSetWithCache();
        else
            inumSpace = new HashSet<InumPlan>();

        inumSpaceComputation.compute(inumSpace, sql, delegate, catalog);

        return inumSpace;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedSQLStatement prepareExplain(SQLStatement sql) throws SQLException
    {
        return new InumPreparedSQLStatement(this, sql, matchingStrategy);
    }
}
