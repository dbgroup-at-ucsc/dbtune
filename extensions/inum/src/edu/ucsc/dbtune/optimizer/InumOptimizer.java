package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.inum.EagerSpaceComputation;
import edu.ucsc.dbtune.inum.ExhaustiveMatchingStrategy;
import edu.ucsc.dbtune.inum.InumSpaceComputation;

import edu.ucsc.dbtune.optimizer.plan.InumPlan;
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
    /**
     * inum space computation. This could be passed as argument of the constructor if other kind of 
     * computation is needed
     */
    private InumSpaceComputation inumSpaceComputation;

    /**
     * Constructs an {@code InumOptimizer}. Relies on the given {@code optimizer} to execute actual 
     * optimization calls.
     *
     * @param optimizer
     *      a DBMS-specific implementation of an {@link Optimizer} type.
     */
    public InumOptimizer(Optimizer optimizer)
    {
        this.delegate = optimizer;
        this.inumSpaceComputation = new EagerSpaceComputation();
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
        return inumSpaceComputation.compute(sql, delegate, catalog);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedSQLStatement prepareExplain(SQLStatement sql) 
        throws SQLException
    {
        return new InumPreparedSQLStatement(this, sql, new ExhaustiveMatchingStrategy());
    }
}
