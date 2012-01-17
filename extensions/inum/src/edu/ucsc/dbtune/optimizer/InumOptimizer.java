package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.inum.ExhaustiveInumSpaceComputation;
import edu.ucsc.dbtune.inum.ExhaustiveMatchingStrategy;
import edu.ucsc.dbtune.inum.InumSpaceComputation;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;

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
public class InumOptimizer implements Optimizer
{
    /**
     * The {@link Optimizer} that the {@link InumOptimizer} uses for actual what-if calls.
     */
    protected Optimizer delegate;

    /** local reference to the catalog. */
    private Catalog catalog;

    /** inum space computation. This could be passed as argument if other kind of computation is 
     * needed
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
        this.inumSpaceComputation = new ExhaustiveInumSpaceComputation();
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

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> recommendIndexes(SQLStatement sql) throws SQLException
    {
        return delegate.recommendIndexes(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCatalog(Catalog catalog)
    {
        delegate.setCatalog(catalog);
        this.catalog = catalog;
    }

    /**
     * estimate what-if optimization cost given a single sql statement.
     *
     * @param sql
     *      sql statement
     * @param configuration
     *      an index configuration
     * @return
     *      the prepared statement
     * @throws SQLException
     *      unable to estimate cost due to the stated reasons.
     */
    public ExplainedSQLStatement explain(SQLStatement sql, Set<Index> configuration)
        throws SQLException
    {
        return delegate.explain(sql, configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExplainedSQLStatement explain(String sql) throws SQLException
    {
        return delegate.explain(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExplainedSQLStatement explain(SQLStatement sql) throws SQLException
    {
        return delegate.explain(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExplainedSQLStatement explain(String sql, Set<Index> configuration)
        throws SQLException
    {
        return delegate.explain(sql, configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> recommendIndexes(String sql) throws SQLException
    {
        return delegate.recommendIndexes(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getWhatIfCount()
    {
        return delegate.getWhatIfCount();
    }
}
