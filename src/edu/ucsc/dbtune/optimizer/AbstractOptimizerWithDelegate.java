package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;

import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;

import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Common base class for optimizers that use a delegate to execute all but {@link #prepareExplain}.
 *
 * @author Ivo Jimenez
 */
public abstract class AbstractOptimizerWithDelegate extends AbstractOptimizer
{
    /**
     * The {@link Optimizer} that the {@link IBGOptimizer} uses for actual what-if calls.
     */
    protected Optimizer delegate;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public abstract PreparedSQLStatement prepareExplain(SQLStatement sql) throws SQLException;

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
        this.catalog = catalog;
        delegate.setCatalog(catalog);
    }

    /**
     * {@inheritDoc}
     */
    @Override
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFTSDisabled(boolean isFTSDisabled)
    {
        delegate.setFTSDisabled(isFTSDisabled);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optimizer getDelegate()
    {
        return delegate;
    }
}
