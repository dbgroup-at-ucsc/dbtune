package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.ibg.IndexBenefitGraphConstructor.construct;

/**
 * Represents a variant of the optimizer concept in the dbtune API that relies on the {@link 
 * IndexBenefitGraph} to optimize statements.
 *
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 * @author Neoklis Polyzotis
 */
public class IBGOptimizer implements Optimizer
{
    /**
     * The {@link Optimizer} that the {@link IBGOptimizer} uses for actual what-if calls.
     */
    protected Optimizer delegate;

    /**
     * Constructs an {@code IBGOptimizer}. Relies on the given {@code optimizer} to execute actual 
     * optimization calls.
     *
     * @param optimizer
     *      a DBMS-specific implementation of an {@link Optimizer} type.
     */
    public IBGOptimizer(Optimizer optimizer)
    {
        this.delegate = optimizer;
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
     * Build an {@link IndexBenefitGraph} corresponding to a specific {@link SQLStatement} and 
     * a specific {@link Configuration} that represents the universe of indexes.
     *
     * @param sql
     *      the statement
     * @param universe
     *      the configuration that comprises all indexes of interest
     * @return
     *      the index benefit graph
     * @throws SQLException
     *      if there's an error
     */
    IndexBenefitGraph buildIBG(SQLStatement sql, Set<Index> universe)
        throws SQLException
    {
        return construct(delegate, sql, universe);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedSQLStatement prepareExplain(SQLStatement sql) 
        throws SQLException
    {
        return new IBGPreparedSQLStatement(this, sql, null, null);
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

    @Override
    public void setFTSDisabled(Set<Table> tables, boolean isFTSDisabled) 
    {
        delegate.setFTSDisabled(tables, isFTSDisabled);
    }
}
