package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Common base class for optimizers.
 *
 * @author Alkis Polyzotis
 * @author Ivo Jimenez
 */
public abstract class AbstractOptimizer implements Optimizer
{
    /** Catalog object used to obtain database object metadata. */
    protected Catalog catalog;

    /** Counter for the actual number of what-if calls. */
    protected int whatIfCount;
    
    /** Whether or not to disable the generation of plans containing FTS operators. */
    protected boolean isFTSDisabled;

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract ExplainedSQLStatement explain(SQLStatement sql, Set<Index> configuration)
        throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract Set<Index> recommendIndexes(SQLStatement sql) throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFTSDisabled(boolean isFTSDisabled)
    {
        this.isFTSDisabled = isFTSDisabled;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ExplainedSQLStatement explain(String sql) throws SQLException
    {
        return explain(new SQLStatement(sql), new LinkedHashSet<Index>());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExplainedSQLStatement explain(SQLStatement sql) throws SQLException
    {
        return explain(sql, new LinkedHashSet<Index>());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExplainedSQLStatement explain(String sql, Set<Index> configuration) throws SQLException
    {
        return explain(new SQLStatement(sql), configuration);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> recommendIndexes(String sql) throws SQLException
    {
        return recommendIndexes(new SQLStatement(sql));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCatalog(Catalog catalog)
    {
        this.catalog = catalog;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int getWhatIfCount() 
    {
        return whatIfCount;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedSQLStatement prepareExplain(SQLStatement sql) throws SQLException
    {
        return new DefaultPreparedSQLStatement(this, sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optimizer getDelegate()
    {
        return null;
    }
}
