package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;

import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Represents a variant of the optimizer concept in the dbtune API that relies on the {@link 
 * IndexBenefitGraph} to optimize statements.
 *
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 * @author Neoklis Polyzotis
 */
public class IBGOptimizer extends AbstractOptimizerWithDelegate
{
    /**
     * Constructs an {@code IBGOptimizer}. Relies on the given {@code optimizer} to execute actual 
     * optimization calls.
     *
     * @param optimizer
     *      a DBMS-specific implementation of an {@link Optimizer} type.
     */
    public IBGOptimizer(Optimizer optimizer)
    {
        super(optimizer);
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
}
