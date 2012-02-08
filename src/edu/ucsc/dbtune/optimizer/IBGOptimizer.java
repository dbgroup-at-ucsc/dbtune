package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.metadata.Index;
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
        this.delegate = optimizer;
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
}
