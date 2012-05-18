package edu.ucsc.dbtune.util;

import java.sql.SQLException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;

import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;

/**
 * Utilities for testing.
 *
 * @author Ivo Jimenez
 */
public final class OptimizerUtils
{
    /**
     * Utility class.
     */
    private OptimizerUtils()
    {
    }

    /**
     * Returns the base Optimizer, i.e. the DBMSOptimizer (eg. MySQLOptimizer, DB2Optimizer, etc...)
     * <p>
     * Keeps getting the delegate until there's no one. This is useful for when more than one 
     * optimizer is layered on top of the base one (e.g. IBG on top of INUM on top of DB2) 
     *
     * @param optimizer
     *      for which the base optimizer is retrieved
     * @return
     *      the base optimizer
     */
    public static Optimizer getBaseOptimizer(Optimizer optimizer)
    {
        if (optimizer.getDelegate() == null)
            return optimizer;

        Optimizer baseOptimizer = optimizer.getDelegate();

        while (baseOptimizer.getDelegate() != null)
            baseOptimizer = baseOptimizer.getDelegate();

        return baseOptimizer;
    }

    /**
     * Returns the benefits of having each index in the set. For each index, the {@link #getBenefit} 
     * method is invoked.
     *
     * @param pStmt
     *      a prepared statement
     * @param indexes
     *      index set the benefits are obtained for
     * @return
     *      a map containing the benefit of each index
     * @throws SQLException
     *      if an error occurs while explaining through the given prepared statement
     */
    public static Map<Index, Double> getBenefits(PreparedSQLStatement pStmt, Set<Index> indexes)
        throws SQLException
    {
        Map<Index, Double> benefits = new HashMap<Index, Double>();

        for (Index idx : indexes)
            benefits.put(idx, getBenefit(pStmt, idx, indexes));

        return benefits;
    }

    /**
     * Returns the benefit of having an index in a configuration. The benefit of an index is just 
     * the cost of the statement without the index, minus the cost of having that index in the set 
     * of indexes passed to the what-if optimization call.
     *
     * @param pStmt
     *      statement
     * @param index
     *      index for which the benefit is calculated
     * @param indexes
     *      set of indexes used to explain the statement
     * @return
     *      the benefit of the given index
     * @throws SQLException
     *      if there's an issue while explaining the statement
     */
    public static double getBenefit(PreparedSQLStatement pStmt, Index index, Set<Index> indexes)
        throws SQLException
    {
        Set<Index> without = new HashSet<Index>(indexes);
        Set<Index> with = indexes;

        without.remove(index);

        return pStmt.explain(without).getTotalCost() - pStmt.explain(with).getTotalCost();
    }
}
