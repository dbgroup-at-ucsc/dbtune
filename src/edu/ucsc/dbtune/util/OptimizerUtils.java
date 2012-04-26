package edu.ucsc.dbtune.util;

import edu.ucsc.dbtune.optimizer.Optimizer;

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
}
