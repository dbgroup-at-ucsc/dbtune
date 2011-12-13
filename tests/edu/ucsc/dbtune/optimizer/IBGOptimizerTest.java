package edu.ucsc.dbtune.optimizer;

import org.junit.Test;

/**
 * IBG-specific functional tests.
 * <p>
 * This test executes all the tests for which {@link IBGOptimizerTest} relies on DBMS-specific mocks 
 * (i.e. classes contained in {@link java.sql}).
 *
 * @author Ivo Jimenez
 * @see OptimizerTest
 */
public class IBGOptimizerTest
{
    /**
     * @see checkUsedBitSet
     */
    @Test
    public void testUsedBitSet() throws Exception
    {
        checkUsedBitSet();
    }

    /**
     * Checks that the bitSet returned by an {@link IBGOptimizer#explain} invokation are turned on 
     * appropriately
     */
    public static void checkUsedBitSet() throws Exception
    {
        // XXX
    }

    /**
     * Checks that the number of optimization counts is set appropriately, taking into account that 
     * the optimizer is an IBG-based one.
     */
    public static void checkOptimizationCount() throws Exception
    {
        // XXX
    }
}
