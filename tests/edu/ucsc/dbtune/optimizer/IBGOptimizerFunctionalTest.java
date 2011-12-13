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
public class IBGOptimizerFunctionalTest
{
    /**
     * @see checkUsedBitSet
     */
    @Test
    public void testUsedBitSet() throws Exception
    {
        IBGOptimizerTest.checkUsedBitSet();
    }
}
