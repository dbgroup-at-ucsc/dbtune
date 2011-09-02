/* ************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
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
