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

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.util.Environment;

import java.sql.Connection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.DatabaseSystem.newConnection;

/**
 * Functional test for optimizer implementations. The optimizer being tested is specified by the 
 * {@link EnvironmentProperties#OPTIMIZER} property.
 * <p>
 * This test executes all the tests for which {@link OptimizerTest} relies on DBMS-specific mocks 
 * (eg. classes contained in {@link java.sql}).
 *
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 * @see OptimizerTest
 */
public class OptimizerFunctionalTest
{
    public static DatabaseSystem db;
    public static Environment    env;
    public static Optimizer      opt;

    @BeforeClass
    public static void setUp() throws Exception
    {
        Connection con;
        String     ddl;

        env  = Environment.getInstance();
        ddl = env.getScriptAtWorkloadsFolder("one_table/create.sql");
        con = newConnection(env);

        //execute(con, ddl);
        con.close();

        db  = newDatabaseSystem(env);
        opt = db.getOptimizer();
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        db.getConnection().close();
    }

    /**
     * @see OptimizerTest#checkExplain
     */
    @Test
    public void testExplain() throws Exception
    {
        OptimizerTest.checkExplain(opt);
    }

    /**
     * @see OptimizerTest#checkWhatIfExplain
     */
    @Test
    public void testWhatIfExplain() throws Exception
    {
        OptimizerTest.checkWhatIfExplain(db.getCatalog(),opt);
    }

    /**
     * @see OptimizerTest#checkRecommendIndexes
     */
    @Test
    public void testRecommendIndexes() throws Exception
    {
        OptimizerTest.checkRecommendIndexes(opt);
    }

    /**
     * @see OptimizerTest#checkUsedIndexes
     */
    @Test
    public void testUsedIndexes() throws Exception
    {
        OptimizerTest.checkUsedConfiguration(db.getCatalog(), opt);
    }

    /**
     * Checks that each supported optimizer is well behaved.
     *
     * @see OptimizerTest#checkIsWellBehaved
     */
    @Test
    public void testIsWellBehaved() throws Exception
    {
        OptimizerTest.checkIsWellBehaved(opt);
    }

    /**
     * @see OptimizerTest#checkMonotonicity
     */
    @Test
    public void testMonotonicity() throws Exception
    {
        OptimizerTest.checkMonotonicity(db.getCatalog(),opt);
    }

    /**
     * @see OptimizerTest#checkSanity
     */
    @Test
    public void testSanity() throws Exception
    {
        OptimizerTest.checkSanity(db.getCatalog(),opt);
    }
}
