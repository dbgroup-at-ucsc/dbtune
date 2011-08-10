/****************************************************************************
 * Copyright 2010 Huascar A. Sanchez                                        *
 *                                                                          *
 * Licensed under the Apache License, Version 2.0 (the "License");          *
 * you may not use this file except in compliance with the License.         *
 * You may obtain a copy of the License at                                  *
 *                                                                          *
 *     http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                          *
 * Unless required by applicable law or agreed to in writing, software      *
 * distributed under the License is distributed on an "AS IS" BASIS,        *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 * See the License for the specific language governing permissions and      *
 * limitations under the License.                                           *
 ****************************************************************************/
package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.connectivity.ConnectionManager;
import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.core.optimizer.IBGWhatIfOptimizer;
import edu.ucsc.dbtune.util.IndexBitSet;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class IndexBenefitGraphWhatIfOptimizerTest {
    private ConnectionManager connectionManager;
    @Before
    public void setUp() throws Exception {
         connectionManager = DBTuneInstances.newPGDatabaseConnectionManager();
    }

    @Test
    public void testWhatIfOptimizerForRandQuery() throws Exception {
        final DatabaseConnection connect = connectionManager.connect();
        final IBGWhatIfOptimizer whatIf  = whatIfOptimizer(connect);
        final double cost  = whatIf.estimateCost(basicQuery(), firstIndexesConfiguration(), usedSet());
        assertTrue(Double.compare(cost, 1.0) == 0);
    }

    private static IndexBitSet firstIndexesConfiguration(){
        return new IndexBitSet(){{
            set(12);
            set(21);
            set(31);
        }};
    }

    private static IndexBitSet usedSet(){
        return new IndexBitSet(){{set(21);}};
    }



    private static String basicQuery(){
        return "SELECT R.salary, R.timeOffCount FROM R WHERE R.fullname = 'Bruce Wayne';";
    }

    private static IBGWhatIfOptimizer whatIfOptimizer(DatabaseConnection connection){
        return connection.getIBGWhatIfOptimizer();
    }
       
}
