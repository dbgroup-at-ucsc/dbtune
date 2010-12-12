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

import edu.ucsc.dbtune.core.metadata.PGIndex;
import edu.ucsc.dbtune.util.DefaultBitSet;
import org.junit.Before;
import org.junit.Test;

import static edu.ucsc.dbtune.core.DBTuneInstances.newPGIndex;
import static org.junit.Assert.assertTrue;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class IndexBenefitGraphWhatIfOptimizerTest {
    private DatabaseConnectionManager<PGIndex>  connectionManager;
    @Before
    public void setUp() throws Exception {
         connectionManager = DBTuneInstances.newPGDatabaseConnectionManager();
    }

    @Test
    public void testWhatIfOptimizerForRandQuery() throws Exception {
        final DatabaseConnection<PGIndex>      connect = connectionManager.connect();
        final DatabaseWhatIfOptimizer<PGIndex> whatIf  = whatIfOptimizer(connect);
        final double cost  = whatIf.whatIfOptimize(basicQuery())
                                   .using(
                                           firstIndexesConfiguration(),
                                           usedSet()
                                   )
                                   .toGetCost();
        assertTrue(Double.compare(cost, 1.0) == 0);
    }

    private static DefaultBitSet firstIndexesConfiguration(){
        return new DefaultBitSet(){{
            set(12);
            set(21);
            set(31);
        }};
    }

    private static DefaultBitSet usedSet(){
        return new DefaultBitSet(){{set(21);}};
    }



    private static String basicQuery(){
        return "SELECT R.salary, R.timeOffCount FROM R WHERE R.fullname = 'Bruce Wayne';";
    }

    private static DatabaseWhatIfOptimizer<PGIndex> whatIfOptimizer(DatabaseConnection<PGIndex> connection){
        return connection.getWhatIfOptimizer();
    }
       
}
