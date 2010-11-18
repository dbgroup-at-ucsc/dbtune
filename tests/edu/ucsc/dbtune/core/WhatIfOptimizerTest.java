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
import edu.ucsc.dbtune.util.BitSet;
import edu.ucsc.dbtune.util.Instances;
import edu.ucsc.dbtune.util.Objects;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static edu.ucsc.dbtune.core.DBTuneInstances.newPGIndex;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class WhatIfOptimizerTest {
    private DatabaseConnectionManager<PGIndex>  connectionManager;
    @Before
    public void setUp() throws Exception {
         connectionManager = DBTuneInstances.newPGDatabaseConnectionManager();
    }

    @Test
    public void testWhatIfOptimizerForRandQuery() throws Exception {
        final DatabaseConnection<PGIndex>      connect = connectionManager.connect();
        final DatabaseWhatIfOptimizer<PGIndex> whatIf  = whatIfOptimizer(connect);
        final DatabaseIndexExtractor<PGIndex>  ie      = fixSomeCandidates(connect);
        final double cost  = whatIf.whatIfOptimize(basicQuery())
                                   .using(
                                           firstIndexesConfiguration(),
                                           usedSet()
                                   )
                                   .toGetCost();
        final double cost2 = whatIf.whatIfOptimize(basicQuery())
                .using(
                        secondIndexesConfiguration(),
                        Objects.<AbstractDatabaseIndexExtractor>as(ie).getCachedBitSet()
                )
                .toGetCost();
        System.out.println(Double.compare(cost, cost2) == 0);
    }

    private static DatabaseIndexExtractor<PGIndex> fixSomeCandidates(DatabaseConnection<PGIndex> connection) throws Exception {
        final DatabaseIndexExtractor<PGIndex> ie   = connection.getIndexExtractor();
        final List<PGIndex> pgCandidateSet  = Instances.newList();
        final PGIndex index1 = newPGIndex(12);
        final PGIndex index2 = newPGIndex(21);
        pgCandidateSet.add(index1);
        pgCandidateSet.add(index2);
        ie.fixCandidates(pgCandidateSet);
        return ie;
    }

    private static BitSet firstIndexesConfiguration(){
        return new BitSet(){{
            set(12);
            set(21);
            set(31);
        }};
    }

    private static BitSet secondIndexesConfiguration(){
        return new BitSet(){{
            set(13);
            set(21);
            set(41);
        }};
    }

    private static BitSet usedSet(){
        return new BitSet(){{set(21);}};
    }



    private static String basicQuery(){
        return "SELECT R.salary, R.timeOffCount FROM R WHERE R.fullname = 'Bruce Wayne';";
    }

    private static DatabaseWhatIfOptimizer<PGIndex> whatIfOptimizer(DatabaseConnection<PGIndex> connection){
        return connection.getWhatIfOptimizer();
    }
       
}
