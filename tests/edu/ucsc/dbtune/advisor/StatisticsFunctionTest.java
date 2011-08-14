/*
 * ****************************************************************************
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
 *  ****************************************************************************
 */
package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.DBTuneInstances;
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.ibg.InteractionBank;
import edu.ucsc.dbtune.metadata.PGIndex;
import edu.ucsc.dbtune.metadata.SQLCategory;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.IBGPreparedSQLStatement;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Instances;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class StatisticsFunctionTest {
    StatisticsFunction pgStatistics;
    @Before
    public void setUp() throws Exception {
        pgStatistics = new IndexStatisticsFunction(100);
    }


    @Test @Ignore
    public void testBasicDoiScenario() throws Exception {
        final List<PGIndex> callback = Instances.newList();
        final DynamicIndexSet dynamicSet = makeDynamicIndexSet(callback, 1, 2, 3, 4);
        pgStatistics.addQuery(makeProfiledQuery(callback), dynamicSet);
        final double a = pgStatistics.doi(callback.get(0), callback.get(1));
        assertTrue(Double.compare(3.6666666666666665, a) == 0);
    }

    @Test @Ignore
    public void testBasicBenefitScenario() throws Exception {
        final List<PGIndex> callback = Instances.newList();
        final DynamicIndexSet dynamicSet = makeDynamicIndexSet(callback, 1, 2, 3, 4);
        pgStatistics.addQuery(makeProfiledQuery(callback), dynamicSet);
        final double b = pgStatistics.benefit(callback.get(0), new IndexBitSet());
        assertTrue(Double.compare(-1.6666666666666667, b) == 0);
    }


    @After
    public void tearDown() throws Exception {
        pgStatistics = null;
    }

    private static IBGPreparedSQLStatement makeProfiledQuery(List<PGIndex> callback) throws Exception {
        final CandidatePool candidatePool = new CandidatePool();
        candidatePool.addIndex(callback.get(0));
        candidatePool.addIndex(callback.get(1));

        return new IBGPreparedSQLStatement("SELECT * FROM R;",
			   SQLCategory.QUERY,
               candidatePool.getSnapshot(),
               new IndexBenefitGraph(makeIBGNode(), 5.0, new IndexBitSet()),
               makeInteractionBank(),
               40000, 0.0);
    }

    private static IndexBenefitGraph.IBGNode makeIBGNode() throws Exception {
        //IBGNode(BitSet config, int id) {
        final Constructor<IndexBenefitGraph.IBGNode> c = IndexBenefitGraph.IBGNode.class.getDeclaredConstructor(IndexBitSet.class, int.class);
        c.setAccessible(true);
        return c.newInstance(new IndexBitSet(), 555555555);
    }


    private static InteractionBank makeInteractionBank() throws Exception {
        final CandidatePool candidatePool = new CandidatePool();
        candidatePool.addIndex(DBTuneInstances.newPGIndex(4567, 987123456));
        candidatePool.addIndex(DBTuneInstances.newPGIndex(7654, 4567932));
        final InteractionBank bank = new InteractionBank(candidatePool.getSnapshot());
        bank.assignInteraction(0, 1, 11.0);
        return bank;
    }

    private static DynamicIndexSet makeDynamicIndexSet(final List<PGIndex> callback, final int... ids) throws Exception {
        return new DynamicIndexSet(){{
            for(int id : ids){
                final PGIndex local = DBTuneInstances.newPGIndex(id+id, id);
                callback.add(local);
                add(local);
            }
        }};
    }
}
