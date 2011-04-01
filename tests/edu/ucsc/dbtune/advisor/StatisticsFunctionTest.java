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

import edu.ucsc.dbtune.core.DBTuneInstances;
import edu.ucsc.dbtune.core.SQLStatement;
import edu.ucsc.dbtune.core.metadata.PGExplainInfo;
import edu.ucsc.dbtune.core.metadata.PGIndex;
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.ibg.InteractionBank;
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
    StatisticsFunction<PGIndex> pgStatistics;
    @Before
    public void setUp() throws Exception {
        pgStatistics = new IndexStatisticsFunction<PGIndex>(100);
    }


    @Test @Ignore
    public void testBasicDoiScenario() throws Exception {
        final List<PGIndex> callback = Instances.newList();
        final DynamicIndexSet<PGIndex> dynamicSet = makeDynamicIndexSet(callback, 1, 2, 3, 4);
        pgStatistics.addQuery(makeProfiledQuery(callback), dynamicSet);
        final double a = pgStatistics.doi(callback.get(0), callback.get(1));
        assertTrue(Double.compare(3.6666666666666665, a) == 0);
    }

    @Test @Ignore
    public void testBasicBenefitScenario() throws Exception {
        final List<PGIndex> callback = Instances.newList();
        final DynamicIndexSet<PGIndex> dynamicSet = makeDynamicIndexSet(callback, 1, 2, 3, 4);
        pgStatistics.addQuery(makeProfiledQuery(callback), dynamicSet);
        final double b = pgStatistics.benefit(callback.get(0), new IndexBitSet());
        assertTrue(Double.compare(-1.6666666666666667, b) == 0);
    }


    @After
    public void tearDown() throws Exception {
        pgStatistics = null;
    }

    private static ProfiledQuery<PGIndex> makeProfiledQuery(List<PGIndex> callback) throws Exception {
        final CandidatePool<PGIndex> candidatePool = new CandidatePool<PGIndex>();
        candidatePool.addIndex(callback.get(0));
        candidatePool.addIndex(callback.get(1));

        return new ProfiledQuery.Builder<PGIndex>("SELECT * FROM R;")
               .snapshotOfCandidateSet(candidatePool.getSnapshot())
               .indexBenefitGraph(new IndexBenefitGraph(makeIBGNode(), 5.0, new IndexBitSet()))
               .explainInfo(new PGExplainInfo(SQLStatement.SQLCategory.DML, new double[]{5.0, 3.0, 7.0, 6.0}))
               .interactionBank(makeInteractionBank())
               .indexBenefitGraphAnalysisTime(40000).get();
    }

    private static IndexBenefitGraph.IBGNode makeIBGNode() throws Exception {
        //IBGNode(BitSet config, int id) {
        final Constructor<IndexBenefitGraph.IBGNode> c = IndexBenefitGraph.IBGNode.class.getDeclaredConstructor(IndexBitSet.class, int.class);
        c.setAccessible(true);
        return c.newInstance(new IndexBitSet(), 555555555);
    }


    private static InteractionBank makeInteractionBank() throws Exception {
        final CandidatePool<PGIndex> candidatePool = new CandidatePool<PGIndex>();
        candidatePool.addIndex(DBTuneInstances.newPGIndex(4567, 987123456));
        candidatePool.addIndex(DBTuneInstances.newPGIndex(7654, 4567932));
        final InteractionBank bank = new InteractionBank(candidatePool.getSnapshot());
        bank.assignInteraction(0, 1, 11.0);
        return bank;
    }

    private static DynamicIndexSet<PGIndex> makeDynamicIndexSet(final List<PGIndex> callback, final int... ids) throws Exception {
        return new DynamicIndexSet<PGIndex>(){{
            for(int id : ids){
                final PGIndex local = DBTuneInstances.newPGIndex(id+id, id);
                callback.add(local);
                add(local);
            }
        }};
    }
}
