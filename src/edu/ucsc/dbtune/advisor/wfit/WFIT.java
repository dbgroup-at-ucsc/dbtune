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
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied  *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.advisor.wfit;

import edu.ucsc.dbtune.advisor.Advisor;
import edu.ucsc.dbtune.advisor.DynamicIndexSet;
import edu.ucsc.dbtune.advisor.HotSetSelector;
import edu.ucsc.dbtune.advisor.HotsetSelection;
import edu.ucsc.dbtune.advisor.IndexPartitions;
import edu.ucsc.dbtune.advisor.IndexStatisticsFunction;
import edu.ucsc.dbtune.advisor.InteractionSelection;
import edu.ucsc.dbtune.advisor.InteractionSelector;
import edu.ucsc.dbtune.advisor.StaticIndexSet;
import edu.ucsc.dbtune.advisor.StatisticsFunction;
import edu.ucsc.dbtune.ibg.IBGBestBenefitFinder;
import edu.ucsc.dbtune.ibg.InteractionBank;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.optimizer.IBGPreparedSQLStatement;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.util.ArrayList;
import java.util.List;
import java.sql.SQLException;

/**
 * WFIT
 */
public class WFIT extends Advisor
{
    List<IBGPreparedSQLStatement> qinfos;
    List<Double>                  overheads;
    List<Configuration>           configurations;

    Configuration         indexes;
    IndexPartitions       partitions;
    WorkFunctionAlgorithm wfa;
    IBGOptimizer          ibgOptimizer;

    int maxNumIndexes;
    int maxNumStates;
    int windowSize;
    int partitionIterations;

    /**
     */
    public WFIT(
            Optimizer optimizer,
            Configuration configuration,
            int maxNumIndexes,
            int maxNumStates,
            int windowSize,
            int partitionIterations)
    {
        this.indexes             = configuration;
        this.maxNumIndexes       = maxNumIndexes;
        this.maxNumStates        = maxNumStates;
        this.windowSize          = windowSize;
        this.partitionIterations = partitionIterations;
        this.ibgOptimizer        = new IBGOptimizer(optimizer); // XXX: check if optimizer is IBGOptimizer
        this.qinfos              = new ArrayList<IBGPreparedSQLStatement>();
        this.wfa                 = new WorkFunctionAlgorithm(partitions, false);
        this.overheads           = new ArrayList<Double>();
        this.configurations      = new ArrayList<Configuration>();
    }

    /**
     * Adds a query to the set of queries that are considered for recommendation.
     *
     * @param sql
     *      sql statement
     * @throws SQLException
     *      if the given statement can't be processed
     */
    @Override
    public void process(SQLStatement sql) throws SQLException
    {
        IBGPreparedSQLStatement qinfo;

        qinfo = (IBGPreparedSQLStatement) ibgOptimizer.explain(sql,indexes);

        qinfos.add(qinfo);

        partitions =
            getIndexPartitions(
                indexes, qinfos, maxNumIndexes, maxNumStates, windowSize, partitionIterations);

        wfa.repartition(partitions);
        wfa.newTask(qinfo);

        configurations.add(new Configuration(wfa.getRecommendation()));
    }

    /**
     * Returns the configuration obtained by the Advisor.
     *
     * @return
     *      a {@code Configuration} object containing the information related to
     *      the recommendation produced by the advisor.
     * @throws SQLException
     *      if the given statement can't be processed
     */
    @Override
    public Configuration getRecommendation() throws SQLException
    {
        if(qinfos.size() == 0) {
            return new Configuration("");
        }

        return configurations.get(qinfos.size()-1);
    }

    public IBGPreparedSQLStatement getStatement(int i) {
        return qinfos.get(i);
    }
    public IndexPartitions getPartitions() {
        return partitions;
    }

    private IndexPartitions getIndexPartitions(
            Iterable<? extends Index>     candidateSet,
            List<IBGPreparedSQLStatement> qinfos, 
            int                 maxNumIndexes,
            int                 maxNumStates,
            int                 windowSize,
            int                 partitionIterations )
    {
        int maxId = 1024;
        StaticIndexSet hotSet = getHotSet(candidateSet, qinfos, maxNumIndexes, windowSize, maxId);
        InteractionSelection is;

        StatisticsFunction doiFunc =
            new TempDoiFunction(qinfos, candidateSet, windowSize,maxId);

        is = 
            new InteractionSelection(
                    new IndexPartitions(hotSet),
                    doiFunc,
                    maxNumStates,
                    hotSet);

        IndexPartitions parts = 
            InteractionSelector.choosePartitions(is, partitionIterations);

        return parts;
    }

    private StaticIndexSet getHotSet(
            Iterable<? extends Index>     candidateSet,
            List<IBGPreparedSQLStatement> qinfos,
            int                 maxNumIndexes,
            int                 windowSize,
            int                 maxId)
    {
        StatisticsFunction benefitFunc;
        HotsetSelection hs;

        benefitFunc =
            new TempBenefitFunction(
                    qinfos,
                    maxId,
                    windowSize);

        hs =
            new HotsetSelection(
                    candidateSet,
                    new StaticIndexSet(),
                    new DynamicIndexSet(),
                    benefitFunc,
                    maxNumIndexes,
                    false);

        StaticIndexSet hotSet = HotSetSelector.chooseHotSetGreedy(hs);

        return hotSet;
    }

    private class TempDoiFunction extends IndexStatisticsFunction
    {
        private InteractionBank bank;

        TempDoiFunction(
                List<IBGPreparedSQLStatement> qinfos,
                Iterable<? extends Index> candidateSet,
                int indexStatisticsWindow,
                int maxId)
        {
            super(indexStatisticsWindow);

            bank = new InteractionBank(maxId);

            for (Index a : candidateSet) {
                int id_a = a.getId();
                for (Index b : candidateSet) {
                    int id_b = b.getId();
                    if (id_a < id_b) {
                        double doi = 0;
                        for (IBGPreparedSQLStatement qinfo : qinfos) {
                            doi +=
                                qinfo.getInteractionBank().interactionLevel(a.getId(), b.getId());
                        }
                        bank.assignInteraction(a.getId(), b.getId(), doi);
                    }
                }
            }
        }

        @Override
        public double doi(Index a, Index b) {
            return bank.interactionLevel(a.getId(), b.getId());
        }
    }

    private class TempBenefitFunction extends IndexStatisticsFunction {
        List<IBGPreparedSQLStatement> qinfos;

        IBGBestBenefitFinder finder = new IBGBestBenefitFinder();
        double[][]      bbCache;
        double[]        bbSumCache;
        int[][]         componentId;
        IndexBitSet[]   prevM;
        IndexBitSet     diffM;

        TempBenefitFunction(
                List<IBGPreparedSQLStatement> qinfos0,
                int maxInternalId,
                int indexStatisticsWindow)
        {
            super(indexStatisticsWindow);
            qinfos = qinfos0;

            componentId = componentIds(qinfos0, maxInternalId);

            bbCache = new double[maxInternalId+1][qinfos0.size()];
            bbSumCache = new double[maxInternalId+1];
            prevM = new IndexBitSet[maxInternalId+1];
            for (int i = 0; i <= maxInternalId; i++) {
                prevM[i] = new IndexBitSet();
                reinit(i, prevM[i]);
            }
            diffM = new IndexBitSet(); // temp bit set
        }

        private int[][] componentIds(List<IBGPreparedSQLStatement> qinfos, int maxInternalId) {
            int[][] componentId = new int[qinfos.size()][maxInternalId+1];
            int q = 0;
            for (IBGPreparedSQLStatement qinfo : qinfos) {
                IndexBitSet[] parts =
                    qinfo.getInteractionBank().stablePartitioning(0);
                for (Index index : qinfo.getConfiguration()) {
                    int id = index.getId();
                    componentId[q][id] = -id;
                    for (int p = 0; p < parts.length; p++) {
                        if (parts[p].get(id)) {
                            componentId[q][id] = p;
                            break;
                        }
                    }
                }
                ++q;
            }
            return componentId;
        }

        private void reinit(int id, IndexBitSet M) {
            int q = 0;
            double ben = 0;
            double cache[] = bbCache[id];
            for (IBGPreparedSQLStatement qinfo : qinfos) {
                double bb = finder.bestBenefit(qinfo.getIndexBenefitGraph(), id, M);
                cache[q] = bb;
                ben += bb;
                ++q;
            }
            bbSumCache[id] = ben;
            prevM[id].set(M); 
        }

        private void reinitIncremental(int id, IndexBitSet M, int b) {
            int q = 0;
            double ben = 0;
            double cache[] = bbCache[id];
            for (IBGPreparedSQLStatement qinfo : qinfos) {
                if (componentId[q][id] == componentId[q][b]) {
                    // interaction, recompute
                    double bb = finder.bestBenefit(qinfo.getIndexBenefitGraph(), id, M);
                    cache[q] = bb;
                    ben += bb;
                }
                else 
                    ben += cache[q];
                ++q;
            }
            prevM[id].set(M);
            bbSumCache[id] = ben;
        }

        @Override
        public double benefit(Index a, IndexBitSet M) {
            int id = a.getId();
            if (!M.equals(prevM)) {
                diffM.set(M);
                diffM.xor(prevM[id]);
                if (diffM.cardinality() == 1) {
                    reinitIncremental(id, M, diffM.nextSetBit(0));
                }
                else {
                    reinit(id, M);
                }
            }
            return bbSumCache[id];
        }

        @Override
        public double doi(Index a, Index b) {
            throw new RuntimeException("Not implemented");
        }
    }
}
