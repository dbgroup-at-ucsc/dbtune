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
package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;

import edu.ucsc.dbtune.ibg.IBGBestBenefitFinder;
import edu.ucsc.dbtune.ibg.InteractionBank;
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
    List<ProfiledQuery<DBIndex>> qinfos;
    List<Double> overheads;
    List<IndexBitSet> configurations;
    WorkloadProfiler<DBIndex> profiler;
    Snapshot<DBIndex>      snapshot;
    KarlsIndexPartitions<DBIndex> partitions;
    KarlsWorkFunctionAlgorithm<DBIndex> wfa;

    int maxNumIndexes;
    int maxNumStates;

    /**
     */
    public WFIT(
            DatabaseConnection con,
            CandidatePool<DBIndex> pool,
            int maxNumIndexes,
            int maxNumStates)
    {
        this.snapshot      = pool.getSnapshot();
        this.maxNumIndexes = maxNumIndexes;
        this.maxNumStates  = maxNumStates;

        profiler       = new WorkloadProfilerImpl<DBIndex>(con, pool, false);
        qinfos         = new ArrayList<ProfiledQuery<DBIndex>>();
        wfa            = new KarlsWorkFunctionAlgorithm<DBIndex>(partitions, false);
        overheads      = new ArrayList<Double>();
        configurations = new ArrayList<IndexBitSet>();
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
        ProfiledQuery<DBIndex> qinfo;
        IndexBitSet configuration;

        qinfo = profiler.processQuery(sql.getSQL());

        qinfos.add(qinfo);

        partitions = getIndexPartitions(snapshot, qinfos, maxNumIndexes, maxNumStates);

        wfa.repartition(partitions);
        wfa.newTask(qinfo);

        configuration = new IndexBitSet();

        for (DBIndex idx : wfa.getRecommendation()) {
            configuration.set(idx.internalId());
        }

        configurations.add(configuration);
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
    public IndexBitSet getRecommendation() throws SQLException
    {
        return configurations.get(qinfos.size()-1);
    }

    public ProfiledQuery<DBIndex> getProfiledQuery(int i) {
        return qinfos.get(i);
    }
    public KarlsIndexPartitions<DBIndex> getPartitions() {
        return partitions;
    }

    private KarlsIndexPartitions<DBIndex> getIndexPartitions(
            Snapshot<DBIndex>            candidateSet,
            List<ProfiledQuery<DBIndex>> qinfos,
            int                          maxNumIndexes,
            int                          maxNumStates )
    {
        final StaticIndexSet<DBIndex> hotSet = KarlsHotsetSelector.chooseHotSet(
                candidateSet, new StaticIndexSet<DBIndex>(), new DynamicIndexSet<DBIndex>(),
                new TempBenefitFunction<DBIndex>(qinfos, candidateSet.maxInternalId()),
                maxNumIndexes, false
                );

        return KarlsInteractionSelector.choosePartitions(
                hotSet,
                new KarlsIndexPartitions<DBIndex>(hotSet),
                new TempDoiFunction<DBIndex>(qinfos, candidateSet),
                maxNumStates
                );
    }

    private static class TempBenefitFunction<I extends DBIndex> implements BenefitFunction<I> {
		IBGBestBenefitFinder finder = new IBGBestBenefitFinder();
		double[][] bbCache;
		double[] bbSumCache;
		int[][] componentId;
		IndexBitSet[] prevM;
		IndexBitSet diffM;
		List<ProfiledQuery<I>> qinfos;

		TempBenefitFunction(List<ProfiledQuery<I>> qinfos0, int maxInternalId) {
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

		private static <I extends DBIndex> int[][] componentIds(List<ProfiledQuery<I>> qinfos, int maxInternalId) {
			int[][] componentId = new int[qinfos.size()][maxInternalId+1];
			int q = 0;
			for (ProfiledQuery<I> qinfo : qinfos) {
				IndexBitSet[] parts = qinfo.getInteractionBank().stablePartitioning(0);
				for (I index : qinfo.getCandidateSnapshot()) {
					int id = index.internalId();
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
			for (ProfiledQuery<I> qinfo : qinfos) {
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
			for (ProfiledQuery<I> qinfo : qinfos) {
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
		public double apply(I a, IndexBitSet M) {
			int id = a.internalId();
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
	}

    private static class TempDoiFunction<I extends DBIndex> implements DoiFunction<I> {
		private InteractionBank bank;
		TempDoiFunction(List<ProfiledQuery<I>> qinfos, Snapshot<I> candidateSet) {
			bank = new InteractionBank(candidateSet);
			for (I a : candidateSet) {
				int id_a = a.internalId();
				for (I b : candidateSet) {
					int id_b = b.internalId();
					if (id_a < id_b) {
						double doi = 0;
						for (ProfiledQuery<I> qinfo : qinfos) {
							doi += qinfo.getInteractionBank().interactionLevel(a.internalId(), b.internalId());
						}
						bank.assignInteraction(a.internalId(), b.internalId(), doi);
					}
				}
			}
		}

        @Override
		public double apply(I a, I b) {
			return bank.interactionLevel(a.internalId(), b.internalId());
		}
	}
}
