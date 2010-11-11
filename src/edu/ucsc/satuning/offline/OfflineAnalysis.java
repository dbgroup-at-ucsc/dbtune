package edu.ucsc.satuning.offline;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.db.DatabaseConnection;
import edu.ucsc.satuning.util.DBUtilities;
import edu.ucsc.satuning.engine.CandidatePool;
import edu.ucsc.satuning.engine.ProfiledQuery;
import edu.ucsc.satuning.engine.CandidatePool.Snapshot;
import edu.ucsc.satuning.engine.profiling.Profiler;
import edu.ucsc.satuning.engine.selection.BenefitFunction;
import edu.ucsc.satuning.engine.selection.DoiFunction;
import edu.ucsc.satuning.engine.selection.DynamicIndexSet;
import edu.ucsc.satuning.engine.selection.HotSetSelector;
import edu.ucsc.satuning.engine.selection.IndexPartitions;
import edu.ucsc.satuning.engine.selection.InteractionSelector;
import edu.ucsc.satuning.engine.selection.StaticIndexSet;
import edu.ucsc.satuning.ibg.IBGBestBenefitFinder;
import edu.ucsc.satuning.ibg.log.InteractionBank;
import edu.ucsc.satuning.util.BitSet;
import edu.ucsc.satuning.util.Files;

public class OfflineAnalysis {
	public static <I extends DBIndex<I>> CandidatePool<I> 
	getCandidates(DatabaseConnection<I> conn, File workloadFile) throws SQLException {
		// run the advisor to get the initial candidates
		// Give a budget of -1 to mean "no budget"
		Iterable<I> candidateSet = null;
		try {
			candidateSet = conn.getIndexExtractor().recommendIndexes(workloadFile);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		} 
		
		// add each candidate
		CandidatePool<I> pool = new CandidatePool<I>();
		for (I index : candidateSet) {
			pool.addIndex(index);
		}
		
		return pool;
	}
	
	public static <I extends DBIndex<I>> ArrayList<ProfiledQuery<I>> 
	profileQueries(DatabaseConnection<I> conn, File workloadFile, CandidatePool<I> pool) throws SQLException {
		Profiler<I> profiler = new Profiler<I>(conn, pool, false);
		
		// get an IBG etc for each statement
		java.util.List<String> lines = null;
		ArrayList<ProfiledQuery<I>> qinfos = null;
		try {
			lines = Files.getLines(workloadFile);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		qinfos = new ArrayList<ProfiledQuery<I>>();
		{
			for (String sql : lines) {
				qinfos.add(profiler.processQuery(DBUtilities.trimSqlStatement(sql)));
			}
		}
		
		return qinfos;
	}
	
	public static <I extends DBIndex<I>> IndexPartitions<I> 
	getPartition(Snapshot<I> candidateSet, ArrayList<ProfiledQuery<I>> qinfos, 
	             int maxNumIndexes, int maxNumStates) throws SQLException {		
		// get the hot set
		StaticIndexSet<I> hotSet = getHotSet(candidateSet, qinfos, maxNumIndexes);
		
		// partition the hot set
		DoiFunction<I> doiFunc = new TempDoiFunction<I>(qinfos, candidateSet);
		IndexPartitions<I> parts = InteractionSelector.<I>choosePartitions(hotSet,
																	 new IndexPartitions<I>(hotSet),
																	 doiFunc,
																	 maxNumStates);
		
		return parts;
	}
	
	public static <I extends DBIndex<I>> StaticIndexSet<I> 
	getHotSet(Snapshot<I> candidateSet, ArrayList<ProfiledQuery<I>> qinfos, int maxNumIndexes) {
		// get the hot set
		BenefitFunction<I> benefitFunc = new TempBenefitFunction<I>(qinfos, candidateSet.maxInternalId());
		StaticIndexSet<I> hotSet = HotSetSelector.<I>chooseHotSetGreedy(candidateSet, 
														    new StaticIndexSet<I>(),
														    new DynamicIndexSet<I>(),
														    benefitFunc,
														    maxNumIndexes,
														    false);
		return hotSet;
	}

	
	private static class TempBenefitFunction<I extends DBIndex<I>> implements BenefitFunction<I> {
		IBGBestBenefitFinder finder = new IBGBestBenefitFinder();
		double[][] bbCache;
		double[] bbSumCache;
		int[][] componentId;
		BitSet[] prevM;
		BitSet diffM;
		ArrayList<ProfiledQuery<I>> qinfos;
		
		TempBenefitFunction(ArrayList<ProfiledQuery<I>> qinfos0, int maxInternalId) {
			qinfos = qinfos0;
			
			componentId = componentIds(qinfos0, maxInternalId);
			
			bbCache = new double[maxInternalId+1][qinfos0.size()];
			bbSumCache = new double[maxInternalId+1];
			prevM = new BitSet[maxInternalId+1];
			for (int i = 0; i <= maxInternalId; i++) {
				prevM[i] = new BitSet();
				reinit(i, prevM[i]);
			}
			diffM = new BitSet(); // temp bit set
		}
		
		private static <I extends DBIndex<I>> int[][] componentIds(ArrayList<ProfiledQuery<I>> qinfos, int maxInternalId) {
			int[][] componentId = new int[qinfos.size()][maxInternalId+1];
			int q = 0;
			for (ProfiledQuery<I> qinfo : qinfos) {
				BitSet[] parts = qinfo.bank.stablePartitioning(0);
				for (I index : qinfo.candidateSet) {
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
		
		private void reinit(int id, BitSet M) {
			int q = 0;
			double ben = 0;
			double cache[] = bbCache[id];
			for (ProfiledQuery<I> qinfo : qinfos) {
				double bb = finder.bestBenefit(qinfo.ibg, id, M);
				cache[q] = bb;
				ben += bb;
				++q;
			}
			bbSumCache[id] = ben;
			prevM[id].set(M); 
		}
		
		private void reinitIncremental(int id, BitSet M, int b) {
			int q = 0;
			double ben = 0;
			double cache[] = bbCache[id];
			for (ProfiledQuery<I> qinfo : qinfos) {
				if (componentId[q][id] == componentId[q][b]) {
					// interaction, recompute
					double bb = finder.bestBenefit(qinfo.ibg, id, M);
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
		
		public double benefit(I a, BitSet M) {
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

	private static class TempDoiFunction<I extends DBIndex<I>> implements DoiFunction<I> {
		private InteractionBank bank;
		TempDoiFunction(ArrayList<ProfiledQuery<I>> qinfos, Snapshot<I> candidateSet) {
			bank = new InteractionBank(candidateSet);
			for (I a : candidateSet) {
				int id_a = a.internalId();
				for (I b : candidateSet) {
					int id_b = b.internalId();
					if (id_a < id_b) {
						double doi = 0;
						for (ProfiledQuery<I> qinfo : qinfos) {
							doi += qinfo.bank.interactionLevel(a.internalId(), b.internalId());
						}
						bank.assignInteraction(a.internalId(), b.internalId(), doi);
					}
				}
			}
		}
		
		public double doi(I a, I b) {
			return bank.interactionLevel(a.internalId(), b.internalId());
		}
	}
}
