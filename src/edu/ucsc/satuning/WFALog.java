package edu.ucsc.satuning;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.LinkedList;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.engine.AnalyzedQuery;
import edu.ucsc.satuning.engine.ProfiledQuery;
import edu.ucsc.satuning.engine.CandidatePool.Snapshot;
import edu.ucsc.satuning.engine.selection.IndexPartitions;
import edu.ucsc.satuning.engine.selection.WorkFunctionAlgorithm;
import edu.ucsc.satuning.util.BitSet;

public class WFALog implements Serializable {
	private static final long serialVersionUID = 3L;
	
	private LinkedList<Entry> list = new LinkedList<Entry>();
	
	WFALog() {
	}
	
	public class Entry implements Serializable {
		private static final long serialVersionUID = WFALog.serialVersionUID;
		double planCost;
		double maintenanceCost;
		double transitionCost;
		double nullCost;
		BitSet[] partition;
		BitSet recommendation;
		int numCandidates;
		int numWhatif;
		double logicOverhead;
	}


	public void add(AnalyzedQuery<?> qinfo, BitSet recommendation, double planCost, double maintCost, double transitionCost, double overhead) {
		Entry e = new Entry();
		e.planCost = planCost;
		e.maintenanceCost = maintCost;
		e.transitionCost = transitionCost;
		e.nullCost = qinfo.profileInfo.ibg.emptyCost();
		e.partition = qinfo.partition;
		e.recommendation = recommendation;
		e.numCandidates = qinfo.profileInfo.candidateSet.maxInternalId() + 1;
		e.numWhatif = qinfo.profileInfo.whatifCount;
		e.logicOverhead = overhead + qinfo.profileInfo.ibgAnalysisTime;
		list.add(e);
	}

	public void add(ProfiledQuery<?> qinfo, 
			        BitSet[] partition, BitSet recommendation,
			        double planCost, double maintCost, double transitionCost, int whatifCount, double overhead) {
		Entry e = new Entry();
		e.planCost = planCost;
		e.maintenanceCost = maintCost;
		e.transitionCost = transitionCost;
		e.nullCost = qinfo.ibg.emptyCost();
		e.partition = partition;
		e.recommendation = recommendation;
		e.numCandidates = qinfo.candidateSet.maxInternalId() + 1;
		e.numWhatif = whatifCount;
		e.logicOverhead = overhead + qinfo.ibgAnalysisTime;
		list.add(e);
	}


	public void dump() {
		int i = 0;
		int c = 0;
		int w = 0;
		for (Entry e : list) {
			System.out.println("QUERY " + i);
			System.out.println("   "+"pcost\t"+e.planCost+"\t"
									+"mcost\t"+e.maintenanceCost+"\t"
					                +"tcost\t"+e.transitionCost+"\t"
					                +"ncost\t"+e.nullCost+"\t"
					                +"cands\t"+(e.numCandidates-c)+"\t"
					                +"whatif\t"+(e.numWhatif-w)+"\t"
					                +"ohead\t"+e.logicOverhead);
			System.out.print("   partitions: ");
			for (BitSet bs : e.partition) System.out.print(bs + " ");
			System.out.println();
			System.out.println("   rec: " + e.recommendation);
			System.out.println();
			
			c = e.numCandidates;
			w = e.numWhatif;
			++i;
		}
	}


	public void dumpPerformance(PrintStream out) {
		int w = 0;

		//out.println("qcost\ttcost\tncost\tcands\twhatif\t");
		
		for (Entry e : list) {
			out.println(e.planCost+"\t"+
							   e.maintenanceCost+"\t"+
					           e.transitionCost+"\t"+
					           //e.nullCost+"\t"+
					           (e.numWhatif-w)+"\t"+
					           e.logicOverhead+"\t"+
					           e.numCandidates);
			
			w = e.numWhatif;
		}
	}

	// print all pairs i,j where i is a query and j is recommended at step i
	public void dumpHistory(PrintStream out) {
		int i = 1;
		BitSet prev = new BitSet();
		for (Entry e : list) {
			BitSet bs = e.recommendation;
			for (int j = prev.nextSetBit(0); j >= 0; j = prev.nextSetBit(j+1)) {
				if (!bs.get(j)) out.println("Q"+i+"\tI"+j+"\t"+"\" ---drop \"");
			}
			for (int j = bs.nextSetBit(0); j >= 0; j = bs.nextSetBit(j+1)) {
				if (!prev.get(j)) out.println("Q"+i+"\tI"+j+"\t"+"\" +++ADD \"");
			}
			prev = bs;
			++i;
			//out.println();
		}
	}
	
	// write whole log for an experiment with a fixed candidate set
	public static <I extends DBIndex<I>> WFALog generateFixed(java.util.List<ProfiledQuery<I>> qinfos, BitSet[] recs, Snapshot<I> snapshot, IndexPartitions<I> parts, double[] overheads) {
		int queryCount = qinfos.size();
		WFALog log = new WFALog();
		BitSet scratch = new BitSet();

		BitSet[] partBitSets = parts.bitSetArray();
		for (int q = 0; q < queryCount; q++) {
			ProfiledQuery<I> qinfo = qinfos.get(q);
			BitSet state = recs[q];
			BitSet prevState = q == 0 ? new BitSet() : recs[q-1];
			double planCost;
			if (parts != null)
				planCost = parts.theoreticalCost(qinfo, state, scratch);
			else
				planCost = qinfo.planCost(state);
			double maintCost = qinfo.maintenanceCost(state);
			double transitionCost = WorkFunctionAlgorithm.transitionCost(snapshot, prevState, state);
			log.add(qinfo, partBitSets, state, planCost, maintCost, transitionCost, qinfo.whatifCount, overheads[q]);
		}
		
		return log;
	}

	public static <I extends DBIndex<I>> WFALog generateDual(java.util.List<ProfiledQuery<I>> qinfos, BitSet[] optRecs, BitSet[] wfitRecs, Snapshot<I> snapshot, IndexPartitions<I> parts, double[] overheads) {
		int queryCount = qinfos.size();
		WFALog log = new WFALog();

		BitSet scratch = new BitSet();
		BitSet emptyRec = new BitSet();
		BitSet indicesToCreate = new BitSet();
		BitSet indicesToDrop = new BitSet();
		BitSet materialized1 = new BitSet();
		BitSet materialized2 = new BitSet();
		BitSet materialized3 = new BitSet();
		BitSet[] partBitSets = parts.bitSetArray();
		for (int q = 0; q < queryCount; q++) {
			ProfiledQuery<I> qinfo = qinfos.get(q);

			materialized1.set(materialized3);
			
			// process OPT's recommendations
			BitSet optState = optRecs[q];
			BitSet prevOptState = q == 0 ? emptyRec : optRecs[q-1];
			indicesToCreate.set(optState);
			indicesToCreate.andNot(prevOptState);
			indicesToDrop.set(prevOptState);
			indicesToDrop.andNot(optState);
			materialized2.set(materialized1);
			materialized2.or(indicesToCreate);
			materialized2.andNot(indicesToDrop);
			
			// process WFIT's recommendations
			BitSet state = wfitRecs[q];
			BitSet prevState = q == 0 ? emptyRec : wfitRecs[q-1];
			indicesToCreate.set(state);
			indicesToCreate.andNot(prevState);
			indicesToDrop.set(prevState);
			indicesToDrop.andNot(state);
			materialized3.set(materialized2);
			materialized3.or(indicesToCreate);
			materialized3.andNot(indicesToDrop);
			
			double planCost;
			if (parts != null) {
				planCost = parts.theoreticalCost(qinfo, materialized3, scratch);
			}
			else {
				planCost = qinfo.planCost(materialized3);
			}
			double maintCost = qinfo.maintenanceCost(materialized3);
			double transitionCost = WorkFunctionAlgorithm.transitionCost(snapshot, materialized1, materialized2) +
			                        WorkFunctionAlgorithm.transitionCost(snapshot, materialized2, materialized3);
			log.add(qinfo, partBitSets, materialized3.clone(), planCost, maintCost, transitionCost, qinfo.whatifCount, overheads[q]);
		}
		
		return log;
	}

	public static <I extends DBIndex<I>> WFALog 
	generateDynamic(java.util.List<AnalyzedQuery<I>> qinfos, BitSet[] recs, double[] overheads) {
		int queryCount = qinfos.size();
		WFALog log = new WFALog();
		
		for (int q = 0; q < queryCount; q++) {
			AnalyzedQuery<I> qinfo = qinfos.get(q);
			BitSet state = recs[q];
			BitSet prevState = q == 0 ? new BitSet() : recs[q-1];
			double planCost = qinfo.profileInfo.planCost(state);
			double maintCost = qinfo.profileInfo.maintenanceCost(state);
			double transitionCost = WorkFunctionAlgorithm.transitionCost(qinfo.profileInfo.candidateSet, prevState, state);
			log.add(qinfo, state, planCost, maintCost, transitionCost, overheads[q]);
		}
		
		return log;
	}

	public int entryCount() {
		return list.size();
	}

	public Entry getEntry(int i) {
		return list.get(i);
	}

	public int countRepartitions() {
		Entry p = null;
		int c = 0;
		for (Entry e : list) {
			if (p != null && !partsEqual(p.partition, e.partition)) {
				c++;
				for (BitSet bs : e.partition)
					System.out.println(bs);
				System.out.println();
			}
			p = e;
		}
		return c;
	}
	
	private boolean partsEqual(BitSet[] arr1, BitSet[] arr2) {
		if (arr1.length != arr2.length) return false;
		
		for (BitSet bs1 : arr1) {
			boolean foundMatch = false;
			for (BitSet bs2 : arr2) {
				if (bs1.equals(bs2)) { foundMatch = true; break; }
			}
			if (!foundMatch) return false;
		}
		return true;
	}
}
