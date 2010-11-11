package edu.ucsc.satuning.engine.selection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.ucsc.satuning.Configuration;
import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.engine.ProfiledQuery;
import edu.ucsc.satuning.engine.CandidatePool.Snapshot;
import edu.ucsc.satuning.util.BitSet;
import edu.ucsc.satuning.util.Debug;

public class WorkFunctionAlgorithm<I extends DBIndex<I>> {
	TotalWorkValues wf = new TotalWorkValues();
	SubMachineArray<I> submachines = new SubMachineArray<I>(0);
	
	// for tracking history
	private boolean keepHistory;
	private WfaTrace<I> trace;
	
	//
	// temporary workspace
	//
	private TotalWorkValues wf2 = new TotalWorkValues();
	private CostVector tempCostVector = new CostVector();
	private BitSet tempBitSet = new BitSet();

	public WorkFunctionAlgorithm(IndexPartitions<I> parts, boolean keepHistory0) {
		if (parts != null) {
			repartition(parts);
			if (keepHistory0) {
				trace = new WfaTrace<I>(parts, wf);
			}
			keepHistory = keepHistory0;
		}
		else {
			Debug.assertion(!keepHistory0, "may only keep history if given the initial partition");
			keepHistory = false;
		}
		
		
		dump("INITIAL");
	}
	
	public WorkFunctionAlgorithm(IndexPartitions<I> parts) {
		this(parts, false);
	}
	
	public WorkFunctionAlgorithm() {
		this(null);
	}
	
	public void dump(String msg) {
		Debug.println(msg);
		for (int i = 0; i < submachines.length; i++) {
			Debug.println("SUBMACHINE " + i);
			submachines.get(i).dump(wf);
			Debug.println();
		}
		Debug.println("----");
	}
	
	public void newTask(ProfiledQuery<I> qinfo) {
		tempBitSet.clear(); // just to be safe
		
		for (int subsetNum = 0; subsetNum < submachines.length; subsetNum++) {
			SubMachine<I> subm = submachines.get(subsetNum);
			
			// preprocess cost into a vector
			for (int stateNum = 0; stateNum < subm.numStates; stateNum++) {
				// this will explicitly set each index in the array to 1 or 0
				setStateBits(subm.indexIds, stateNum, tempBitSet);
				double queryCost = qinfo.totalCost(tempBitSet);
				tempCostVector.set(stateNum, queryCost);
			}

			// clear all indexes in the array
			clearStateBits(subm.indexIds, tempBitSet);
			
			// run the task through the submachine
			subm.newTask(tempCostVector, wf, wf2);
		}
		
		// all submachines have assigned the new values into wf2
		// swap wf and wf2
		{ TotalWorkValues wfTemp = wf; wf = wf2; wf2 = wfTemp; }
		
		// keep trace info
		if (keepHistory) {
			double nullCost = qinfo.ibg.emptyCost();
			trace.addValues(wf, nullCost);
		}
		
		dump("NEW TASK");
	}

	public void vote(I index, boolean isPositive) {
		Debug.assertion(!keepHistory, "tracing WFA is not supported with user feedback");
		for (SubMachine<I> subm : submachines) 
			if (subm.subset.contains(index))
				subm.vote(wf, index, isPositive);

		dump("VOTE " + (isPositive ? "POSITIVE " : "NEGATIVE ") + "for " + index.internalId());
	}

	public List<I> getRecommendation() {
		ArrayList<I> rec = new ArrayList<I>(Configuration.maxHotSetSize);
		for (SubMachine<I> subm : submachines) {
			for (I index : subm.subset)
				if (subm.currentBitSet.get(index.internalId()))
					rec.add(index);
		}
		return rec;
	}

	public void repartition(IndexPartitions<I> newPartitions) {
		Debug.assertion(!keepHistory, "tracing WFA is not supported with repartitioning");
		int newSubsetCount = newPartitions.subsetCount();
		int oldSubsetCount = submachines.length;
		SubMachineArray<I> submachines2 = new SubMachineArray<I>(newSubsetCount);
		BitSet overlappingSubsets = new BitSet();
		
		// get the set of previously hot indexes
		BitSet oldHotSet = new BitSet();
		for (SubMachine<I> oldSubmachine : submachines) {
			for (int oldIndexId : oldSubmachine.indexIds) {
				oldHotSet.set(oldIndexId);
			}
		}
		
		// prepare wf2 with new partitioning
		wf2.reallocate(newPartitions);
		
		for (int newSubsetNum = 0; newSubsetNum < newSubsetCount; newSubsetNum++) {
			IndexPartitions<I>.Subset newSubset = newPartitions.get(newSubsetNum);
			
			// translate old recommendation into new
			BitSet recBitSet = new BitSet();
			int recStateNum = 0;
			int i = 0;
			for (I index : newSubset) {
				if (isRecommended(index)) {
					recBitSet.set(index.internalId());
					recStateNum |= (1 << i);
				}
				++i;
			}
			SubMachine<I> newSubmachine = new SubMachine<I>(newSubset, newSubsetNum, recStateNum, recBitSet);
			submachines2.set(newSubsetNum, newSubmachine);

			// find overlapping subsets (required to recompute work function)
			overlappingSubsets.clear();
			for (int oldSubsetNum = 0; oldSubsetNum < submachines.length; oldSubsetNum++) {
				if (newSubset.overlaps(submachines.get(oldSubsetNum).subset)) {
					overlappingSubsets.set(oldSubsetNum);
				}
			}
			
			// recompute work function values
			for (int stateNum = 0; stateNum < newSubmachine.numStates; stateNum++) {
				double value = 0;
				
				// add creation cost of new indexes
				i = 0;
				for (I index : newSubmachine.subset) {
					int mask = (1 << (i++));
					if (0 != (stateNum & mask) && !oldHotSet.get(index.internalId()))
						value += index.creationCost();
				}
				
				for (int oldSubsetNum = 0; oldSubsetNum < oldSubsetCount; oldSubsetNum++) {
					if (!overlappingSubsets.get(oldSubsetNum))
						continue;
					
					int[] oldIndexIds = submachines.get(oldSubsetNum).indexIds;
					int oldStateNum = 0;
					for (int i_old = 0; i_old < oldIndexIds.length; i_old++) {
						int i_new = newSubmachine.indexPos(oldIndexIds[i_old]);
						if (i_new < 0)
							continue;
						if ((stateNum & (1 << i_new)) != 0)
							oldStateNum |= (1 << i_old);
					}
					value += wf.get(oldSubsetNum, oldStateNum);
				}
				
				wf2.set(newSubsetNum, stateNum, value, 0); // XXX: we don't recompute the predecessor during repartitioning, but it is feasible
			}
		}
		
		// submachines2 now reflects the new partitioning 
		// wf2 now has the new workfunction values for submachines2
		
		submachines = submachines2; // start using new subsets
		wf.reallocate(wf2); // copy wf2 into wf (also changes the implicit partitioning within wf)
		dump("REPARTITION");
	}

	static void setStateBits(int[] ids, int stateNum, BitSet bitSet) {
		for (int i = 0; i < ids.length; i++)
			bitSet.set(ids[i], 0 != (stateNum & (1 << i)));
	}

	private static void clearStateBits(int[] ids, BitSet bitSet) {
		for (int i = 0; i < ids.length; i++)
			bitSet.clear(ids[i]);
	}
	
	private boolean isRecommended(I idx) {
		// not sure which submachine has the index, so check them all
		for (SubMachine<I> subm : submachines) {
			if (subm.currentBitSet.get(idx.internalId()))
				return true;
		}
		return false;
	}
	
	public static <J extends DBIndex<J>> double transitionCost(Snapshot<J> candidateSet, BitSet x, BitSet y) {
		double transition = 0;
		for (J index : candidateSet) {
			int id = index.internalId();
			if (y.get(id) && !x.get(id))
				transition += index.creationCost();
		}
		return transition;
	}

	private static <J extends DBIndex<J>> double transitionCost(IndexPartitions<J>.Subset subset, int x, int y) {
		double transition = 0;
		int i = 0;
		for (J index : subset) {
			int mask = 1 << (i++);
			
			if (mask == (y & mask) - (x & mask))
				transition += index.creationCost();
		}
		return transition;
	}
	
	// return a new copy of the current work function
	// DEPRECATED
//	public TotalWorkValues getTotalWorkValues() {
//		return new TotalWorkValues(wf);
//	}
	
	private static class SubMachineArray<J extends DBIndex<J>> implements Iterable<SubMachine<J>> {
		public final int length;
		private final ArrayList<SubMachine<J>> arr;
		
		public SubMachineArray(int len0) {
			length = len0;
			arr = new ArrayList<SubMachine<J>>(len0);
			for (int i = 0; i < len0; i++) 
				arr.add(null);
		}

		public SubMachine<J> get(int i) {
			return arr.get(i);
		}
		
		public void set(int i, SubMachine<J> subm) {
			arr.set(i, subm);
		}

		public Iterator<SubMachine<J>> iterator() {
			return arr.iterator();
		}
		
	}
	
	private static class SubMachine<J extends DBIndex<J>> implements Iterable<J> {
		private IndexPartitions<J>.Subset subset;
		private int subsetNum;
		private int numIndexes;
		private int numStates;
		private int currentState;
		private BitSet currentBitSet;
		private int[] indexIds;
		
		SubMachine(IndexPartitions<J>.Subset subset0, int subsetNum0, int state0, BitSet bs0) {
			subset = subset0;
			subsetNum = subsetNum0;
			numIndexes = subset0.size();
			numStates = 1 << numIndexes;
			currentState = state0;
			currentBitSet = bs0;
			
			indexIds = new int[numIndexes];
			int i = 0;
			for (J index : subset0) {
				indexIds[i++] = index.internalId();
			}
		}
		
		// return position of id in indexIds if exists, else -1
		public int indexPos(int id) {
			for (int i = 0; i < numIndexes; i++)
				if (indexIds[i] == id)
					return i;
			return -1;
		}
		
		public void dump(TotalWorkValues wf) {
			Debug.print("Index IDs : [ ");
			for (int id : indexIds) Debug.print(id + " ");
			Debug.print("]   ");

			Debug.print("REC : [ ");
			for (int id : indexIds) if (currentBitSet.get(id)) Debug.print(id + " ");
			Debug.println("]");
			
//			Debug.println("Current workfunction values ...");
//			for (int s = 0; s < numStates; s++) {
//				Debug.print("   [ ");
//				for (int i = 0; i < indexIds.length; i++) {
//					String id = "" + indexIds[i];
//					if (((s >> i) & 1) == 1)
//						Debug.print(id);
//					else 
//						for (int k = 0; k < id.length(); k++) Debug.print(" ");
//					Debug.print(" ");
//				}
//				Debug.println("] = " + wf.get(subsetNum, s));
//			}
		}
		
		// process a positive or negative vote for the index
		// do the necessary bookkeeping in the input workfunction, and update the current state
		public void vote(TotalWorkValues wf, J index, boolean isPositive) {
			// find the position in indexIds
			int indexIdsPos;
			int stateMask;
			for (indexIdsPos = 0; indexIdsPos < numIndexes; indexIdsPos++) 
				if (indexIds[indexIdsPos] == index.internalId())
					break;
			if (indexIdsPos >= numIndexes) {
				Debug.logError("could not process vote: index not found in subset");
				return;
			}
			
			// integer with a 1 in the position of the index
			stateMask = (1 << indexIdsPos);
					
			// register the vote in the recommendation
			if (isPositive) {
				currentBitSet.set(index.internalId());
				currentState |= stateMask;
			}
			else {
				currentBitSet.clear(index.internalId());
				currentState ^= stateMask;
			}
			
			// register the vote in the work function
			double minScore = wf.get(subsetNum, currentState) + index.creationCost();
			for (int stateNum = 0; stateNum < numStates; stateNum++) {
				boolean stateContainsIndex = stateMask == (stateMask & stateNum);
				if (isPositive != stateContainsIndex) {
					// the state is not consistent
					// we require wf + trans >= minScore
					// equivalently, wf >= minScore - trans
					double minWorkFunction = minScore - transitionCost(subset, stateNum, currentState);
					if (wf.get(subsetNum, stateNum) < minWorkFunction) {
						wf.set(subsetNum, stateNum, minWorkFunction, wf.predecessor(subsetNum, stateNum));
					}
				}
			}
		}

		// do the bookkeeping for a new task
		// the current work function values are given by wfOld
		// this function assigns the new work function values into wfNew, but of course
		// only the states within this submachine are handled
		void newTask(CostVector cost, TotalWorkValues wfOld, TotalWorkValues wfNew) {
			// compute new work function
			for (int newStateNum = 0; newStateNum < numStates; newStateNum++) {
				// compute one value of the work function
				double wfValueBest = Double.POSITIVE_INFINITY;
				int bestPredecessor = -1;
				for (int oldStateNum = 0; oldStateNum < numStates; oldStateNum++) {
					double wfValueOld = wfOld.get(subsetNum, oldStateNum);
					double queryCost = cost.get(oldStateNum);
					double transition = transitionCost(subset, oldStateNum, newStateNum);
					double wfValueNew = wfValueOld + queryCost + transition;
					if (wfValueNew < wfValueBest) {
						wfValueBest = wfValueNew;
						bestPredecessor = oldStateNum;
					}
				}
				if (Double.isInfinite(wfValueBest))
					Debug.logError("failed to compute work function");
				wfNew.set(subsetNum, newStateNum, wfValueBest, bestPredecessor);
			}
			
			// wfNew now contains the updated work function
			
			int bestState = -1;
			double bestValue = Double.POSITIVE_INFINITY;
			double bestTransitionCost = Double.POSITIVE_INFINITY;
			for (int stateNum = 0; stateNum < numStates; stateNum++) {
				
				// check the extra condition
				// this says that the optimal path ending in state i actually processes 
				// the task in state i
				if (wfNew.get(subsetNum, stateNum) != wfOld.get(subsetNum, stateNum) + cost.get(stateNum))
					continue;

				double transition = transitionCost(subset, stateNum, currentState);
				double value = wfNew.get(subsetNum, stateNum) + transition;
				
				// switch if value is better
				// if it's a tie, go with the state with lower transition cost
				// if that's a tie, give preference to currentState (which always has transition == 0)
				// the condition is written in a redundant way on purpose
				if (value < bestValue
						|| (value == bestValue && transition < bestTransitionCost)
						|| (value == bestValue && transition == bestTransitionCost && stateNum == currentState)) {
					bestState = stateNum;
					bestValue = value;
					bestTransitionCost = transition;
				}
			}
			
			if (bestState < 0)
				throw new AssertionError("failed to compute best state");
			
			currentState = bestState;
			setStateBits(indexIds, currentState, currentBitSet);
		}

		public Iterator<J> iterator() {
			return subset.iterator();
		}
	}
	
	public static class TotalWorkValues {
		double[] values;
		int[] predecessor;
		int[] subsetStart; 

		TotalWorkValues() {
			values = new double[Configuration.maxNumStates];
			subsetStart = new int[Configuration.maxHotSetSize]; 
			predecessor = new int[Configuration.maxNumStates];
		}

		TotalWorkValues(TotalWorkValues wf2) {
			values = wf2.values.clone();
			subsetStart = wf2.subsetStart.clone();
			predecessor = wf2.predecessor.clone();
		}
		
		double get(int subsetNum, int stateNum) {
			int i = subsetStart[subsetNum] + stateNum;
			return values[i];
		}
		
		int predecessor(int subsetNum, int stateNum) {
			int i = subsetStart[subsetNum] + stateNum;
			return predecessor[i];
		}
			

		void set(int subsetNum, int stateNum, double wfBest, int p) {
			int i = subsetStart[subsetNum] + stateNum;
			values[i] = wfBest;
			predecessor[i] = p;
		}

		void reallocate(IndexPartitions<?> newPartitions) {
			int newValueCount = newPartitions.wfaStateCount();
			if (newValueCount > values.length) {
				values = new double[newValueCount];
				predecessor = new int[newValueCount];
			}
			
			int newSubsetCount = newPartitions.subsetCount();
			if (newSubsetCount > subsetStart.length)
				subsetStart = new int[newSubsetCount];
			
			int start = 0;
			for (int subsetNum = 0; subsetNum < newSubsetCount; subsetNum++) {
				subsetStart[subsetNum] = start;
				start += (1 << newPartitions.get(subsetNum).size());
			}
		}

		void reallocate(TotalWorkValues wf2) {
			int newValueCount = wf2.values.length;
			if (newValueCount > values.length) {
				values = new double[newValueCount];
				predecessor = new int[newValueCount];
			}
			
			int newSubsetCount = wf2.subsetStart.length;
			if (newSubsetCount > subsetStart.length)
				subsetStart = new int[newSubsetCount];
			
			for (int subsetNum = 0; subsetNum < newSubsetCount; subsetNum++) {
				subsetStart[subsetNum] = wf2.subsetStart[subsetNum];
			}
			
			for (int i = 0; i < newValueCount; i++) {
				values[i] = wf2.values[i];
				predecessor[i] = wf2.predecessor[i];
			}
		}
	}
	
	private static class CostVector {
		private double[] vector;
		private int cap;
		
		CostVector() {
			cap = Configuration.maxNumStates;
			vector = new double[cap];
		}
		
		final double get(int i) {
			return vector[i];
		}
		
		final void set(int i, double val) {
			if (i >= cap) grow(i+1);
			vector[i] = val;
		}
		
		final void grow(int minSize) {
			int newCap = minSize + vector.length;
			double[] newVector = new double[newCap];
			for (int i = 0; i < cap; i++) {
				newVector[i] = vector[i];
			}
			vector = newVector;
			cap = newCap;
		}
	}
//
//	public java.util.Iterator<DB2Index> iterator() {
//		return new IndexIterator();
//	}
//	
//	private class IndexIterator implements java.util.Iterator<DB2Index> {
//		// if not null, this iterator has a next element, which is what next() will return
//		java.util.Iterator<DB2Index> subIterator;
//		
//		// the subset that subIterator is over
//		int subsetNum;
//		
//		public IndexIterator() {
//			subIterator = null;
//			for (subsetNum = 0; subsetNum < submachines.length; subsetNum++) {
//				if (submachines[subsetNum].subset.size() > 0) {
//					subIterator = submachines[subsetNum].subset.iterator();
//					break;
//				}
//			}
//		}
//		
//		
//		public boolean hasNext() {
//			return subIterator != null;
//		}
//
//		public DB2Index next() {
//			if (subIterator == null)
//				throw new java.util.NoSuchElementException();
//			
//			DB2Index nextIndex = subIterator.next();
//			if (!subIterator.hasNext()) {
//				subIterator = null;
//				for (subsetNum++; subsetNum < submachines.length; subsetNum++) {
//					if (submachines[subsetNum].subset.size() > 0) {
//						subIterator = submachines[subsetNum].subset.iterator();
//						break;
//					}
//				}
//			}
//			return nextIndex;
//		}
//
//		public void remove() {
//			throw new UnsupportedOperationException();
//		}
//	};
	
	// given a schedule over a set of candidates and queries, get the total work
	public <J extends DBIndex<J>> double 
	getScheduleCost(Snapshot<J> candidateSet, int queryCount, ArrayList<ProfiledQuery<J>> qinfos, IndexPartitions<J> parts, BitSet[] schedule) {
		double cost = 0;
		BitSet prevState = new BitSet();
		BitSet subset = new BitSet();
		for (int q = 0; q < queryCount; q++) {
			BitSet state = schedule[q];
			if (parts != null)
				cost += parts.theoreticalCost(qinfos.get(q), state, subset);
			else
				cost += qinfos.get(q).planCost(state);
			
			cost += qinfos.get(q).maintenanceCost(state);
			cost += transitionCost(candidateSet, prevState, state);
			prevState = state;
		}
		return cost;
	}

	public WfaTrace<I> getTrace() {
		return trace;
	}
}
