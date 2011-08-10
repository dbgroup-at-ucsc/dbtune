package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.IndexBitSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class KarlsWorkFunctionAlgorithm {
  private static final Environment ENV = Environment.getInstance();
  private static final int MAX_NUM_STATES   = ENV.getMaxNumStates();
  private static final int MAX_HOTSET_SIZE  = ENV.getMaxNumIndexes();

	TotalWorkValues wf = new TotalWorkValues();
	SubMachineArray submachines = new SubMachineArray(0);

	// for tracking history
	private boolean keepHistory;
	private WfaTrace trace;

	private TotalWorkValues wf2 = new TotalWorkValues();
	private CostVector tempCostVector = new CostVector();
	private IndexBitSet tempBitSet = new IndexBitSet();

	public KarlsWorkFunctionAlgorithm(KarlsIndexPartitions parts, boolean keepHistory0) {
		if (parts != null) {
			repartition(parts);
			if (keepHistory0) {
				trace = new WfaTrace(parts, wf);
			}
			keepHistory = keepHistory0;
		}
		else {
			Checks.checkAssertion(!keepHistory0,
          "may only keep history if given the initial partition");
			keepHistory = false;
		}


		dump("INITIAL");
	}

	public KarlsWorkFunctionAlgorithm(KarlsIndexPartitions parts) {
		this(parts, false);
	}

	public KarlsWorkFunctionAlgorithm() {
		this(null);
	}

	public void dump(String msg) {
		Console.streaming().log(msg);
		for (int i = 0; i < submachines.length; i++) {
			Console.streaming().log("SUBMACHINE " + i);
			submachines.get(i).dump(wf);
			Console.streaming().skip();
		}
		Console.streaming().log("----");
	}

	public void newTask(ProfiledQuery qinfo) {
		tempBitSet.clear(); // just to be safe

        System.out.println("Obtaining cost for subset " + qinfo.getSQL());

		for (int subsetNum = 0; subsetNum < submachines.length; subsetNum++) {
			SubMachine subm = submachines.get(subsetNum);

            System.out.println("Obtaining cost for subset " + subsetNum);

			// preprocess cost into a vector
			for (int stateNum = 0; stateNum < subm.numStates; stateNum++) {
				// this will explicitly set each index in the array to 1 or 0
				setStateBits(subm.indexIds, stateNum, tempBitSet);
				double queryCost = qinfo.totalCost(tempBitSet);
                System.out.println("  cost with state " + tempBitSet + ": " + queryCost);
				tempCostVector.set(stateNum, queryCost);
			}

            System.out.println("----");

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
			double nullCost = qinfo.getIndexBenefitGraph().emptyCost();
			trace.addValues(wf, nullCost);
		}

		dump("NEW TASK");
	}

	public void vote(Index index, boolean isPositive) {
		Checks.checkAssertion(!keepHistory, "tracing WFA is not supported with user feedback");
		for (SubMachine subm : submachines)
			if (subm.subset.contains(index))
				subm.vote(wf, index, isPositive);

		dump("VOTE " + (isPositive ? "POSITIVE " : "NEGATIVE ") + "for " + index.getId());
	}

	public List<Index> getRecommendation() {
		ArrayList<Index> rec = new ArrayList<Index>(MAX_HOTSET_SIZE);
		for (SubMachine subm : submachines) {
			for (Index index : subm.subset)
				if (subm.currentBitSet.get(index.getId()))
					rec.add(index);
		}
		return rec;
	}

	public void repartition(KarlsIndexPartitions newPartitions) {
		Checks.checkAssertion(!keepHistory, "tracing WFA is not supported with repartitioning");
		int newSubsetCount = newPartitions.subsetCount();
		int oldSubsetCount = submachines.length;
		SubMachineArray submachines2 = new SubMachineArray(newSubsetCount);
		IndexBitSet overlappingSubsets = new IndexBitSet();

		// get the set of previously hot indexes
		IndexBitSet oldHotSet = new IndexBitSet();
		for (SubMachine oldSubmachine : submachines) {
			for (int oldIndexId : oldSubmachine.indexIds) {
				oldHotSet.set(oldIndexId);
			}
		}

		// prepare wf2 with new partitioning
		wf2.reallocate(newPartitions);

		for (int newSubsetNum = 0; newSubsetNum < newSubsetCount; newSubsetNum++) {
			KarlsIndexPartitions.Subset newSubset = newPartitions.get(newSubsetNum);

			// translate old recommendation into new
			IndexBitSet recBitSet = new IndexBitSet();
			int recStateNum = 0;
			int i = 0;
			for (Index index : newSubset) {
				if (isRecommended(index)) {
					recBitSet.set(index.getId());
					recStateNum |= (1 << i);
				}
				++i;
			}
			SubMachine newSubmachine = new SubMachine(newSubset, newSubsetNum, recStateNum, recBitSet);
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
				for (Index index : newSubmachine.subset) {
					int mask = (1 << (i++));
					if (0 != (stateNum & mask) && !oldHotSet.get(index.getId()))
						value += index.getCreationCost();
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

				wf2.set(newSubsetNum, stateNum, value, 0); // note: we don't recompute the predecessor during repartitioning, but it is feasible
			}
		}

		// submachines2 now reflects the new partitioning
		// wf2 now has the new workfunction values for submachines2

		submachines = submachines2; // start using new subsets
		wf.reallocate(wf2); // copy wf2 into wf (also changes the implicit partitioning within wf)
		dump("REPARTITION");
	}

	static void setStateBits(int[] ids, int stateNum, IndexBitSet bitSet) {
		for (int i = 0; i < ids.length; i++)
			bitSet.set(ids[i], 0 != (stateNum & (1 << i)));
	}

	private static void clearStateBits(int[] ids, IndexBitSet bitSet) {
		for (int i = 0; i < ids.length; i++)
			bitSet.clear(ids[i]);
	}

	private boolean isRecommended(Index idx) {
		// not sure which submachine has the index, so check them all
		for (SubMachine subm : submachines) {
			if (subm.currentBitSet.get(idx.getId()))
				return true;
		}
		return false;
	}

	public static  double transitionCost(Snapshot candidateSet, IndexBitSet x, IndexBitSet y) {
		double transition = 0;
		for (Index index : candidateSet) {
			int id = index.getId();
			if (y.get(id) && !x.get(id))
				transition += index.getCreationCost();
		}
		return transition;
	}

	private static double transitionCost(KarlsIndexPartitions.Subset subset, int x, int y) {
		double transition = 0;
		int i = 0;
		for (Index index : subset) {
            System.out.println("Index " + index.getId());

			int mask = 1 << (i++);

			if (mask == (y & mask) - (x & mask)) {
                System.out.println("  cost " + index.getCreationCost());
				transition += index.getCreationCost();
            }
		}
		return transition;
	}

	// return a new copy of the current work function
	// DEPRECATED
//	public TotalWorkValues getTotalWorkValues() {
//		return new TotalWorkValues(wf);
//	}

	private static class SubMachineArray implements Iterable<SubMachine> {
		public final int length;
		private final ArrayList<SubMachine> arr;

		public SubMachineArray(int len0) {
			length = len0;
			arr = new ArrayList<SubMachine>(len0);
			for (int i = 0; i < len0; i++)
				arr.add(null);
		}

		public SubMachine get(int i) {
			return arr.get(i);
		}

		public void set(int i, SubMachine subm) {
			arr.set(i, subm);
		}

		public Iterator<SubMachine> iterator() {
			return arr.iterator();
		}

	}

	private static class SubMachine implements Iterable<Index> {
		private KarlsIndexPartitions.Subset subset;
		private int subsetNum;
		private int numIndexes;
		private int numStates;
		private int currentState;
		private IndexBitSet currentBitSet;
		private int[] indexIds;

		SubMachine(KarlsIndexPartitions.Subset subset0, int subsetNum0, int state0, IndexBitSet bs0) {
			subset = subset0;
			subsetNum = subsetNum0;
			numIndexes = subset0.size();
			numStates = 1 << numIndexes;
			currentState = state0;
			currentBitSet = bs0;

			indexIds = new int[numIndexes];
			int i = 0;
			for (Index index : subset0) {
				indexIds[i++] = index.getId();
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
      final StringBuilder message = new StringBuilder();
      message.append("Index IDs : [ ");
      for (int id : indexIds) message.append(id).append(" ");
      message.append("]   ").append("REC : [ ");

      for (int id : indexIds) if (currentBitSet.get(id)) message.append(id).append(" ");
      message.append("]");
      Console.streaming().log(message.toString());
		}

		// process a positive or negative vote for the index
		// do the necessary bookkeeping in the input workfunction, and update the current state
		public void vote(TotalWorkValues wf, Index index, boolean isPositive) {
			// find the position in indexIds
			int indexIdsPos;
			int stateMask;
			for (indexIdsPos = 0; indexIdsPos < numIndexes; indexIdsPos++)
				if (indexIds[indexIdsPos] == index.getId())
					break;
			if (indexIdsPos >= numIndexes) {
				Console.streaming().error("could not process vote: index not found in subset");
				return;
			}

			// integer with a 1 in the position of the index
			stateMask = (1 << indexIdsPos);

			// register the vote in the recommendation
			if (isPositive) {
				currentBitSet.set(index.getId());
				currentState |= stateMask;
			}
			else {
				currentBitSet.clear(index.getId());
				currentState ^= stateMask;
			}

			// register the vote in the work function
			double minScore = wf.get(subsetNum, currentState) + index.getCreationCost();
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
            System.out.println("Num of states: " + numStates);
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

                    System.out.println("State: " + newStateNum + "; cost: " + queryCost + "; transCost: " + transition);

					if (wfValueNew < wfValueBest) {
						wfValueBest = wfValueNew;
						bestPredecessor = oldStateNum;
					}
				}
				if (Double.isInfinite(wfValueBest))
					Console.streaming().error("failed to compute work function");
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

                System.out.println("Transition: " + transition);
                System.out.println("Value: " + value);

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
                    System.out.println("New best: " + bestState);
				}
                System.out.println("Just checking");
			}

			if (bestState < 0)
				throw new AssertionError("failed to compute best state");

			currentState = bestState;
			setStateBits(indexIds, currentState, currentBitSet);
		}

		public Iterator<Index> iterator() {
			return subset.iterator();
		}
	}

	public static class TotalWorkValues {
		double[] values;
		int[] predecessor;
		int[] subsetStart;

		TotalWorkValues() {
			values = new double[MAX_NUM_STATES];
			subsetStart = new int[MAX_HOTSET_SIZE];
			predecessor = new int[MAX_NUM_STATES];
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

		void reallocate(KarlsIndexPartitions newPartitions) {
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

        public String toString() {
            return "values: " + Arrays.toString(values) + " ; ";
        }
	}

	private static class CostVector {
		private double[] vector;
		private int cap;

		CostVector() {
			cap = MAX_NUM_STATES;
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


	// given a schedule over a set of candidates and queries, get the total work
	public  double
	getScheduleCost(Snapshot candidateSet, int queryCount, List<ProfiledQuery> qinfos, KarlsIndexPartitions parts, IndexBitSet[] schedule) {
		double cost = 0;
		IndexBitSet prevState = new IndexBitSet();
		IndexBitSet subset = new IndexBitSet();
		for (int q = 0; q < queryCount; q++) {
			IndexBitSet state = schedule[q];
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

	public WfaTrace getTrace() {
		return trace;
	}


public static class WfaTrace {
//	private BitSet[] parts;
	private ArrayList<TotalWorkValues> wfValues = new ArrayList<TotalWorkValues>();
	private ArrayList<Double> sumNullCost = new ArrayList<Double>();

	public WfaTrace(KarlsIndexPartitions parts0, TotalWorkValues wf) {
//		parts = parts0.bitSetArray();
		wfValues.add(new TotalWorkValues(wf));
		sumNullCost.add(0.0);
	}

	public void addValues(TotalWorkValues wf, double nullCost) {
		double prevSum = sumNullCost.get(sumNullCost.size()-1);

		wfValues.add(new TotalWorkValues(wf));
		sumNullCost.add(prevSum + nullCost);
	}


	public TotalWorkValues[] getTotalWorkValues() {
		TotalWorkValues[] arr = new TotalWorkValues[wfValues.size()];
		return wfValues.toArray(arr);
	}

//	public double[] getMinWfValues() {
//		int valcount = wfValues.size();
//		double[] arr = new double[valcount];
//		for (int i = 0; i < valcount; i++) {
//			arr[i] = minWfValue(i);
//		}
//		return arr;
//	}


	public IndexBitSet[] optimalSchedule(KarlsIndexPartitions parts, int queryCount, Iterable<ProfiledQuery> qinfos) {

		// We will fill each BitSet with the optimal indexes for the corresponding query
		IndexBitSet[] bss = new IndexBitSet[queryCount];
		for (int i = 0; i < queryCount; i++) bss[i] = new IndexBitSet();

		// this array holds the optimal schedule for a single partition
		// it has the state that each query should be processed in, plus the
		// last state that should the system should be in after the last
		// query (which we expect to be the same as the state for the last
		// query, but that's not required). Each schedule, excluding the last
		// state, will be pushed into bss after it's computed
		IndexBitSet[] partSchedule = new IndexBitSet[queryCount+1];
		for (int q = 0; q <= queryCount; q++) partSchedule[q] = new IndexBitSet();

		for (int subsetNum = 0; subsetNum < parts.subsetCount(); subsetNum++) {
			KarlsIndexPartitions.Subset subset = parts.get(subsetNum);
			int[] indexIds = subset.indexIds();
			int stateCount = (int) subset.stateCount();

			// get the best final state
			int bestSuccessor = -1;
			{
				double bestValue = Double.POSITIVE_INFINITY;
				for (int stateNum = 0; stateNum < stateCount; stateNum++) {
					double value = wfValues.get(queryCount).get(subsetNum, stateNum);
					// use non-strict inequality to favor states with more indices
					// this is a mild hack to get more intuitive schedules
					if (value <= bestValue) {
						bestSuccessor = stateNum;
						bestValue = value;
					}
				}
			}

			Checks.checkAssertion(bestSuccessor >= 0, "could not determine best final state");
			partSchedule[queryCount].clear();
			WorkFunctionAlgorithm.setStateBits(indexIds, bestSuccessor, partSchedule[queryCount]);

			// traverse the workload to get the path
			for (int q = queryCount - 1; q >= 0; q--) {
				int stateNum = wfValues.get(q+1).predecessor(subsetNum, bestSuccessor);
				partSchedule[q].clear();
				WorkFunctionAlgorithm.setStateBits(indexIds, stateNum, partSchedule[q]);
				bestSuccessor = stateNum;
			}

			// now bestStates has the optimal schedule within current subset
			// merge with the global schedule
			for (int q = 0; q < queryCount; q++) {
				bss[q].or(partSchedule[q]);
			}
		}

		return bss;
	}
}
}
