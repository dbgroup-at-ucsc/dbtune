package edu.ucsc.dbtune.advisor.wfit;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

import edu.ucsc.dbtune.advisor.wfit.IndexPartitions.Subset;
import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.util.MetadataUtils.find;

//CHECKSTYLE:OFF
public class WorkFunctionAlgorithm {
    TotalWorkValues wf;
    SubMachine[] submachines = new SubMachine[0];
    
    // for tracking history
    private boolean keepHistory;
    private WfaTrace trace;
    private int minId;
    
    //
    // temporary workspace
    //
    private TotalWorkValues wf2;
    private CostVector tempCostVector;
    private BitSet tempBitSet = new BitSet();

    public WorkFunctionAlgorithm(
            IndexPartitions parts,
            boolean keepHistory0,
            int maxNumStates,
            int maxNumIndexes,
            int minId)
    {
        this.minId = minId;
        this.wf = new TotalWorkValues(maxNumStates, maxNumIndexes);
        this.tempCostVector = new CostVector(maxNumStates);
        this.wf2 = new TotalWorkValues(maxNumStates, maxNumIndexes);

        dump("BEFORE INITIAL");
        repartition(parts);

        if (keepHistory0) {
            trace = new WfaTrace(parts, wf);
            keepHistory = keepHistory0;
        }
        
        dump("AFTER INITIAL");
    }
    
    public WorkFunctionAlgorithm(
            IndexPartitions parts,
            int maxNumStates,
            int maxNumIndexes,
            int minId)
    {
        this(parts, false, maxNumStates, maxNumIndexes, minId);
    }

    public WorkFunctionAlgorithm(int minId) {
        this.minId = minId;
        keepHistory = false;
    }
    
    public void dump(String msg) {
        //System.out.println(msg);
        //for (int i = 0; i < submachines.length; i++) {
         //   System.out.println("SUBMACHINE " + i);
            //submachines[i].dump(wf);
          //  System.out.println();
        //}
        //System.out.println("----");
    }
    
    public void newTask(ProfiledQuery qinfo) {
        dump("BEFORE NEW TASK");
        tempBitSet.clear(); // just to be safe
        
        for (int subsetNum = 0; subsetNum < submachines.length; subsetNum++) {
            SubMachine subm = submachines[subsetNum];
            
            // preprocess cost into a vector
            for (int stateNum = 0; stateNum < subm.numStates; stateNum++) {
                // this will explicitly set each index in the array to 1 or 0
                setStateBits(subm.indexIds, stateNum, tempBitSet);
                double queryCost = qinfo.cost(tempBitSet);
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
            tempBitSet.clear();
            double nullCost = qinfo.cost(tempBitSet);
            trace.addValues(wf, nullCost);
        }
        
        dump("AFTER NEW TASK");
    }

    public void vote(Index index, boolean isPositive) throws NoSuchElementException {
        dump("BEFORE VOTE");
        boolean voted = false;

        for (SubMachine subm : submachines)
            if (subm.subset.contains(index)) {
                subm.vote(wf, index, isPositive);
                voted = true;
            }

        if (!voted)
            throw new NoSuchElementException("Index " + index + " not on any WFA partition");

        dump("AFTER VOTE " + (isPositive ? "POSITIVE " : "NEGATIVE ") + "for " + 
                (index.getId()-minId));
    }

    public Set<Index> getRecommendation() {
        Set<Index> rec = new TreeSet<Index>();

        for (SubMachine subm : submachines)
            for (Index index : subm.subset)
                if (subm.currentBitSet.get(index.getId()-minId))
                    rec.add(index);
        return rec;
    }

    public void repartition(IndexPartitions newPartitions) {
        dump("BEFORE REPARTITION");
        int newSubsetCount = newPartitions.subsetCount();
        int oldSubsetCount = submachines.length;
        SubMachine[] submachines2 = new SubMachine[newSubsetCount];
        BitSet overlappingSubsets = new BitSet();
        
        // get the set of previously hot indexes
        BitSet oldHotSet = new BitSet();
        for (SubMachine oldSubmachine : submachines) {
            for (int oldIndexId : oldSubmachine.indexIds) {
                oldHotSet.set(oldIndexId);
            }
        }
        
        // prepare wf2 with new partitioning
        wf2.reallocate(newPartitions);
        
        for (int newSubsetNum = 0; newSubsetNum < newSubsetCount; newSubsetNum++) {
            IndexPartitions.Subset newSubset = newPartitions.get(newSubsetNum);
            
            // translate old recommendation into new
            BitSet recBitSet = new BitSet();
            int recStateNum = 0;
            int i = 0;
            for (Index index : newSubset) {
                if (isRecommended(index)) {
                    recBitSet.set(index.getId()-minId);
                    recStateNum |= (1 << i);
                }
                ++i;
            }
            SubMachine newSubmachine = new SubMachine(newSubset, newSubsetNum, recStateNum, 
                    recBitSet, minId);
            submachines2[newSubsetNum] = newSubmachine;

            // find overlapping subsets (required to recompute work function)
            overlappingSubsets.clear();
            for (int oldSubsetNum = 0; oldSubsetNum < submachines.length; oldSubsetNum++) {
                if (newSubset.overlaps(submachines[oldSubsetNum].subset)) {
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
                    if (0 != (stateNum & mask) && !oldHotSet.get(index.getId()-minId)) {
                        value += index.getCreationCost();
                    }
                }
                
                for (int oldSubsetNum = 0; oldSubsetNum < oldSubsetCount; oldSubsetNum++) {
                    if (!overlappingSubsets.get(oldSubsetNum))
                        continue;
                    
                    int[] oldIndexIds = submachines[oldSubsetNum].indexIds;
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
        dump("AFTER REPARTITION");
    }

    public Map<Set<Index>, Double> getWorkFunctionScores(Set<Index> pool)
    {
        Map<Set<Index>, Double> wfValues = new HashMap<Set<Index>, Double>();

        for (SubMachine subm : submachines) {

            for (int s = 0; s < subm.numStates; s++) {
                Set<Index> subset = new TreeSet<Index>();

                for (int i = 0; i < subm.indexIds.length; i++)
                    if (((s >> i) & 1) == 1)
                        subset.add(find(pool, subm.indexIds[i]));

                wfValues.put(subset, wf.get(subm.subsetNum, s));
            }
        }

        return wfValues;
    }
    
    static void setStateBits(int[] ids, int stateNum, BitSet bitSet) {
        for (int i = 0; i < ids.length; i++)
            bitSet.set(ids[i], 0 != (stateNum & (1 << i)));
    }

    private static void clearStateBits(int[] ids, BitSet bitSet) {
        for (int i = 0; i < ids.length; i++)
            bitSet.clear(ids[i]);
    }
    
    private boolean isRecommended(Index idx) {
        // not sure which submachine has the index, so check them all
        for (SubMachine subm : submachines) {
            if (subm.currentBitSet.get(idx.getId()-minId))
                return true;
        }
        return false;
    }
    
    public static double transitionCost(Set<Index> candidateSet, BitSet x, BitSet y, int minId) {
        double transition = 0;
        for (Index index : candidateSet) {
            int id = index.getId()-minId;
            if (y.get(id) && !x.get(id)) {
                transition += index.getCreationCost();
            }
        }
        return transition;
    }

    private static double transitionCost(Subset subset, int x, int y) {
        double transition = 0;
        int i = 0;
        for (Index index : subset) {
            int mask = 1 << (i++);
            
            if (mask == (y & mask) - (x & mask)) {
                transition += index.getCreationCost();
            }
        }
        return transition;
    }
    
    // return a new copy of the current work function
    // DEPRECATED
//  public TotalWorkValues getTotalWorkValues() {
//      return new TotalWorkValues(wf);
//  }
    
    private static class SubMachine implements Iterable<Index> {
        private IndexPartitions.Subset subset;
        private int subsetNum;
        private int numIndexes;
        private int numStates;
        private int currentState;
        private BitSet currentBitSet;
        private int[] indexIds;
        private int minId;
        
        SubMachine(IndexPartitions.Subset subset0, int subsetNum0, int state0, BitSet bs0, int 
                minId) {
            subset = subset0;
            subsetNum = subsetNum0;
            numIndexes = subset0.size();
            numStates = 1 << numIndexes;
            currentState = state0;
            currentBitSet = bs0;
            
            indexIds = new int[numIndexes];
            int i = 0;
            for (Index index : subset0) {
                indexIds[i++] = index.getId()-minId;
            }
            this.minId = minId;
        }
        
        // return position of id in indexIds if exists, else -1
        public int indexPos(int id) {
            for (int i = 0; i < numIndexes; i++)
                if (indexIds[i] == id)
                    return i;
            return -1;
        }
        
        /*
        public void dump(TotalWorkValues wf) {
            //System.out.print("Index IDs : [ ");
            //for (int id : indexIds) System.out.print(id + " ");
            //System.out.print("]   ");

            //System.out.print("REC : [ ");
            //for (int id : indexIds) if (currentBitSet.get(id)) System.out.print(id + " ");
            //System.out.println("]");
            
            //System.out.println("Current workfunction values ...");
            //for (int s = 0; s < numStates; s++) {
                //System.out.print("   [ ");
                //for (int i = 0; i < indexIds.length; i++) {
                    //String id = "" + indexIds[i];
                    //if (((s >> i) & 1) == 1)
                        //System.out.print(id);
                    //else 
                        //for (int k = 0; k < id.length(); k++) System.out.print(" ");
                    //System.out.print(" ");
                //}
                //System.out.println("] = " + wf.get(subsetNum, s));
            //}
        }
        */

        // process a positive or negative vote for the index
        // do the necessary bookkeeping in the input workfunction, and update the current state
        public void vote(TotalWorkValues wf, Index index, boolean isPositive) {
            // find the position in indexIds
            int indexIdsPos;
            int stateMask;
            for (indexIdsPos = 0; indexIdsPos < numIndexes; indexIdsPos++) if (indexIds[indexIdsPos] 
                    == index.getId()-minId)
                    break;
            if (indexIdsPos >= numIndexes) {
                //Debug.logError("could not process vote: index not found in subset");
                return;
            }
            
            // integer with a 1 in the position of the index
            stateMask = (1 << indexIdsPos);
                    
            // register the vote in the recommendation
            if (isPositive) {
                currentBitSet.set(index.getId()-minId);
                currentState |= stateMask;
            }
            else {
                currentBitSet.clear(index.getId()-minId);
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
                //if (Double.isInfinite(wfValueBest))
                    //Debug.logError("failed to compute work function");
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

        @Override
        public Iterator<Index> iterator() {
            return subset.iterator();
        }
    }
    
    public static class TotalWorkValues {
        double[] values;
        int[] predecessor;
        int[] subsetStart; 

        TotalWorkValues(int maxNumStates, int maxNumIndexes) {
            values = new double[maxNumStates];
            subsetStart = new int[maxNumIndexes];
            predecessor = new int[maxNumStates];
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

        void reallocate(IndexPartitions newPartitions) {
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
        
        CostVector(int maxNumStates) {
            cap = maxNumStates;
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
    public static double getScheduleCost(
            Set<Index> candidateSet, int queryCount, ProfiledQuery[] qinfos, BitSet[] schedule, int 
            minId)
    {
        double cost = 0;
        BitSet prevState = new BitSet();
        for (int q = 0; q < queryCount; q++) {
            BitSet state = schedule[q];
            cost += transitionCost(candidateSet, prevState, state, minId);
            cost += qinfos[q].cost(state);
            prevState = state;
        }
        return cost;
    }

    public WfaTrace getTrace() {
        return trace;
    }
}
//CHECKSTYLE:ON
