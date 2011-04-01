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

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * The foundation for WFIT. The general Work Function Algorithm is described in subsection 3.3 of 
 * Karl Schnaitter's Doctoral Thesis "On-line Index Selection for Physical Database Tuning". The 
 * adapted version of the function (which is implemented in this class) is covered in section 6, 
 * specifically in subsection 6.1.
 *
 * @see <a 
 * href="http://proquest.umi.com/pqdlink?did=2171968721&Fmt=7&clientId=1565&RQT=309&VName=PQD">
 *     "On-line Index Selection for Physical Database Tuning"</a>
 */
public class WorkFunctionAlgorithm<I extends DBIndex>
{
    SubMachineArray<I>  submachines;
    TotalWorkValues     wf;
    private int         maxNumStates;
    private int         maxHotSize;
    private Workspace   workspace;
    private boolean     keepHistory;
    private WfaTrace<I> trace;

    private final Console console = Console.streaming();

    /**
     * construct a {@link WorkFunctionAlgorithm} object from a list of partitions. This object
     * will either keep some history or not. This choice will be determined by the
     * {@code keepHistory} flag.
     *
     * @param parts
     *      a list of index partitions.
     * @param keepHistory
     *      {@code true} if we want to keep some history of the work done by this object,
     *      {@code false} if we don't want to.
     */
    public WorkFunctionAlgorithm(
            IndexPartitions<I> parts,
            int                maxNumStates,
            int                maxHotSize,
            boolean            keepHistory)
    {
        this.maxNumStates = maxNumStates;
        this.maxHotSize   = maxHotSize;
        this.workspace    = new Workspace(this.maxNumStates,this.maxHotSize);
        this.submachines  = new SubMachineArray<I>(0);
        this.wf           = new TotalWorkValues(this.maxNumStates, this.maxHotSize);
    
        if (parts != null) {
            repartition(parts);

            if (keepHistory) {
                trace = new WfaTrace<I>(wf);
            }

            this.keepHistory = keepHistory;
        }
        else {
            Checks.checkAssertion(!keepHistory, "may only keep history if given the initial partition");
            this.keepHistory = false;
        }
        
        dump("INITIAL");
    }

    /**
     * construct a {@link WorkFunctionAlgorithm} object from a list of partitions.
     *
     * @param parts
     *    a list of index partitions.
     */
    public WorkFunctionAlgorithm(IndexPartitions<I> parts, int maxNumStates, int maxHotSize) {
        this(parts, maxNumStates, maxHotSize, false);
    }

    /**
     * construct a {@link WorkFunctionAlgorithm} object from a {@code null} list of partitions.
     */
    public WorkFunctionAlgorithm(int maxNumStates, int maxHotSize) {
        this(null, maxNumStates, maxHotSize);
    }

    /**
     * dump the total work values per {@link SubMachine}.
     * @param msg
     *      a debug message.
     */
    public void dump(String msg) {
        console.log(msg);
        for (int i = 0; i < submachines.length; i++) {
            console.log("SUBMACHINE " + i);
            submachines.get(i).dump();
            console.skip();
        }
        console.log("----");
    }

    /**
     * run a new task for a {@link ProfiledQuery query} through a {@link WorkFunctionAlgorithm}'s
     * submachine.
     * <p>
     * This method corresponds to algorithm {@code analyzeQuery} from Schnaitter's thesis, which is 
     * described in page in page 162 (Figure 6.3)
     *
     * @param qinfo
     *    a {@link ProfiledQuery query}.
     * @see {@link #getRecommendation()}
     */
    public void newTask(ProfiledQuery<I> qinfo) {
        workspace.tempBitSet.clear(); // just to be safe
        
        for (int subsetNum = 0; subsetNum < submachines.length; subsetNum++) {
            SubMachine<I> subm = submachines.get(subsetNum);
            preprocessCostIntoVector(qinfo, subm);
        }
        
        // all submachines have assigned the new values into wf2
        // swap wf and wf2
        { TotalWorkValues wfTemp = wf; wf = workspace.wf2; workspace.wf2 = wfTemp; }
        
        // keep trace info
        if (keepHistory) {
            double nullCost = qinfo.getIndexBenefitGraph().emptyCost();
            trace.addValues(wf, nullCost);
        }
        
        dump("NEW TASK");
    }

    private void preprocessCostIntoVector(ProfiledQuery<I> qinfo, SubMachine<I> subm) {
        // preprocess cost into a vector
        for (int stateNum = 0; stateNum < subm.numStates; stateNum++) {
            // this will explicitly set each index in the array to 1 or 0
            setStateBits(subm.indexIds, stateNum, workspace.tempBitSet);
            double queryCost = qinfo.totalCost(workspace.tempBitSet);
            workspace.tempCostVector.set(stateNum, queryCost);
        }

        // clear all indexes in the array
        clearStateBits(subm.indexIds, workspace.tempBitSet);

        // run the task through the submachine
        subm.newTask(workspace.tempCostVector, wf, workspace.wf2);
    }

    /**
     * process a positive or negative vote for the index found in some {@code SubMachine}.
     *
     * @param index
     *      a {@link DBIndex index} object.
     * @param isPositive
     *      value of vote given to an index object.
     */
    public void vote(I index, boolean isPositive) {
        Checks.checkAssertion(!keepHistory, "tracing WFA is not supported with user feedback");
        for (SubMachine<I> subm : submachines) {
            if (subm.subset.contains(index)){
                subm.vote(wf, index, isPositive);
            }
        }
        dump("VOTE " + (isPositive ? "POSITIVE " : "NEGATIVE ") + "for " + index.internalId());
    }

    /**
     * This method along with method {@link #newTask(ProfiledQuery)} correspond to algorithm {@code chooseCands}
     * from Schnaitter's thesis, which is described in page in page 169 (Figure 6.5).
     *
     * @return a list of recommended {@link DBIndex indexes}.
     */
    public List<I> getRecommendation() {
        ArrayList<I> rec = new ArrayList<I>(maxHotSize);
        for (SubMachine<I> subm : submachines) {
            for (I index : subm.subset) {
                if (subm.currentBitSet.get(index.internalId())){
                    System.out.println(index);
                    rec.add(index);
                }                        
            }
        }
        return rec;
    }

    /**
     * Perform a repartition of indexes in the {@link WorkFunctionAlgorithm} given a brand new
     * {@link IndexPartitions} object.
     * <p>
     * This method corresponds to algorithms {@code repartition} and {@code choosePartition} of 
     * Schnaitter's thesis (Figure 6.4 and 6.6 (pages 166 and 175), respectively).
     *
     * @param newPartitions
     *      a {@link IndexPartitions} object.
     */
    public void repartition(IndexPartitions<I> newPartitions) {
        Checks.checkAssertion(!keepHistory, "tracing WFA is not supported with repartitioning");
        int newSubsetCount = newPartitions.subsetCount();
        int oldSubsetCount = submachines.length;
        SubMachineArray<I> submachines2 = new SubMachineArray<I>(newSubsetCount);
        IndexBitSet overlappingSubsets = new IndexBitSet();
        
        // get the set of previously hot indexes
        IndexBitSet oldHotSet = new IndexBitSet();
        for (SubMachine<I> oldSubmachine : submachines) {
            for (int oldIndexId : oldSubmachine.indexIds) {
                oldHotSet.set(oldIndexId);
            }
        }
        
        // prepare wf2 with new partitioning
        workspace.wf2.reallocate(newPartitions);
        
        for (int newSubsetNum = 0; newSubsetNum < newSubsetCount; newSubsetNum++) {
            IndexPartitions.Subset<I> newSubset = newPartitions.get(newSubsetNum);
            
            // translate old recommendation into a new one.
            IndexBitSet recBitSet = new IndexBitSet();
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

                // we don't recompute the predecessor during repartitioning, but it is feasible
                workspace.wf2.set(newSubsetNum, stateNum, value, 0);
            }
        }
        
        // submachines2 now reflects the new partitioning 
        // wf2 now has the new workfunction values for submachines2
        
        submachines = submachines2; // start using new subsets
        wf.reallocate(workspace.wf2); // copy wf2 into wf (also changes the implicit partitioning within wf)
        dump("REPARTITION");
    }


    /**
     * sets the state bits in new index configuration.
     * @param ids
     *      indexes' internalIds.
     * @param stateNum
     *      number of state bits changes.
     * @param bitSet
     *      an index configuration.
     */
    static void setStateBits(int[] ids, int stateNum, IndexBitSet bitSet) {
        for (int i = 0; i < ids.length; i++)
            bitSet.set(ids[i], 0 != (stateNum & (1 << i)));
    }

    private static void clearStateBits(int[] ids, IndexBitSet bitSet) {
        for (int id : ids) bitSet.clear(id);
    }
    
    private boolean isRecommended(I idx) {
        // not sure which submachine has the index, so check them all
        for (SubMachine<I> subm : submachines) {
            if (subm.currentBitSet.get(idx.internalId())){
                return true;
            }
        }
        return false;
    }
    
    public static <J extends DBIndex> double transitionCost(Snapshot<J> candidateSet, IndexBitSet x, IndexBitSet y) {
        double transition = 0;
        for (J index : candidateSet) {
            int id = index.internalId();
            if (y.get(id) && !x.get(id))
                transition += index.creationCost();
        }
        return transition;
    }

    private static <J extends DBIndex> double transitionCost(IndexPartitions.Subset<J> subset, int x, int y) {
        double transition = 0;
        int i = 0;
        for (J index : subset) {
            int mask = 1 << (i++);
            
            if (mask == (y & mask) - (x & mask))
                transition += index.creationCost();
        }
        return transition;
    }

    /**
     * given a schedule over a set of candidates and queries, get the total work
     *
     * @param candidateSet
     *      a snapshot of candidate set.
     * @param queryCount
     *      number of profiled queries to be used.
     * @param qinfos
     *      list of profiled queries.
     * @param parts
     *      available index partitiones.
     * @param schedule
     *      an array of bit sets; each one represent an index configuration.
     * @param <J>
     *     the {@link DBIndex index} object.
     * @return
     *      a schedule cost over a set of candidate and queries.
     */
    public <J extends DBIndex> double getScheduleCost(Snapshot<J> candidateSet,
                                                         int queryCount, List<ProfiledQuery<J>> qinfos,
                                                         IndexPartitions<J> parts, IndexBitSet[] schedule
    ) {
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

    /**
     * @return the {@link WorkFunctionAlgorithm}'s work trace.
     */
    public WfaTrace<I> getTrace() {
        return trace;
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
            System.arraycopy(vector, 0, newVector, 0, cap);
            vector = newVector;
            cap = newCap;
        }

        @Override
        public String toString() {
            return new ToStringBuilder<CostVector>(this)
                   .add("vector", Arrays.toString(vector))
                   .add("cap", cap)
                   .toString();
        }
    }
    
    
    private static class SubMachineArray<J extends DBIndex> implements Iterable<SubMachine<J>> {
        public final int length;
        private final List<SubMachine<J>> arr;
        
        public SubMachineArray(int len0) {
            this.length = len0;
            arr = new ArrayList<SubMachine<J>>(len0);
            for (int i = 0; i < len0; i++){
                arr.add(null);
            }
        }

        public SubMachine<J> get(int i) {
            return arr.get(i);
        }
        
        public void set(int i, SubMachine<J> subm) {
            arr.set(i, subm);
        }

        @Override
        public Iterator<SubMachine<J>> iterator() {
            return arr.iterator();
        }

        @Override
        public String toString() {
            return new ToStringBuilder<SubMachineArray<J>>(this)
                   .add("arr", arr.toString())
                   .add("length", length)
                   .toString();
        }
    }
    
    private static class SubMachine<J extends DBIndex> implements Iterable<J> {
        private IndexPartitions.Subset<J> subset;
        private int subsetNum;
        private int numIndexes;
        private int numStates;
        private int currentState;
        private IndexBitSet currentBitSet;
        private int[] indexIds;
        private final Console console = Console.streaming();
        
        SubMachine(IndexPartitions.Subset<J> subset, int subsetNum, int state, IndexBitSet bitSet) {
            this.subset         = subset;
            this.subsetNum      = subsetNum;
            this.numIndexes     = subset.size();
            this.numStates      = 1 << numIndexes;
            this.currentState   = state;
            this.currentBitSet  = bitSet;
            
            this.indexIds = new int[numIndexes];
            int i = 0;
            for (J index : subset) {
                this.indexIds[i++] = index.internalId();
            }
        }
        
        /**
         * Returns the position of an index given its internalIds.
         * @param id
         *      index's internalId.
         * @return
         *      position of id in indexIds if exists, else -1
         */
        public int indexPos(int id) {
            for (int i = 0; i < numIndexes; i++)
                if (indexIds[i] == id)
                    return i;
            return -1;
        }

        /**
         * prints a debugging message, for this {@link SubMachine}, displaying all indexes'
         * internalIds.
         */
        public void dump() {
            final StringBuilder message = new StringBuilder();
            message.append("Index IDs : [ ");
            for (int id : indexIds) message.append(id).append(" ");
            message.append("]   ").append("REC : [ ");

            for (int id : indexIds) if (currentBitSet.get(id)) message.append(id).append(" ");
            message.append("]");
            console.log(message.toString());
        }

        /**
         * process a positive or negative vote for the index and do the necessary bookkeeping in
         * the input workfunction, and update the current state.
         * 
         * @param wf
         *      the {@link WorkFunctionAlgorithm}'s total work values.
         * @param index
         *      a {@link DBIndex index} object.
         * @param isPositive
         *      a positive ({@code true} value) or negative({@code false} value) vote.
         */
        public void vote(TotalWorkValues wf, J index, boolean isPositive) {
            // find the position in indexIds
            int indexIdsPos;
            int stateMask;
            for (indexIdsPos = 0; indexIdsPos < numIndexes; indexIdsPos++) 
                if (indexIds[indexIdsPos] == index.internalId())
                    break;
            if (indexIdsPos >= numIndexes) {
                console.error("could not process vote: index not found in subset");
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

        /**
         * Briefly, this method does the bookkeeping for a new task. The current work function
         * values are given by wfOld. this function assigns the new work function values into
         * wfNew, but of course only the states within this submachine are handled
         *
         * @param cost
         *      cost vector
         * @param wfOld
         *      {@code WorkFunctionAlgorithm}'s old total work values.
         * @param wfNew
         *      {@code WorkFunctionAlgorithm}'s new total work values.
         */
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
                    console.error("failed to compute work function");
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
        public Iterator<J> iterator() {
            return subset.iterator();
        }

        @Override
        public String toString() {
            return new ToStringBuilder<SubMachine<J>>(this)
                   .add("subsetNum",subsetNum)
                   .add("numIndexes",numIndexes)
                   .add("numStates",numStates)
                   .add("currentState",currentState)
                   .add("currentBitSet",currentBitSet)
                   .add("indexIds", Arrays.toString(indexIds))
                   .add("subset", subset)
                   .toString();
        }
    }

    /**
     * wraps the {@link WorkFunctionAlgorithm}'s total work values.
     */
    public static class TotalWorkValues {
        double[] values;
        int[] predecessor;
        int[] subsetStart;

        /**
         * construct a {@code total work values} object. This object is constructed given the
         * max number of states and the max hotset size accepted by the {@code work function algo}.
         */
        TotalWorkValues(int maxNumStates, int maxHotSize) {
            values      = new double[maxNumStates];
            subsetStart = new int[maxHotSize];
            predecessor = new int[maxNumStates];
        }

        /**
         * construct a {@code total work values} object given the previous
         * {@code WorkFunctionAlgorithm}'s total work values.
         * @param wf2
         *   a previous {@code WorkFunctionAlgorithm}'s total work values.
         */
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
            if (newSubsetCount > subsetStart.length){
                subsetStart = new int[newSubsetCount];
            }

            System.arraycopy(wf2.subsetStart, 0, subsetStart, 0, newSubsetCount);
            
            for (int i = 0; i < newValueCount; i++) {
                values[i] = wf2.values[i];
                predecessor[i] = wf2.predecessor[i];
            }
        }

        @Override
        public String toString() {
            return new ToStringBuilder<TotalWorkValues>(this)
                   .add("values", Arrays.toString(values))
                   .add("predecessor", Arrays.toString(predecessor))
                   .add("subsetStart", Arrays.toString(subsetStart))
                   .toString();
        }
    }

    /**
     * WorkFunctionAlgorithm's workspace.
     */
    private static class Workspace {
        TotalWorkValues wf2;
        CostVector      tempCostVector;
        IndexBitSet     tempBitSet;

        Workspace(int maxNumStates, int maxHotSize)
        {
            wf2            = new TotalWorkValues(maxNumStates,maxHotSize);
            tempCostVector = new CostVector(maxNumStates);
            tempBitSet     = new IndexBitSet();
        }
    }
}
