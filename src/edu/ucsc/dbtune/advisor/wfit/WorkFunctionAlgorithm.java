package edu.ucsc.dbtune.advisor.wfit;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.sql.SQLException;

import edu.ucsc.dbtune.advisor.interactions.IndexPartitions;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;
import edu.ucsc.dbtune.util.BitArraySet;

/**
 * The foundation for WFIT. The general Work Function Algorithm is described in subsection 3.3 of 
 * Karl Schnaitter's Doctoral Thesis "On-line Index Selection for Physical Database Tuning". The 
 * adapted version of the function (which is implemented in this class) is covered in section 6, 
 * specifically in subsection 6.1.
 *
 * @see <a 
 * href="http://proquest.umi.com/pqdlink?did=2171968721&Fmt=7&clientId=1565&RQT=309&VName=PQD">
 *     On-line Index Selection for Physical Database Tuning</a>
 */
public class WorkFunctionAlgorithm 
{
    private TotalWorkValues wf;
    private SubMachineArray submachines;
    private Workspace       workspace;
    private Set<Index>      allIndexes;
    private int             maxNumOfIndexes;

    /**
     * construct a {@link WorkFunctionAlgorithm} object from a list of partitions. This object
     * will either keep some history or not. This choice will be determined by the
     * {@code keepHistory} flag.
     *
     * @param parts
     *      a list of index partitions.
     * @param maxNumStates
     *      maximum number of states (partitions) to consider during calculations
     * @param maxNumIndexes
     *      maximum number of indexes to consider per each state (partition)
     */
    public WorkFunctionAlgorithm(Set<Index> allIndexes, int maxNumStates, int maxNumIndexes)
    {
        this.maxNumOfIndexes = maxNumIndexes;
        this.allIndexes      = allIndexes;
        this.workspace       = new Workspace(maxNumStates,maxNumIndexes);
        this.wf              = new TotalWorkValues(maxNumStates,maxNumIndexes);
        this.submachines     = new SubMachineArray(0);

        IndexPartitions parts = new IndexPartitions(allIndexes);

        repartition(parts);
    }

    /**
     * run a new task for a {@link PreparedSQLStatement query} through a {@link WorkFunctionAlgorithm}'s
     * submachine.
     * <p>
     * This method corresponds to algorithm {@code analyzeQuery} from Schnaitter's thesis, which is 
     * described in page in page 162 (Figure 6.3)
     *
     * @param qinfo
     *    a {@link PreparedSQLStatement} query.
     * @param configuration
     *     a {@link Configuration} that represents the set of all indexes that are 
     * @see #getRecommendation()
     */
    public void newTask(PreparedSQLStatement qinfo, Set<Index> configuration) throws SQLException
    {
      workspace.tempBitSet.clear(); // just to be safe

      Set<Index> conf;
      double queryCost;

      for (Index idx : configuration) {
          allIndexes.add(idx);
      }

      for (int subsetNum = 0; subsetNum < submachines.length; subsetNum++) {
          SubMachine subm = submachines.get(subsetNum);
          // preprocess cost into a vector
          for (int stateNum = 0; stateNum < subm.numStates; stateNum++) {
            // this will explicitly set each index in the array to 1 or 0
            conf = new BitArraySet<Index>(workspace.tempBitSet);
            queryCost = qinfo.explain(conf).getTotalCost();
            workspace.tempCostVector.set(stateNum, queryCost);
          }

        }
        
        // all submachines have assigned the new values into wf2
        // swap wf and wf2
        {
          TotalWorkValues wfTemp = wf;
          wf = workspace.wf2;
          workspace.wf2 = wfTemp;
        }
    }

    /**
     * process a positive or negative vote for the index found in some {@code SubMachine}.
     *
     * @param index
     *      a {@link Index index} object.
     * @param isPositive
     *      value of vote given to an index object.
     */
    public void vote(Index index, boolean isPositive)
    {
        for (SubMachine subm : submachines) {
            if (subm.subset.contains(index)){
                subm.vote(wf, index, isPositive);
            }
        }
    }

    /**
     * This method along with method {@link #newTask(ExplainedSQLStatement)} corresponds to algorithm {@code chooseCands} from 
     * Schnaitter's thesis, which is described in page in page 169 (Figure 6.5).
     *
     * @return a list of recommended {@link Index indexes}.
     */
    public List<Index> getRecommendation()
    {
        ArrayList<Index> rec = new ArrayList<Index>(maxNumOfIndexes);
        for (SubMachine subm : submachines) {
            for (Index index : subm.subset) {
                if (subm.currentBitSet.contains(index)) {
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
    public void repartition(IndexPartitions newPartitions)
    {
        int newSubsetCount = newPartitions.subsetCount();
        int oldSubsetCount = submachines.length;
        SubMachineArray submachines2 = new SubMachineArray(newSubsetCount);
        BitSet overlappingSubsets = new BitSet();
        
        // get the set of previously hot indexes
        BitArraySet<Index> oldHotSet = new BitArraySet<Index>();
        
        // prepare wf2 with new partitioning
        workspace.wf2.reallocate(newPartitions);
        
        for (int newSubsetNum = 0; newSubsetNum < newSubsetCount; newSubsetNum++) {
            Set<Index> newSubset = new HashSet<Index>(newPartitions.get(newSubsetNum));
            
            // translate old recommendation into a new one.
            BitArraySet<Index> recBitSet = new BitArraySet<Index>();
            int recStateNum = 0;
            int i = 0;
            for (Index index : newSubset) {
                if (isRecommended(index)) {
                    recBitSet.add(index);
                    recStateNum |= (1 << i);
                }
                ++i;
            }
            SubMachine newSubmachine = new SubMachine(allIndexes, newSubset, newSubsetNum, recStateNum, recBitSet);
            submachines2.set(newSubsetNum, newSubmachine);

            // find overlapping subsets (required to recompute work function)
            overlappingSubsets.clear();
            for (int oldSubsetNum = 0; oldSubsetNum < submachines.length; oldSubsetNum++) {
                if (newSubset.retainAll(submachines.get(oldSubsetNum).subset)) {
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
                    if (0 != (stateNum & mask) && !oldHotSet.contains(index))
                        value += index.getCreationCost();
                }
                
                for (int oldSubsetNum = 0; oldSubsetNum < oldSubsetCount; oldSubsetNum++) {
                    if (!overlappingSubsets.get(oldSubsetNum))
                        continue;
                }

                // we don't recompute the predecessor during repartitioning, but it is feasible
                workspace.wf2.set(newSubsetNum, stateNum, value, 0);
            }
        }
        
        // submachines2 now reflects the new partitioning 
        // wf2 now has the new workfunction values for submachines2
        
        submachines = submachines2; // start using new subsets
        wf.reallocate(workspace.wf2); // copy wf2 into wf (also changes the implicit partitioning within wf)
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
    static void setStateBits(Index[] indexes, int stateNum, BitArraySet<Index> bitSet) {
        for (Index index : indexes) {
            if (0 != (stateNum & (1 << index.getId())))
                bitSet.add(index);
            else
                bitSet.remove(index);
        }
    }

    private boolean isRecommended(Index idx)
    {
        // not sure which submachine has the index, so check them all
        for (SubMachine subm : submachines) {
            if (subm.currentBitSet.contains(idx)) {
                return true;
            }
        }
        return false;
    }
    
    public static double transitionCost(Set<Index> candidateSet, Set<Index> x, Set<Index> y)
    {
        double transition = 0;
        for (Index index : candidateSet) {
            if (y.contains(index) && !x.contains(index))
                transition += index.getCreationCost();
        }
        return transition;
    }

    private static double transitionCost(Set<Index> subset, int x, int y)
    {
        double transition = 0;
        int i = 0;
        for (Index index : subset) {
            int mask = 1 << (i++);
            
            if (mask == (y & mask) - (x & mask))
                transition += index.getCreationCost();
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
     * @return
     *      a schedule cost over a set of candidate and queries.
     */
    public double getScheduleCost(Set<Index> candidateSet,
            int queryCount, List<PreparedSQLStatement> qinfos,
            IndexPartitions parts, BitArraySet<Index>[] schedule )
        throws SQLException
    {
        throw new SQLException("This method needs to be reimplemented -- see issue #122");

        /**
        double cost = 0;
        
        // This makes the assumption that the starting state is the empty state
        IndexBitSet prevState = new IndexBitSet();
        
        for (int q = 0; q < queryCount; q++) {
            IndexBitSet state = schedule[q];
            Configuration configuration = new ConfigurationBitSet(candidateSet, state);
            //if (parts != null)
            //
            // parts.theoreticalCost is just invoking a explain() on the qinfo, that is, the else is 
            // doing the same that the if. Commenting until this gets clarified.
            // Alkis: The partitions are approximate and not precise. Note that the explain
            // called in {@link #theoreticalCost()} is on the subset of the recommendation
            // corresponding to the partition, whereas the following explain is on the
            // whole recommendation.
            //
            //    cost += parts.theoreticalCost(qinfos.get(q), state, subset);
            //else
            // Alkis I rewrote the following parts so that they correspond to something
            // sane. I am not sure what was the intended meaning of the previous code.
            ExplainedSQLStatement stmt = qinfos.get(q).explain(configuration);    
            cost += stmt.getCost();
            cost += stmt.getUpdateCost(stmt.getUsedConfiguration().toList());
            cost += transitionCost(candidateSet, prevState, state);

            prevState = state;
        }
        return cost;
        **/
    }

    private static class CostVector
    {
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
    
    
    private static class SubMachineArray implements Iterable<SubMachine>
    {
        public final int length;
        private final List<SubMachine> arr;
        
        public SubMachineArray(int len0)
        {
            this.length = len0;
            arr = new ArrayList<SubMachine>(len0);
            for (int i = 0; i < len0; i++){
                arr.add(null);
            }
        }

        public SubMachine get(int i)
        {
            return arr.get(i);
        }
        
        public void set(int i, SubMachine subm)
        {
            arr.set(i, subm);
        }

        @Override
        public Iterator<SubMachine> iterator()
        {
            return arr.iterator();
        }
    }
    
    private static class SubMachine implements Iterable<Index>
    {
        private Set<Index> subset;
        private int subsetNum;
        private int numIndexes;
        private int numStates;
        private int currentState;
        private BitArraySet<Index> currentBitSet;
        private Index[] indexes;
        
        SubMachine(Set<Index> candidateSet, Set<Index> subset, int subsetNum, int state, 
                BitArraySet<Index> bitSet) {
            this.subset         = subset;
            this.subsetNum      = subsetNum;
            this.numIndexes     = subset.size();
            this.numStates      = 1 << numIndexes;
            this.currentState   = state;
            this.currentBitSet  = bitSet;
            
            this.indexes = new Index[numIndexes];
            int i = 0;
            for (Index index : subset) {
                this.indexes[i++] = index;
            }
        }
        
        /**
         * process a positive or negative vote for the index and do the necessary bookkeeping in
         * the input workfunction, and update the current state.
         * 
         * @param wf
         *      the {@link WorkFunctionAlgorithm}'s total work values.
         * @param index
         *      a {@link Index index} object.
         * @param isPositive
         *      a positive ({@code true} value) or negative({@code false} value) vote.
         */
        public void vote(TotalWorkValues wf, Index index, boolean isPositive)
        {
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
        void newTask(Set<Index> conf, CostVector cost, TotalWorkValues wfOld, TotalWorkValues wfNew) 
        {
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
        }

        @Override
        public Iterator<Index> iterator()
        {
            return subset.iterator();
        }
    }

    /**
     * wraps the {@link WorkFunctionAlgorithm}'s total work values.
     */
    public static class TotalWorkValues
    {
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

        void reallocate(IndexPartitions newPartitions) {
            int newValueCount = 1 << newPartitions.indexCount();
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

            for (int subsetNum = 0; subsetNum < newSubsetCount; subsetNum++) {
              subsetStart[subsetNum] = wf2.subsetStart[subsetNum];
            }

            for (int i = 0; i < newValueCount; i++) {
                values[i] = wf2.values[i];
                predecessor[i] = wf2.predecessor[i];
            }
        }
    }

    /**
     * WorkFunctionAlgorithm's workspace.
     */
    private static class Workspace
    {
        TotalWorkValues wf2;
        CostVector      tempCostVector;
        BitArraySet<Index>     tempBitSet;

        public Workspace(int maxNumOfStates, int maxHotsize)
        {
            wf2            = new TotalWorkValues(maxNumOfStates,maxHotsize);
            tempCostVector = new CostVector(maxNumOfStates);
            tempBitSet     = new BitArraySet<Index>();
        }
    }
}
