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

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.IBGPreparedSQLStatement;
import edu.ucsc.dbtune.util.ToStringBuilder;
import edu.ucsc.dbtune.util.Instances;

import java.util.List;
import java.sql.SQLException;

/**
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class CandidatesSelector {
    private final   IndexStatisticsFunction idxStats;
    private final   WorkFunctionAlgorithm   wfa;
    private final   DynamicIndexSet         matSet;
    private         StaticIndexSet          hotSet;
    private final   DynamicIndexSet         userHotSet;
    private         IndexPartitions         hotPartitions;
    private         int                     maxHotSetSize;
    private         int                     maxNumStates;
    private         int                     partitionIterations;

    /**
      * Construct a {@code CandidatesSelector} object.
      */
    public CandidatesSelector(
            int maxNumStates,
            int maxHotSetSize,
            int partitionIterations,
            int indexStatisticsWindow)
    {
        this(new IndexStatisticsFunction(indexStatisticsWindow),
             new WorkFunctionAlgorithm(null, maxNumStates, maxHotSetSize),
             new StaticIndexSet(),
             new DynamicIndexSet(),
             new DynamicIndexSet(),
             maxNumStates, maxHotSetSize, partitionIterations );
    }

    /**
     * Construct a {@code CandidatesSelector} object.
     * <strong>Note</strong>: this was added to ease the testing of the {@code candidate selector}.
     * @param indexesStats
     *      an {@link IndexStatisticsFunction} object.
     * @param wfa
     *      an instance of {@link WorkFunctionAlgorithm} object.
     * @param hotSet
     *      a hot set of indexes.
     * @param userHotSet
     *      the user's hot set of indexes.
     * @param matSet
     *      the set of materialized indexes.
     */
    CandidatesSelector(IndexStatisticsFunction indexesStats,
                       WorkFunctionAlgorithm   wfa,
                       StaticIndexSet          hotSet,
                       DynamicIndexSet         userHotSet,
                       DynamicIndexSet         matSet,
                       int                        maxNumStates,
                       int                        maxNumIndexes,
                       int                        partitionIterations
    ){
        idxStats                 = indexesStats;
        this.wfa                 = wfa;
        this.hotSet              = hotSet;
        this.userHotSet          = userHotSet;
        this.matSet              = matSet;
        this.hotPartitions       = new IndexPartitions(hotSet);
        this.maxHotSetSize       = maxNumIndexes;
        this.maxNumStates        = maxNumStates;
        this.partitionIterations = partitionIterations;
    }


    /**
     * Perform the per-query tasks that are done after profiling
     * @param qinfo
     *      a {@link IBGPreparedSQLStatement} object.
     * @return an {@link AnalyzedQuery} object.
     */
    public AnalyzedQuery analyzeQuery(IBGPreparedSQLStatement qinfo) throws SQLException {
        // add the query to the statistics repository
        idxStats.addQuery(qinfo, matSet);
        
        reorganizeCandidates(qinfo.getConfiguration());
        
        wfa.newTask(qinfo);
        
        return new AnalyzedQuery(qinfo, hotPartitions.bitSetArray());
    }
    
    /**
     * Called by main thread to get a recommendation.
     * @return a list of indexes recommendations.
     */
    public List<Index> getRecommendation() {
        return wfa.getRecommendation();
    }

    /**
     * Bias the statistics of an index, in a candidate pool of indexes, in its favor.
     * @param index
     *      a {@link Index index} object.
     * @param candSet
     *      a {@code snapshot} of a {@code candidate pool} of indexes.
     */
    public void positiveVote(Index index, Iterable<? extends Index> candSet) {
        // get it in the hot set
        if (!userHotSet.contains(index)) {
            userHotSet.add(index);
            
            // ensure that userHotSet is a subset of HotSet
            if (!hotSet.contains(index)) {
                reorganizeCandidates(candSet);
            }
        }
        
        // Now the index is being monitored by WFA
        // Just need to bias the statistics in its favor
        wfa.vote(index, true);
    }

    /**
     * Bias the statistics against the index. 
     * @param index
     *      a {@link Index index} object.
     */
    public void negativeVote(Index index) {     
        // Check if the index is hot before doing anything.
        //
        // If the index is not being tracked by WFA, we have nothing to do.
        // Note that this check skips indexes that are not in 
        // the overall candidate pool.
        if (hotSet.contains(index)) {
            // ensure that the index is no longer forced in the hot set
            userHotSet.remove(index);
            
            // don't remove from the hot set necessarily
            
            // bias the statistics against the index
            wfa.vote(index, false);
        }
    }

    /**
     * Returns the current cost of a {@code profiled query} given a set
     * of materialized indexes.
     * @param qinfo
     *      a {@link IBGPreparedSQLStatement} object.
     * @return
     *      the total cost of a {@code profiled query}.
     */
    public double currentCost(IBGPreparedSQLStatement qinfo) throws SQLException {
        return qinfo.explain(
                Instances.newIndexList(
                    qinfo.getConfiguration(), matSet.bitSet())).getTotalCost();
    }

    /**
     * Drop an index (i.e., remove it from the materialized index set) and return a {@code 0} cost
     * assuming there is no cost to drop.
     * @param index
     *      index to be dropped.
     * @return
     *      zero since there is no cost to drop.
     */
    public double drop(Index index) {
        matSet.remove(index);
        return 0; // note: assuming no cost to drop
    }

    /**
     * adds an index to the materialized index set and returns its creation cost.
     * @param index
     *      index to be added.
     * @return either the index's {@code creationCost} or (if the index is already in the
     *      materialized index set) 0.
     */
    public double create(Index index) {
        if (!matSet.contains(index)) {
            matSet.add(index);
            return index.getCreationCost();
        }
        return 0;
    }
    
    /**
     * common code between positiveVote and processQuery
     * @param candSet
     *      a {@code snapshot} of {@code candidate pool of indexes}.
     */
    private void reorganizeCandidates(Iterable<? extends Index> candSet) {
        // determine the hot set
        DynamicIndexSet reqIndexes = new DynamicIndexSet();
        for (Index index : userHotSet) reqIndexes.add(index);
        for (Index index : matSet) reqIndexes.add(index);
        final HotsetSelection hotSelection = new HotsetSelection.StrictBuilder(false)
                .candidateSet(candSet)
                .oldHotSet(hotSet)
                .requiredIndexSet(reqIndexes)
                .benefitFunction(idxStats)
                .maxSize(maxHotSetSize)
            .get();
        StaticIndexSet newHotSet = 
            HotSetSelector.chooseHotSet(hotSelection);
        
        // determine new partitioning
        // store into local variable, since we might reject it
        final InteractionSelection interactionSelection = new InteractionSelection.StrictBuilder()
                .newHotSet(newHotSet)
                .oldPartitions(hotPartitions)
                .doiFunction(idxStats)
                .maxNumStates(maxNumStates)
            .get();
        IndexPartitions newHotPartitions = 
            InteractionSelector.choosePartitions(interactionSelection,partitionIterations);
        
        // commit hot set
        hotSet = newHotSet;
        if (hotSet.size() > maxHotSetSize) {
            maxHotSetSize = hotSet.size();
        }
        
        // commit new partitioning
        if (!newHotPartitions.equals(hotPartitions)) {
            hotPartitions = newHotPartitions;
            wfa.repartition(hotPartitions);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder<CandidatesSelector>(this)
               .add("idxStats",idxStats)
               .add("wfa",wfa)
               .add("matSet",matSet)
               .add("hotSet",hotSet)
               .add("userHotSet",userHotSet)
               .add("hotPartitions",hotPartitions)
               .add("maxHotSetSize",maxHotSetSize)
               .add("maxNumStates",maxNumStates)
           .toString();
    }
}
