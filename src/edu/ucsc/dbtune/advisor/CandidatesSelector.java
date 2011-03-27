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
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.util.List;

public class CandidatesSelector<I extends DBIndex> {
	// configurable options
	public static final int MAX_HOTSET_SIZE = 40;
	public static final int MAX_NUM_STATES  = 12345;

	private final   IndexStatisticsFunction<I>  idxStats;
	private final   WorkFunctionAlgorithm<I>    wfa;
	private final   DynamicIndexSet<I>          matSet;
	private         StaticIndexSet<I>           hotSet;
	private final   DynamicIndexSet<I>          userHotSet;
	private         IndexPartitions<I>          hotPartitions;
	private         int                         maxHotSetSize   = MAX_HOTSET_SIZE;
	private         int                         maxNumStates    = MAX_NUM_STATES;
    private final Console console = Console.streaming();

    /**
     * Construct a {@code CandidatesSelector} object.
     */
	public CandidatesSelector() {
        this(new IndexStatisticsFunction<I>(),
             new WorkFunctionAlgorithm<I>(),
             new StaticIndexSet<I>(),
             new DynamicIndexSet<I>(),
             new DynamicIndexSet<I>()
        );
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
    CandidatesSelector(IndexStatisticsFunction<I> indexesStats,
                       WorkFunctionAlgorithm<I> wfa,
                       StaticIndexSet<I> hotSet,
                       DynamicIndexSet<I> userHotSet,
                       DynamicIndexSet<I> matSet
    ){
        idxStats            = indexesStats;
        this.wfa            = wfa;
        this.hotSet         = hotSet;
        this.userHotSet     = userHotSet;
        this.matSet         = matSet;
        this.hotPartitions  = new IndexPartitions<I>(hotSet);
    }


	/**
	 * Perform the per-query tasks that are done after profiling
     * @param qinfo
     *      a {@link ProfiledQuery} object.
     * @return an {@link AnalyzedQuery} object.
     */
	public AnalyzedQuery<I> analyzeQuery(ProfiledQuery<I> qinfo) {
		// add the query to the statistics repository
		idxStats.addQuery(qinfo, matSet);
		
		reorganizeCandidates(qinfo.getCandidateSnapshot());
		
		wfa.newTask(qinfo);
		
		return new AnalyzedQuery<I>(qinfo, hotPartitions.bitSetArray());
	}
	
	/**
	 * Called by main thread to get a recommendation.
     * @return a list of indexes recommendations.
	 */
	public List<I> getRecommendation() {
		return wfa.getRecommendation();
	}

    /**
     * Bias the statistics of an index, in a candidate pool of indexes, in its favor.
     * @param index
     *      a {@link DBIndex index} object.
     * @param candSet
     *      a {@code snapshot} of a {@code candidate pool} of indexes.
     */
	public void positiveVote(I index, CandidatePool.Snapshot<I> candSet) {
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
     *      a {@link DBIndex index} object.
     */
	public void negativeVote(I index) {		
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
     *      a {@link ProfiledQuery} object.
     * @return
     *      the total cost of a {@code profiled query}.
     */
	public double currentCost(ProfiledQuery<I> qinfo) {
		return qinfo.totalCost(matSet.bitSet());
	}

    /**
     * Drop an index (i.e., remove it from the materialized index set) and return a {@code 0} cost
     * assuming there is no cost to drop.
     * @param index
     *      index to be dropped.
     * @return
     *      zero since there is no cost to drop.
     */
	public double drop(I index) {
		matSet.remove(index);
		return 0; // XXX: assuming no cost to drop
	}

    /**
     * adds an index to the materialized index set and returns its creation cost.
     * @param index
     *      index to be added.
     * @return either the index's {@code creationCost} or (if the index is already in the
     *      materialized index set) 0.
     */
	public double create(I index) {
		if (!matSet.contains(index)) {
			matSet.add(index);
			return index.creationCost();
		}
		return 0;
	}
	
	/**
	 * common code between positiveVote and processQuery
     * @param candSet
     *      a {@code snapshot} of {@code candidate pool of indexes}.
     */
	@SuppressWarnings({"RedundantTypeArguments"})
    private void reorganizeCandidates(CandidatePool.Snapshot<I> candSet) {
		// determine the hot set
		DynamicIndexSet<I> reqIndexes = new DynamicIndexSet<I>();
		for (I index : userHotSet) reqIndexes.add(index);
		for (I index : matSet) reqIndexes.add(index);
        final HotsetSelection<I> hotSelection = new HotsetSelection.StrictBuilder<I>(false)
                .candidateSet(candSet)
                .oldHotSet(hotSet)
                .requiredIndexSet(reqIndexes)
                .benefitFunction(idxStats)
                .maxSize(maxHotSetSize)
            .get();
		StaticIndexSet<I> newHotSet = 
			HotSetSelector.<I>chooseHotSet(hotSelection);
		
		// determine new partitioning
		// store into local variable, since we might reject it
        final InteractionSelection<I> interactionSelection = new InteractionSelection.StrictBuilder<I>()
                .newHotSet(newHotSet)
                .oldPartitions(hotPartitions)
                .doiFunction(idxStats)
                .maxNumStates(maxNumStates)
            .get();
		IndexPartitions<I> newHotPartitions = 
			InteractionSelector.<I>choosePartitions(interactionSelection);
		
		// commit hot set
		hotSet = newHotSet;
		if (hotSet.size() > maxHotSetSize) {
			maxHotSetSize = hotSet.size();
			console.info(
                    "Maximum number of monitored indexes has been automatically increased to "
                            + maxHotSetSize
            );
		}
		
		// commit new partitioning
		if (!newHotPartitions.equals(hotPartitions)) {
			hotPartitions = newHotPartitions;
			wfa.repartition(hotPartitions);
		}
	}

    @Override
    public String toString() {
        return new ToStringBuilder<CandidatesSelector<I>>(this)
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
