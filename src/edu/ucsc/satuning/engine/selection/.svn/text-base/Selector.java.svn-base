package edu.ucsc.satuning.engine.selection;

import java.util.List;

import edu.ucsc.satuning.Configuration;
import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.engine.AnalyzedQuery;
import edu.ucsc.satuning.engine.ProfiledQuery;
import edu.ucsc.satuning.engine.CandidatePool.Snapshot;
import edu.ucsc.satuning.util.Debug;

public class Selector<I extends DBIndex<I>> {
	private final IndexStatistics<I> idxStats;
	private final WorkFunctionAlgorithm<I> wfa;
	private final DynamicIndexSet<I> matSet;
	private StaticIndexSet<I> hotSet;
	private final DynamicIndexSet<I> userHotSet;
	private IndexPartitions<I> hotPartitions;
	private int maxHotSetSize = Configuration.maxHotSetSize;
	private int maxNumStates = Configuration.maxNumStates;
	
	public Selector() {
		idxStats = new IndexStatistics<I>();
		wfa = new WorkFunctionAlgorithm<I>();
		hotSet = new StaticIndexSet<I>();
		userHotSet = new DynamicIndexSet<I>();
		matSet = new DynamicIndexSet<I>();
		hotPartitions = new IndexPartitions<I>(hotSet); 
	}

	/*
	 * Perform the per-query tasks that are done after profiling
	 */
	public AnalyzedQuery<I> analyzeQuery(ProfiledQuery<I> qinfo) {
		// add the query to the statistics repository
		idxStats.addQuery(qinfo, matSet);
		
		reorganizeCandidates(qinfo.candidateSet);
		
		wfa.newTask(qinfo);
		
		return new AnalyzedQuery<I>(qinfo, hotPartitions.bitSetArray());
	}
	
	/*
	 * Called by main thread to get a recommendation
	 */
	public List<I> getRecommendation() {
		return wfa.getRecommendation();
	}
	
	public void positiveVote(I index, Snapshot<I> candSet) {
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

	public double currentCost(ProfiledQuery<I> qinfo) {
		return qinfo.totalCost(matSet.bitSet());
	}

	public double drop(I index) {
		matSet.remove(index);
		return 0; // XXX: assuming no cost to drop
	}


	public double create(I index) {
		if (!matSet.contains(index)) {
			matSet.add(index);
			return index.creationCost();
		}
		return 0;
	}
	
	/* 
	 * common code between positiveVote and processQuery 
	 */
	private void reorganizeCandidates(Snapshot<I> candSet) {
		// determine the hot set
		DynamicIndexSet<I> reqIndexes = new DynamicIndexSet<I>();
		for (I index : userHotSet) reqIndexes.add(index);
		for (I index : matSet) reqIndexes.add(index);
		StaticIndexSet<I> newHotSet = 
			HotSetSelector.<I>chooseHotSet(candSet, hotSet, reqIndexes, idxStats, maxHotSetSize, false);
		
		// determine new partitioning
		// store into local variable, since we might reject it
		IndexPartitions<I> newHotPartitions = 
			InteractionSelector.<I>choosePartitions(newHotSet, hotPartitions, idxStats, maxNumStates);
		
		// commit hot set
		hotSet = newHotSet;
		if (hotSet.size() > maxHotSetSize) {
			maxHotSetSize = hotSet.size();
			Debug.logNotice("Maximum number of monitored indexes has been automatically increased to " + maxHotSetSize);
		}
		
		// commit new partitioning
		if (!newHotPartitions.equals(hotPartitions)) {
			hotPartitions = newHotPartitions;
			wfa.repartition(hotPartitions);
		}
	}
}
