package edu.ucsc.dbtune.advisor.wfit;

import java.util.List;

import edu.ucsc.dbtune.advisor.wfit.CandidatePool.Snapshot;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;

public class Selector {
	private final IndexStatistics idxStats;
	private final WorkFunctionAlgorithm wfa;
	private final DynamicIndexSet matSet;
	private StaticIndexSet hotSet;
	private final DynamicIndexSet userHotSet;
	private IndexPartitions hotPartitions;
    private int maxHotSetSize = Environment.getInstance().getMaxNumIndexes(); // hotSet size
    private int maxNumStates = Environment.getInstance().getMaxNumStates();
	
	public Selector() {
		idxStats = new IndexStatistics();
		wfa = new WorkFunctionAlgorithm();
		hotSet = new StaticIndexSet();
		userHotSet = new DynamicIndexSet();
		matSet = new DynamicIndexSet();
		hotPartitions = new IndexPartitions(hotSet); 
	}

	/*
	 * Perform the per-query tasks that are done after profiling
	 */
	public AnalyzedQuery analyzeQuery(ProfiledQuery qinfo) {
		// add the query to the statistics repository
		idxStats.addQuery(qinfo, matSet);
		
		reorganizeCandidates(qinfo.candidateSet);
		
		wfa.newTask(qinfo);
		
		return new AnalyzedQuery(qinfo, hotPartitions.bitSetArray());
	}
	
	/*
	 * Called by main thread to get a recommendation
	 */
	public List<Index> getRecommendation() {
		return wfa.getRecommendation();
	}
	
	public void positiveVote(Index index, Snapshot candSet) {
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

	public double currentCost(ProfiledQuery qinfo) {
		return qinfo.cost(matSet.bitSet());
	}

	public double drop(Index index) {
		matSet.remove(index);
		return 0; // XXX: assuming no cost to drop
	}


	public double create(Index index) {
		if (!matSet.contains(index)) {
			matSet.add(index);
            return index.getCreationCost();
		}
		return 0;
	}
	
	/* 
	 * common code between positiveVote and processQuery 
	 */
	private void reorganizeCandidates(Snapshot candSet) {
		// determine the hot set
		DynamicIndexSet reqIndexes = new DynamicIndexSet();
		for (Index index : userHotSet) reqIndexes.add(index);
		for (Index index : matSet) reqIndexes.add(index);
		StaticIndexSet newHotSet = 
			HotSetSelector.chooseHotSet(candSet, hotSet, reqIndexes, idxStats, maxHotSetSize);
		
		// determine new partitioning
		// store into local variable, since we might reject it
		IndexPartitions newHotPartitions = 
			InteractionSelector.choosePartitions(newHotSet, hotPartitions, idxStats, maxNumStates);
		
		// commit hot set
		hotSet = newHotSet;
		if (hotSet.size() > maxHotSetSize) {
			maxHotSetSize = hotSet.size();
            //Debug.logNotice("Maximum number of monitored indexes has been automatically increased 
            //to " + maxHotSetSize);
		}
		
		// commit new partitioning
		if (!newHotPartitions.equals(hotPartitions)) {
			hotPartitions = newHotPartitions;
			wfa.repartition(hotPartitions);
		}
	}
}
