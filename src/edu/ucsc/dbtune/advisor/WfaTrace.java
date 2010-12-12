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

import edu.ucsc.dbtune.advisor.WorkFunctionAlgorithm.TotalWorkValues;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.spi.ibg.ProfiledQuery;
import edu.ucsc.dbtune.util.DefaultBitSet;
import edu.ucsc.dbtune.util.Debug;
import edu.ucsc.dbtune.util.Instances;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.util.List;

public class WfaTrace<I extends DBIndex<I>> {
	private List<TotalWorkValues>   wfValues    = Instances.newList();
	private List<Double>            sumNullCost = Instances.newList();

    /**
     * Construct a {@link WorkFunctionAlgorithm}'s work trace from this {@code algorithm}'s
     * total work values.
     * @param wf
     *      a {@link WorkFunctionAlgorithm}'s {@link TotalWorkValues total work values}.
     */
	public WfaTrace(TotalWorkValues wf) {
		wfValues.add(new TotalWorkValues(wf));
		sumNullCost.add(0.0);
	}

    /**
     * adds both a new {@link TotalWorkValues total work values} and the incurred {@code nullCost}.
     * @param wf
     *      a new {@link TotalWorkValues total work values}
     * @param nullCost
     *     incurred {@code nullCost}.
     */
	public void addValues(TotalWorkValues wf, double nullCost) {
		double prevSum = sumNullCost.get(sumNullCost.size()-1);
		
		wfValues.add(new TotalWorkValues(wf));
		sumNullCost.add(prevSum + nullCost);
	}


    /**
     * @return an array of {@link TotalWorkValues total work values}.
     */
	public TotalWorkValues[] getTotalWorkValues() {
		TotalWorkValues[] arr = new TotalWorkValues[wfValues.size()];
		return wfValues.toArray(arr);
	}

    /**
     * obtains the optimal schedule for an iterable partition object (e.g., list, set, or collection)
     * by filling bitSet with the optimal indexes of each corresponding profiled query found in the
     * iterable object.
     * @param parts
     *      available partitions.
     * @param queryCount
     *      number of profiled queries.
     * @param qinfos
     *      an iterable object containing profile queries.
     * @return
     *      the optimal schedule (i.e., an array of bitsets refering indexes' internalIds) of a
     *      number of profiled queries.
     */
	public DefaultBitSet[] optimalSchedule(IndexPartitions<I> parts, int queryCount, Iterable<ProfiledQuery<I>> qinfos) {
		
		// We will fill each BitSet with the optimal indexes for the corresponding query
		DefaultBitSet[] bss = new DefaultBitSet[queryCount];
		for (int i = 0; i < queryCount; i++) bss[i] = new DefaultBitSet();
		
		// this array holds the optimal schedule for a single partition
		// it has the state that each query should be processed in, plus the 
		// last state that should the system should be in after the last
		// query (which we expect to be the same as the state for the last
		// query, but that's not required). Each schedule, excluding the last
		// state, will be pushed into bss after it's computed
		DefaultBitSet[] partSchedule = new DefaultBitSet[queryCount+1];
		for (int q = 0; q <= queryCount; q++) partSchedule[q] = new DefaultBitSet();
		
		for (int subsetNum = 0; subsetNum < parts.subsetCount(); subsetNum++) {
			IndexPartitions.Subset<I> subset = parts.get(subsetNum);
			int[] indexIds = subset.indexIds();
			int stateCount = (int) subset.stateCount(); // XXX: this should return int
			
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

			Debug.assertion(bestSuccessor >= 0, "could not determine best final state");
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

    @Override
    public String toString() {
        return new ToStringBuilder<WfaTrace<I>>(this)
               .add("WFA totalWorkValues", wfValues)
               .add("sumNullCost", sumNullCost)
               .toString();
    }
}
