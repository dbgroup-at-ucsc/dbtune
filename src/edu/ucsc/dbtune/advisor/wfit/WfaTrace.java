package edu.ucsc.dbtune.advisor.wfit;

import edu.ucsc.dbtune.advisor.wfit.IndexPartitions.Subset;
import edu.ucsc.dbtune.advisor.wfit.WorkFunctionAlgorithm.TotalWorkValues;

public class WfaTrace {
//  private BitSet[] parts;
    private java.util.ArrayList<TotalWorkValues> wfValues = new java.util.ArrayList<TotalWorkValues>();
    private java.util.ArrayList<Double> sumNullCost = new java.util.ArrayList<Double>();

    public WfaTrace(IndexPartitions parts0, TotalWorkValues wf) {
//      parts = parts0.bitSetArray();
        wfValues.add(new TotalWorkValues(wf));
        sumNullCost.add(0.0);
    }
    
    public void addValues(TotalWorkValues wf, double nullCost) {
        double prevSum = sumNullCost.get(sumNullCost.size()-1);
        
        wfValues.add(new TotalWorkValues(wf));
        sumNullCost.add(prevSum + nullCost);
    }

//  public double minWfValue(int q) {
//      if (q < 0)
//          throw new IllegalArgumentException("query number must not be negative");
//      if (q >= wfValues.size())
//          throw new IllegalArgumentException("query number too large");
//      
//      TotalWorkValues wf = wfValues.get(q);
//      int subsetCount = parts.length;
//      double minSum = 0;
//      for (int subsetNum = 0; subsetNum < subsetCount; subsetNum++) {
//          BitSet subset = parts[subsetNum];
//          int stateCount = 1 << subset.cardinality();
//          double minSubValue = Double.POSITIVE_INFINITY;
//          for (int s = 0; s < stateCount; s++) {
//              minSubValue = Math.min(minSubValue, wf.get(subsetNum, s));
//          }
//          minSum += minSubValue;
//      }
//      double minValue = minSum - (subsetCount - 1) * sumNullCost.get(q);
//      return minValue;
//  }

    public TotalWorkValues[] getTotalWorkValues() {
        TotalWorkValues[] arr = new TotalWorkValues[wfValues.size()];
        return wfValues.toArray(arr);
    }
    
//  public double[] getMinWfValues() {
//      int valcount = wfValues.size();
//      double[] arr = new double[valcount];
//      for (int i = 0; i < valcount; i++) {
//          arr[i] = minWfValue(i);
//      }
//      return arr;
//  }
    

    public BitSet[] optimalSchedule(IndexPartitions parts, int queryCount, ProfiledQuery[] qinfos) {
        
        // We will fill each BitSet with the optimal indexes for the corresponding query
        BitSet[] bss = new BitSet[queryCount];
        for (int i = 0; i < queryCount; i++) bss[i] = new BitSet();
        
        // this array holds the optimal schedule for a single partition
        // it has the state that each query should be processed in, plus the 
        // last state that should the system should be in after the last
        // query (which we expect to be the same as the state for the last
        // query, but that's not required). Each schedule, excluding the last
        // state, will be pushed into bss after it's computed
        BitSet[] partSchedule = new BitSet[queryCount+1];
        for (int q = 0; q <= queryCount; q++) partSchedule[q] = new BitSet();
        
        for (int subsetNum = 0; subsetNum < parts.subsetCount(); subsetNum++) {
            Subset subset = parts.get(subsetNum);
            int[] indexIds = subset.indexIds();
            int stateCount = (int) subset.stateCount(); // XXX: this should return int
            
            // get the best final state
            int bestSuccessor = -1;

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

            //Debug.assertion(bestSuccessor >= 0, "could not determine best final state");
            //if (bestSuccessor >= 0)
                //throw new RuntimeException("could not determine best final state");
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
