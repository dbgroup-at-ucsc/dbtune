/* **************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                         *
 *                                                                              *
 *   Licensed under the Apache License, Version 2.0 (the "License");            *
 *   you may not use this file except in compliance with the License.           *
 *   You may obtain a copy of the License at                                    *
 *                                                                              *
 *       http://www.apache.org/licenses/LICENSE-2.0                             *
 *                                                                              *
 *   Unless required by applicable law or agreed to in writing, software        *
 *   distributed under the License is distributed on an "AS IS" BASIS,          *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 *   See the License for the specific language governing permissions and        *
 *   limitations under the License.                                             *
 * **************************************************************************** */
package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.IBGPreparedSQLStatement;
import edu.ucsc.dbtune.util.IndexBitSet;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class WFALog implements Serializable {
    private static final long serialVersionUID = 3L;
    
    private LinkedList<Entry> list = new LinkedList<Entry>();
    
    WFALog() {
    }
    
    public class Entry implements Serializable {
        private static final long serialVersionUID = WFALog.serialVersionUID;
        double planCost;
        double maintenanceCost;
        double transitionCost;
        double nullCost;
        IndexBitSet[] partition;
        IndexBitSet recommendation;
        int numCandidates;
        int numWhatif;
        double logicOverhead;
    }


    public void add(AnalyzedQuery qinfo, IndexBitSet recommendation, double planCost, double maintCost, double transitionCost, double overhead) {
        Entry e = new Entry();
        e.planCost = planCost;
        e.maintenanceCost = maintCost;
        e.transitionCost = transitionCost;
        e.nullCost = qinfo.getProfileInfo().getIndexBenefitGraph().emptyCost();
        e.partition = qinfo.getPartition();
        e.recommendation = recommendation;

        int count = 0;
        for(Index idx : qinfo.getProfileInfo().getConfiguration()) {
            idx.getId();
            count++;
        }
        e.numCandidates = count;
        e.numWhatif = qinfo.getProfileInfo().getWhatIfCount();
        e.logicOverhead = overhead + qinfo.getProfileInfo().getIBGAnalysisTime();
        list.add(e);
    }

    public void add(IBGPreparedSQLStatement qinfo, 
                    IndexBitSet[] partition, IndexBitSet recommendation,
                    double planCost, double maintCost, double transitionCost, int whatifCount, double overhead) {
        Entry e = new Entry();
        e.planCost = planCost;
        e.maintenanceCost = maintCost;
        e.transitionCost = transitionCost;
        e.nullCost = qinfo.getIndexBenefitGraph().emptyCost();
        e.partition = partition;
        e.recommendation = recommendation;

        int count = 0;
        for(Index idx : qinfo.getConfiguration()) {
            idx.getId();
            count++;
        }
        e.numCandidates = count;
        e.numWhatif = whatifCount;
        e.logicOverhead = overhead + qinfo.getIBGAnalysisTime();
        list.add(e);
    }


    public void dump() {
        int i = 0;
        int c = 0;
        int w = 0;
        for (Entry e : list) {
            System.out.println("QUERY " + i);
            System.out.println("   "+"pcost\t"+e.planCost+"\t"
                                    +"mcost\t"+e.maintenanceCost+"\t"
                                    +"tcost\t"+e.transitionCost+"\t"
                                    +"ncost\t"+e.nullCost+"\t"
                                    +"cands\t"+(e.numCandidates-c)+"\t"
                                    +"whatif\t"+(e.numWhatif-w)+"\t"
                                    +"ohead\t"+e.logicOverhead);
            System.out.print("   partitions: ");
            for (IndexBitSet bs : e.partition) System.out.print(bs + " ");
            System.out.println();
            System.out.println("   rec: " + e.recommendation);
            System.out.println();
            
            c = e.numCandidates;
            w = e.numWhatif;
            ++i;
        }
    }


    public void dumpPerformance(PrintStream out) {
        int w = 0;

        //out.println("qcost\ttcost\tncost\tcands\twhatif\t");
        
        for (Entry e : list) {
            out.println(e.planCost+"\t"+
                               e.maintenanceCost+"\t"+
                               e.transitionCost+"\t"+
                               //e.nullCost+"\t"+
                               (e.numWhatif-w)+"\t"+
                               e.logicOverhead+"\t"+
                               e.numCandidates);
            
            w = e.numWhatif;
        }
    }

    // print all pairs i,j where i is a query and j is recommended at step i
    public void dumpHistory(PrintStream out) {
        int i = 0;
        IndexBitSet prev = new IndexBitSet();
        for (Entry e : list) {
            IndexBitSet bs = e.recommendation;
            for (int j = prev.nextSetBit(0); j >= 0; j = prev.nextSetBit(j+1)) {
                if (!bs.get(j)) out.println("Q"+i+"\tI"+j+"\t"+"\" ---drop \"");
            }
            for (int j = bs.nextSetBit(0); j >= 0; j = bs.nextSetBit(j+1)) {
                if (!prev.get(j)) out.println("Q"+i+"\tI"+j+"\t"+"\" +++ADD \"");
            }
            prev = bs;
            ++i;
            //out.println();
        }
    }
    
    /**
     * Writes the log for an experiment with a fixed candidate set. Iterates through the given 
     * {@code IBGPreparedSQLStatement} objects and obtains the costs for each of the queries, as 
     * well as each of the recommendations done for each.
     *
     * @param qinfos
     *     the profile of each of the queries that are contained in a workload. Each of them have 
     *     been filled with the data corresponding to the WFIT process and contain the historical information (in the 
     *     corresponding {@code IBG} object) executed at each step of the algorithm.
     * @param recs
     *     set of possible recommendations at each step; there should be a one to one correspondence 
     *     between an {@code IndexBitSet} and a profiled query.
     * @param snapshot
     *     set of indexes that are created/dropped during the execution of the workload
     * @param parts
     *     partitioning of the snapshot
     * @param overheads
     *     overheads
     */
    public static WFALog generateFixed(
            List<IBGPreparedSQLStatement> qinfos,
            IndexBitSet[]                recs,
            Snapshot            snapshot,
            IndexPartitions     parts,
            double[]                     overheads)
    {
        IndexBitSet[] partIndexBitSets;
        
        int         queryCount = qinfos.size();
        WFALog      log        = new WFALog();
        IndexBitSet scratch    = new IndexBitSet();

        if( parts != null ) {
            partIndexBitSets = parts.bitSetArray();
        } else {
            partIndexBitSets = null;
        }

        for (int q = 0; q < queryCount; q++) {

            IBGPreparedSQLStatement qinfo     = qinfos.get(q);
            IndexBitSet            state     = recs[q];
            IndexBitSet            prevState = q == 0 ? new IndexBitSet() : recs[q-1];
            double                 planCost;

            if (parts != null) {
                planCost = parts.theoreticalCost(qinfo, state, scratch);
            } else {
                planCost = qinfo.planCost(state);
            }

            double maintCost      = qinfo.maintenanceCost(state);
            double transitionCost = WorkFunctionAlgorithm.transitionCost(snapshot, prevState, state);

            log.add(qinfo, partIndexBitSets, state, planCost, maintCost, transitionCost, qinfo.getWhatIfCount(), overheads[q]);
        }
        
        return log;
    }

    public static WFALog generateDual(
            List<IBGPreparedSQLStatement> qinfos,
            IndexBitSet[] optRecs,
            IndexBitSet[] wfitRecs,
            Snapshot snapshot,
            IndexPartitions parts,
            double[] overheads)
    {
        IndexBitSet[] partIndexBitSets;
        int queryCount = qinfos.size();
        WFALog log = new WFALog();

        IndexBitSet scratch = new IndexBitSet();
        IndexBitSet emptyRec = new IndexBitSet();
        IndexBitSet indicesToCreate = new IndexBitSet();
        IndexBitSet indicesToDrop = new IndexBitSet();
        IndexBitSet materialized1 = new IndexBitSet();
        IndexBitSet materialized2 = new IndexBitSet();
        IndexBitSet materialized3 = new IndexBitSet();

        if(parts != null)
            partIndexBitSets = parts.bitSetArray();
        else
            partIndexBitSets = null;

        for (int q = 0; q < queryCount; q++) {
            IBGPreparedSQLStatement qinfo = qinfos.get(q);

            materialized1.set(materialized3);
            
            // process OPT's recommendations
            IndexBitSet optState = optRecs[q];
            IndexBitSet prevOptState = q == 0 ? emptyRec : optRecs[q-1];
            indicesToCreate.set(optState);
            indicesToCreate.andNot(prevOptState);
            indicesToDrop.set(prevOptState);
            indicesToDrop.andNot(optState);
            materialized2.set(materialized1);
            materialized2.or(indicesToCreate);
            materialized2.andNot(indicesToDrop);
            
            // process WFIT's recommendations
            IndexBitSet state = wfitRecs[q];
            IndexBitSet prevState = q == 0 ? emptyRec : wfitRecs[q-1];
            indicesToCreate.set(state);
            indicesToCreate.andNot(prevState);
            indicesToDrop.set(prevState);
            indicesToDrop.andNot(state);
            materialized3.set(materialized2);
            materialized3.or(indicesToCreate);
            materialized3.andNot(indicesToDrop);
            
            double planCost;
            if (parts != null) {
                planCost = parts.theoreticalCost(qinfo, materialized3, scratch);
            }
            else {
                planCost = qinfo.planCost(materialized3);
            }
            double maintCost = qinfo.maintenanceCost(materialized3);
            double transitionCost = WorkFunctionAlgorithm.transitionCost(snapshot, materialized1, materialized2) +
                                    WorkFunctionAlgorithm.transitionCost(snapshot, materialized2, materialized3);
            log.add(qinfo, partIndexBitSets, materialized3.clone(), planCost, maintCost, transitionCost, qinfo.getWhatIfCount(), overheads[q]);
        }
        
        return log;
    }

    public static WFALog generateDynamic(java.util.List<AnalyzedQuery> qinfos, IndexBitSet[] recs, double[] overheads) {
        int queryCount = qinfos.size();
        WFALog log = new WFALog();
        
        for (int q = 0; q < queryCount; q++) {
            AnalyzedQuery qinfo = qinfos.get(q);
            IndexBitSet state = recs[q];
            IndexBitSet prevState = q == 0 ? new IndexBitSet() : recs[q-1];
            double planCost = qinfo.getProfileInfo().planCost(state);
            double maintCost = qinfo.getProfileInfo().maintenanceCost(state);
            double transitionCost = WorkFunctionAlgorithm.transitionCost(qinfo.getProfileInfo().getConfiguration(), prevState, state);
            log.add(qinfo, state, planCost, maintCost, transitionCost, overheads[q]);
        }
        
        return log;
    }

    public int entryCount() {
        return list.size();
    }

    public Entry getEntry(int i) {
        return list.get(i);
    }

    public int countRepartitions() {
        Entry p = null;
        int c = 0;
        for (Entry e : list) {
            if (p != null && !partsEqual(p.partition, e.partition)) {
                c++;
                for (IndexBitSet bs : e.partition)
                    System.out.println(bs);
                System.out.println();
            }
            p = e;
        }
        return c;
    }
    
    private boolean partsEqual(IndexBitSet[] arr1, IndexBitSet[] arr2) {
        if (arr1.length != arr2.length) return false;
        
        for (IndexBitSet bs1 : arr1) {
            boolean foundMatch = false;
            for (IndexBitSet bs2 : arr2) {
                if (bs1.equals(bs2)) { foundMatch = true; break; }
            }
            if (!foundMatch) return false;
        }
        return true;
    }
}

