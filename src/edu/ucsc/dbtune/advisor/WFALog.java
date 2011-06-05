package edu.ucsc.dbtune.advisor;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.util.IndexBitSet;

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


    public void add(AnalyzedQuery<?> qinfo, IndexBitSet recommendation, double planCost, double maintCost, double transitionCost, double overhead) {
        Entry e = new Entry();
        e.planCost = planCost;
        e.maintenanceCost = maintCost;
        e.transitionCost = transitionCost;
        e.nullCost = qinfo.getProfileInfo().getIndexBenefitGraph().emptyCost();
        e.partition = qinfo.getPartition();
        e.recommendation = recommendation;
        e.numCandidates = qinfo.getProfileInfo().getCandidateSnapshot().maxInternalId() + 1;
        e.numWhatif = qinfo.getProfileInfo().getWhatIfCount();
        e.logicOverhead = overhead + qinfo.getProfileInfo().getIBGAnalysisTime();
        list.add(e);
    }

    public void add(ProfiledQuery<?> qinfo, 
                    IndexBitSet[] partition, IndexBitSet recommendation,
                    double planCost, double maintCost, double transitionCost, int whatifCount, double overhead) {
        Entry e = new Entry();
        e.planCost = planCost;
        e.maintenanceCost = maintCost;
        e.transitionCost = transitionCost;
        e.nullCost = qinfo.getIndexBenefitGraph().emptyCost();
        e.partition = partition;
        e.recommendation = recommendation;
        e.numCandidates = qinfo.getCandidateSnapshot().maxInternalId() + 1;
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
        int i = 1;
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
     * Writes the log for an experiment with a fixed candidate set. Iterates through the given {@code ProfiledQuery} objects 
     * and obtains the costs for each of the queries, as well as each of the recommendations done at each.
     *
     * @param qinfos
     *     represents a workload where each query has been profiled and used in WFIT, containing the historical information 
     *     (in the corresponding {@link IBG} object) executed at each step of the algorithm.
     * @param recs
     *     set of possible recommendations at each step; there should be a one to one correspondence between an {@code 
     *     IndexBitSet} and a profiled query. This means that
     * @param snapshot
     *     set of indexes that are created/dropped during the execution of the workload
     * @param parts
     *     partitioning of the snapshot
     * @param overheads
     *     overheads
     */
    public static WFALog generateFixed(
            List<ProfiledQuery<DBIndex>> qinfos,
            IndexBitSet[]                recs,
            Snapshot<DBIndex>            snapshot,
            IndexPartitions<DBIndex>     parts,
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

            ProfiledQuery<DBIndex> qinfo     = qinfos.get(q);
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
            List<ProfiledQuery<DBIndex>> qinfos,
            IndexBitSet[] optRecs,
            IndexBitSet[] wfitRecs,
            Snapshot<DBIndex> snapshot,
            IndexPartitions<DBIndex> parts,
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
            ProfiledQuery<DBIndex> qinfo = qinfos.get(q);

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

    public static WFALog generateDynamic(java.util.List<AnalyzedQuery<DBIndex>> qinfos, IndexBitSet[] recs, double[] overheads) {
        int queryCount = qinfos.size();
        WFALog log = new WFALog();
        
        for (int q = 0; q < queryCount; q++) {
            AnalyzedQuery<DBIndex> qinfo = qinfos.get(q);
            IndexBitSet state = recs[q];
            IndexBitSet prevState = q == 0 ? new IndexBitSet() : recs[q-1];
            double planCost = qinfo.getProfileInfo().planCost(state);
            double maintCost = qinfo.getProfileInfo().maintenanceCost(state);
            double transitionCost = WorkFunctionAlgorithm.transitionCost(qinfo.getProfileInfo().getCandidateSnapshot(), prevState, state);
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
