package edu.ucsc.dbtune.advisor.wfit;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.LinkedList;

import edu.ucsc.dbtune.advisor.wfit.CandidatePool.Snapshot;

//CHECKSTYLE:OFF
public class WFALog implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private LinkedList<Entry> list = new LinkedList<Entry>();
    
    WFALog() {
    }
    
    public class Entry implements Serializable {
        private static final long serialVersionUID = WFALog.serialVersionUID;
        double queryCost;
        double transitionCost;
        double nullCost;
        BitSet[] partition;
        BitSet recommendation;
        int numCandidates;
        int numWhatif;
    }


    public void add(AnalyzedQuery qinfo, BitSet recommendation, double queryCost, double transitionCost) {
        Entry e = new Entry();
        e.queryCost = queryCost;
        e.transitionCost = transitionCost;
        e.nullCost = qinfo.profileInfo.ibg.emptyCost();
        e.partition = qinfo.partition;
        e.recommendation = recommendation;
        e.numCandidates = qinfo.profileInfo.candidateSet.maxInternalId() + 1;
        e.numWhatif = qinfo.profileInfo.whatifCount;
        list.add(e);
    }

    public void add(ProfiledQuery qinfo, 
                    BitSet[] partition, BitSet recommendation,
                    double queryCost, double transitionCost, int whatifCount) {
        Entry e = new Entry();
        e.queryCost = queryCost;
        e.transitionCost = transitionCost;
        e.nullCost = qinfo.ibg.emptyCost();
        e.partition = partition;
        e.recommendation = recommendation;
        e.numCandidates = qinfo.candidateSet.maxInternalId() + 1;
        e.numWhatif = whatifCount;
        list.add(e);
    }


    public void dump() {
        int i = 0;
        int c = 0;
        int w = 0;
        for (Entry e : list) {
            System.out.println("QUERY " + i);
            System.out.println("   "+"qcost\t"+e.queryCost+"\t"
                                    +"tcost\t"+e.transitionCost+"\t"
                                    +"ncost\t"+e.nullCost+"\t"
                                    +"cands\t"+(e.numCandidates-c)+"\t"
                                    +"whatif\t"+(e.numWhatif-w));
            System.out.print("   partitions: ");
            for (BitSet bs : e.partition) System.out.print(bs + " ");
            System.out.println();
            System.out.println("   rec: " + e.recommendation);
            System.out.println();
            
            c = e.numCandidates;
            w = e.numWhatif;
            ++i;
        }
    }


    public void dumpPerformance(PrintStream out) {
        int c = 0;
        int w = 0;

        //out.println("qcost\ttcost\tncost\tcands\twhatif\t");
        
        for (Entry e : list) {
            out.println(e.queryCost+"\t"+
                               e.transitionCost+"\t"+
                               e.nullCost+"\t"+
                               (e.numCandidates-c)+"\t"+
                               (e.numWhatif-w));
            
            c = e.numCandidates;
            w = e.numWhatif;
        }
    }

    // print all pairs i,j where i is a query and j is recommended at step i
    public void dumpHistory(PrintStream out) {
        int i = 1;
        BitSet prev = new BitSet();
        for (Entry e : list) {
            BitSet bs = e.recommendation;
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
    
    // write whole log for an experiment with a fixed candidate set
    public static WFALog generateFixed(ProfiledQuery[] qinfos, BitSet[] recs, Snapshot snapshot, IndexPartitions parts) {
        int queryCount = qinfos.length;
        WFALog log = new WFALog();

        BitSet[] partBitSets = parts.bitSetArray();
        for (int q = 0; q < queryCount; q++) {
            ProfiledQuery qinfo = qinfos[q];
            BitSet state = recs[q];
            BitSet prevState = q == 0 ? new BitSet() : recs[q-1];
            double queryCost = qinfo.cost(state);
            double transitionCost = WorkFunctionAlgorithm.transitionCost(snapshot, prevState, state);
            log.add(qinfo, partBitSets, state, queryCost, transitionCost, qinfo.whatifCount);
        }
        
        return log;
    }

    public static WFALog generateDual(ProfiledQuery[] qinfos, BitSet[] optRecs, BitSet[] wfitRecs, Snapshot snapshot, IndexPartitions parts) {
        int queryCount = qinfos.length;
        WFALog log = new WFALog();

        BitSet emptyRec = new BitSet();
        BitSet indicesToCreate = new BitSet();
        BitSet indicesToDrop = new BitSet();
        BitSet materialized1 = new BitSet();
        BitSet materialized2 = new BitSet();
        BitSet materialized3 = new BitSet();
        BitSet[] partBitSets = parts.bitSetArray();
        for (int q = 0; q < queryCount; q++) {
            ProfiledQuery qinfo = qinfos[q];

            materialized1.set(materialized3);
            
            // process OPT's recommendations
            BitSet optState = optRecs[q];
            BitSet prevOptState = q == 0 ? emptyRec : optRecs[q-1];
            indicesToCreate.set(optState);
            indicesToCreate.andNot(prevOptState);
            indicesToDrop.set(prevOptState);
            indicesToDrop.andNot(optState);
            materialized2.set(materialized1);
            materialized2.or(indicesToCreate);
            materialized2.andNot(indicesToDrop);
            
            // process WFIT's recommendations
            BitSet state = wfitRecs[q];
            BitSet prevState = q == 0 ? emptyRec : wfitRecs[q-1];
            indicesToCreate.set(state);
            indicesToCreate.andNot(prevState);
            indicesToDrop.set(prevState);
            indicesToDrop.andNot(state);
            materialized3.set(materialized2);
            materialized3.or(indicesToCreate);
            materialized3.andNot(indicesToDrop);
            
            double queryCost = qinfo.cost(materialized3);
            double transitionCost = WorkFunctionAlgorithm.transitionCost(snapshot, materialized1, materialized2) +
                                    WorkFunctionAlgorithm.transitionCost(snapshot, materialized2, materialized3);
            log.add(qinfo, partBitSets, materialized3.clone(), queryCost, transitionCost, qinfo.whatifCount);
        }
        
        return log;
    }

    public static WFALog generateDynamic(AnalyzedQuery[] qinfos, BitSet[] recs) {
        int queryCount = qinfos.length;
        WFALog log = new WFALog();
        
        for (int q = 0; q < queryCount; q++) {
            AnalyzedQuery qinfo = qinfos[q];
            BitSet state = recs[q];
            BitSet prevState = q == 0 ? new BitSet() : recs[q-1];
            double queryCost = qinfo.profileInfo.cost(state);
            double transitionCost = WorkFunctionAlgorithm.transitionCost(qinfo.profileInfo.candidateSet, prevState, state);
            log.add(qinfo, state, queryCost, transitionCost);
        }
        
        return log;
    }

    public int entryCount() {
        return list.size();
    }

    public Entry getEntry(int i) {
        return list.get(i);
    }
}
//CHECKSTYLE:ON
