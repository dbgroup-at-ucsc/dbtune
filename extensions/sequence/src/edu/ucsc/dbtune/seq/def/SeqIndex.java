package edu.ucsc.dbtune.seq.def;

import java.util.HashSet;

import edu.ucsc.dbtune.metadata.Index;

public class SeqIndex {
    public String name;
    public Index index;
    public double createCost, dropCost;
    public long storageCost;

    // used for split
    public boolean markUsed = false;
    public int groupId; // slipt and merge
    public HashSet<SeqQuery> usedByQuery = new HashSet<SeqQuery>();
    public HashSet<SeqIndex> sameGroup = new HashSet<SeqIndex>();

    @Override
    public String toString() {
        return "[" + name + ",create=" + createCost+"]";
    }
}
