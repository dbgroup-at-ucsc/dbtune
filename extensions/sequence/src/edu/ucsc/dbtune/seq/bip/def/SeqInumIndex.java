package edu.ucsc.dbtune.seq.bip.def;

import java.util.HashSet;

import edu.ucsc.dbtune.metadata.Index;

public class SeqInumIndex {
    public String name;
    public Index index;
    public double createCost, dropCost;
    public int storageCost;

    // used for split
    public boolean markUsed = false;
    public int groupId; // slipt and merge
    public HashSet<SeqInumQuery> usedByQuery = new HashSet<SeqInumQuery>();

    @Override
    public String toString() {
        return "[" + name + ",create=" + createCost+"]";
    }
}
