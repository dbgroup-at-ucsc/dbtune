package edu.ucsc.dbtune.seq.bip.def;

import java.util.HashSet;

import edu.ucsc.dbtune.metadata.Index;

public class SeqInumIndex {
    public int id;
    public String name;
    public Index index;
    public double createCost, dropCost;
    public int storageCost;

    public SeqInumIndex(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "[" + name + ",create=" + createCost + "]";
    }
}
