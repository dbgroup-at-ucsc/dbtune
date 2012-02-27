package edu.ucsc.dbtune.seq.def;

import java.util.Hashtable;
import java.util.Vector;

public class SeqQuery {
    public int id; // for sorting after split and merge
    public String name;
    public String sql;
    public double costWithoutIndex;
    public SeqIndex[] relevantIndices;
    public Vector<SeqQueryCostWithIndex> costsWithIndices = new Vector<SeqQueryCostWithIndex>();
    public Hashtable<SeqConfiguration, Double> costCache = new Hashtable<SeqConfiguration, Double>();

    public int groupId; // for split

    public SeqQuery(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name + ",cost=" + costWithoutIndex);
        sb.append("\n" + sql.trim() + "\n");
        for (SeqIndex index : relevantIndices)
            sb.append(index.toString() + "\n");
        return sb.toString();
    }
}
