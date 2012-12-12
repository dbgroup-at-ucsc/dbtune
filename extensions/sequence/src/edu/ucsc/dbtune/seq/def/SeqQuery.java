package edu.ucsc.dbtune.seq.def;

import java.util.Hashtable;
import java.util.Vector;

import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.seq.bip.def.SeqInumQuery;
import edu.ucsc.dbtune.workload.SQLStatement;

public class SeqQuery {
    public int id; // for sorting after split and merge
    public String name;
    public SQLStatement sql;
    public double costWithoutIndex;
    public SeqIndex[] relevantIndices;
    public InumPreparedSQLStatement inum;
    public SeqInumQuery inumCached;
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
//        if (sql != null)
//            sb.append("\n" + sql.trim() + "\n");
        for (SeqIndex index : relevantIndices)
            sb.append(","+index.toString());
        return sb.toString();
    }
}
