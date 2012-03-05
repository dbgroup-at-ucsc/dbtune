package edu.ucsc.dbtune.seq.bip.def;

import java.util.Hashtable;
import java.util.Vector;

public class SeqInumQuery {
    public int id;
    public String name;
    public String sql;
    public SeqInumPlan[] plans;
    public SeqInumIndex[] relevantIndices;

    public SeqInumQuery(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name );
        if (sql != null)
            sb.append("\n" + sql.trim() + "\n");
        for (SeqInumIndex index : relevantIndices)
            sb.append(index.toString() + "\n");
        return sb.toString();
    }
}
