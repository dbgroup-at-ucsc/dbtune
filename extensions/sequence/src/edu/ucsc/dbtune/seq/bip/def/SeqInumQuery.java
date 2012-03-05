package edu.ucsc.dbtune.seq.bip.def;

import java.util.Hashtable;
import java.util.Vector;

import edu.ucsc.dbtune.workload.SQLStatement;

public class SeqInumQuery {
    public int id;
    public String name;
    public SQLStatement sql;
    public SeqInumPlan[] plans;
    public SeqInumIndex[] relevantIndices;

    //the following variables are only for debugging purposes 
    //and should be removed when the system works correctly
    public SeqInumPlan selectedPlan;
    public double transitionCost=0;
    
    public SeqInumQuery(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name );
//        if (sql != null)
//            sb.append("\n" + sql.trim() + "\n");
        for (SeqInumIndex index : relevantIndices)
            sb.append(index.toString() + "\n");
        return sb.toString();
    }
}
