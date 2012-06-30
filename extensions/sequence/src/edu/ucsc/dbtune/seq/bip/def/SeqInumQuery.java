package edu.ucsc.dbtune.seq.bip.def;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.utils.Rx;
import edu.ucsc.dbtune.workload.SQLStatement;

public class SeqInumQuery implements Serializable {
    public int id;
    public String name;
    public SQLStatement sql;
    public SeqInumPlan[] plans;
    public SeqInumIndex[] relevantIndices;
    public double baseTableUpdateCost;

    // the following variables are only for debugging purposes
    // and should be removed when the system works correctly
    public SeqInumPlan selectedPlan;
    public double transitionCost = 0;

    public SeqInumQuery(int id) {
        this.id = id;
    }

    public void save(Rx rx) {
        rx.setAttribute("id", id);
        rx.setAttribute("name", name);
        rx.createChild("sql", sql.getSQL());
        rx.setAttribute("baseTableUpdateCost", baseTableUpdateCost);
        for (SeqInumPlan plan : plans) {
            plan.save(rx.createChild("plan"));
        }
    }

    public SeqInumQuery(SeqInumCost cost,Rx rx) {
        id = rx.getIntAttribute("id");
        name = rx.getAttribute("name");
        Rx[] rs=rx.findChilds("plan");
        plans=new SeqInumPlan[rs.length];
        for (int i=0;i<rs.length;i++) {
            plans[i]=new SeqInumPlan(cost,this,rs[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        // if (sql != null)
        // sb.append("\n" + sql.trim() + "\n");
        for (SeqInumIndex index : relevantIndices)
            sb.append(index.toString() + "\n");
        return sb.toString();
    }
}
