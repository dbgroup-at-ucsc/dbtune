package edu.ucsc.dbtune.seq.bip.def;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.util.Rx;
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

    // performance variables
    public double timeUsedToLoad;

    public SeqInumQuery(int id) {
        this.id = id;
    }

    public double cost(SeqInumIndex[] indexes) {
        double min = Double.MAX_VALUE;
        for (SeqInumPlan plan : plans) {
            double cost = plan.cost(indexes);
            if (cost < min)
                min = cost;
        }
        return min;
    }

    public int totalIndexes() {
        int n = 0;
        for (SeqInumPlan p : plans)
            n += p.totalIndexes();
        return n;
    }

    public int totalSlots() {
        int n = 0;
        for (SeqInumPlan p : plans)
            n += p.slots.length;
        return n;
    }

    public void save(Rx rx) {
        rx.setAttribute("id", id);
        rx.setAttribute("name", name);
        rx.createChild("sql", sql.getSQL());
        rx.createChild("timeUsedToLoad", timeUsedToLoad);
        rx.setAttribute("baseTableUpdateCost", baseTableUpdateCost);
        for (SeqInumPlan plan : plans) {
            plan.save(rx.createChild("plan"));
        }
    }

    public SeqInumQuery(Hashtable<String, SeqInumIndex> indexHash, Rx rx) {
        id = rx.getIntAttribute("id");
        name = rx.getAttribute("name");
        sql = new SQLStatement(rx.getChildText("sql"));
        timeUsedToLoad = rx.getChildDoubleContent("timeUsedToLoad");
        Rx[] rs = rx.findChilds("plan");
        Vector<SeqInumPlan> vs = new Vector<SeqInumPlan>();
        for (int i = 0; i < rs.length; i++) {
            SeqInumPlan plan = new SeqInumPlan(indexHash, this, rs[i]);
            if (plan.valid)
                vs.add(plan);
        }
        plans = vs.toArray(new SeqInumPlan[vs.size()]);
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