package edu.ucsc.dbtune.seq.bip.def;

import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;

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
    public Hashtable<SeqInumIndex, Double> updateCosts = new Hashtable<SeqInumIndex, Double>();

    // the following variables are only for debugging purposes
    // and should be removed when the system works correctly
    public SeqInumPlan selectedPlan;
    public double transitionCost = 0;

    // performance variables
    public double timeUsedToLoad;
    
    //table size of updated table
    public int tableSize;

    public SeqInumQuery(int id) {
        this.id = id;
    }

    public double cost(SeqInumIndex[] indexes) {
        double min = plans.length == 0 ? 0 : Double.MAX_VALUE;
        for (SeqInumPlan plan : plans) {
            double cost = plan.cost(indexes);
            if (cost < min)
                min = cost;
        }
        min += baseTableUpdateCost;
        for (SeqInumIndex index : indexes) {
            Double updateCost = updateCosts.get(index);
            if (updateCost != null && updateCost > 0)
                min += updateCost;
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
        rx.createChild("sqlType", sql.getSQLCategory().name());
        rx.createChild("tableSize", tableSize);
        rx.createChild("timeUsedToLoad", timeUsedToLoad);
        rx.setAttribute("baseTableUpdateCost", baseTableUpdateCost);
        Rx update = rx.createChild("updateCosts");
        for (SeqInumIndex index : updateCosts.keySet()) {
            Rx rx2 = update.createChild("index");
            rx2.setAttribute("name", index.name);
            rx2.setAttribute("updateCost", updateCosts.get(index));
        }
        for (SeqInumPlan plan : plans) {
            plan.save(rx.createChild("plan"));
        }
    }

    public SeqInumQuery(Hashtable<String, SeqInumIndex> indexHash, Rx rx) {
        id = rx.getIntAttribute("id");
        name = rx.getAttribute("name");
        sql = new SQLStatement(rx.getChildText("sql"));
        tableSize = rx.getIntAttribute("tableSize");
        timeUsedToLoad = rx.getChildDoubleContent("timeUsedToLoad");
        baseTableUpdateCost = rx.getDoubleAttribute("baseTableUpdateCost");
        Rx update = rx.findChild("updateCosts");
        for (Rx rx2 : update.findChilds("index")) {
            updateCosts.put(indexHash.get(rx2.getAttribute("name")), rx2
                    .getDoubleAttribute("updateCost"));
        }
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
