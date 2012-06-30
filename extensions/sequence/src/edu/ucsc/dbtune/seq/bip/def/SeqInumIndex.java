package edu.ucsc.dbtune.seq.bip.def;

import java.io.Serializable;
import java.util.HashSet;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.seq.utils.Rx;

public class SeqInumIndex implements Serializable {
    public int id;
    public String name;
    public Index index;
    public double createCost, dropCost;
    public double createCostDB2;
    public String createCostSQL;
    public double storageCost;

    public double indexBenefit; // with this index - with no index
    public double indexBenefit2; // with all index - without this index

    public SeqInumIndex(int id) {
        this.id = id;
    }

    public void save(Rx rx) {
        rx.setAttribute("id", id);
        rx.setAttribute("name", name);
        rx.createChild("createCost", createCost);
        rx.createChild("createCostDB2", createCostDB2);
        if (createCostSQL != null)
            rx.createChild("createCostSQL", createCostSQL);
        rx.createChild("dropCost", dropCost);
        rx.createChild("storageCost", storageCost);
        rx.createChild("indexBenefit", indexBenefit);
        rx.createChild("indexBenefit2", indexBenefit2);
        if (index != null) {
            Rx rx2 = rx.createChild("index");
            rx2.createChild("name", index.getFullyQualifiedName());
            rx2.createChild("table", index.getTable().getFullyQualifiedName());
            for (Column col : index) {
                rx2.createChild("column", col
                        + (index.isAscending(col) ? "(A)" : "(D)"));
            }
        }
    }

    public SeqInumIndex(Rx rx) {
        id = rx.getIntAttribute("id");
        name = rx.getAttribute("name");
        createCost = rx.getChildDoubleContent("createCost");
        dropCost = rx.getChildDoubleContent("dropCost");
        storageCost = rx.getChildDoubleContent("storageCost");
        indexBenefit = rx.getChildDoubleContent("indexBenefit");
        indexBenefit2 = rx.getChildDoubleContent("indexBenefit2");
    }

    @Override
    public String toString() {
        return "[" + name + ",create=" + createCost + "]";
    }
}
