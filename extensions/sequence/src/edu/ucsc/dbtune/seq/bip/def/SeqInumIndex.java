package edu.ucsc.dbtune.seq.bip.def;

import java.io.Serializable;
import java.util.HashSet;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.seq.utils.Rx;

public class SeqInumIndex implements Serializable {
    public int id;
    public String name;
    public Index index;
    public double createCost, dropCost;
    public double storageCost;
    
    public double indexBenefit;

    public SeqInumIndex(int id) {
        this.id = id;
    }
    
    public void save(Rx rx) {
        rx.createChild("id",id);
        rx.createChild("name",name);
        rx.createChild("createCost",createCost);
        rx.createChild("dropCost",dropCost);
        rx.createChild("storageCost",storageCost);
    }
    
    public SeqInumIndex(Rx rx) {
        id=rx.getChildIntContent("id");
        name=rx.getChildText("name");
        createCost=rx.getChildDoubleContent("createCost");
        dropCost=rx.getChildDoubleContent("dropCost");
        storageCost=rx.getChildDoubleContent("storageCost");
    }

    @Override
    public String toString() {
        return "[" + name + ",create=" + createCost + "]";
    }
}
