package edu.ucsc.dbtune.seq.bip.def;

import java.io.Serializable;
import java.util.Hashtable;

import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.util.Rx;

public class SeqInumPlan implements Serializable {
    public SeqInumQuery query;
    public String plan;
    public int id;
    public double internalCost;
    public SeqInumSlot[] slots;

    public SeqInumPlan(SeqInumQuery query,int id) {
        this.query = query;
        this.id=id;
    }
    
    public void save(Rx rx) {
        rx.setAttribute("id",id);
        rx.setAttribute("internalCost",internalCost);
        rx.createChild("plan",plan);
        for (SeqInumSlot slot : slots) {
            slot.save(rx.createChild("slot"));
        }
    }
    
    public SeqInumPlan(Hashtable<String, SeqInumIndex> indexHash,SeqInumQuery query,Rx rx) {
        this.query = query;
        id=rx.getIntAttribute("id");
        internalCost=rx.getDoubleAttribute("internalCost");
        plan=rx.getChildText("plan");
        Rx[] rs=rx.findChilds("slot");
        slots=new SeqInumSlot[rs.length];
        for (int i=0;i<rs.length;i++) {
            slots[i]=new SeqInumSlot(indexHash,this,rs[i],i);
        }
    }
}
