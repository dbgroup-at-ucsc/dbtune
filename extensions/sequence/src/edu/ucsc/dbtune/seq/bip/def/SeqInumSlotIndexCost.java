package edu.ucsc.dbtune.seq.bip.def;

import java.io.Serializable;

import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.utils.Rx;

public class SeqInumSlotIndexCost implements Serializable {
    public SeqInumIndex index;
    public double cost;

    public SeqInumSlotIndexCost() {
    }

    public void save(Rx rx) {
        rx.createChild("indexId", index.id);
        rx.createChild("indexName", index.name);
        rx.createChild("cost", cost);
    }

    public SeqInumSlotIndexCost(SeqInumCost costModel, Rx rx) {
        String indexName = rx.getChildText("indexName");
        index = costModel.indexHash.get(indexName);
        if (index == null)
            throw new Error();
        cost = rx.getChildDoubleContent("cost");
    }
}
