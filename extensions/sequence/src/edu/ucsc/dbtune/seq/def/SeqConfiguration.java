package edu.ucsc.dbtune.seq.def;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.deployAware.CPlexWrapper;
import edu.ucsc.dbtune.deployAware.DATQuery;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;

public class SeqConfiguration {
	public SeqIndex[] indices;
	public Hashtable<SeqConfiguration, Double> transitionCostCache = new Hashtable<SeqConfiguration, Double>();

	public SeqConfiguration(SeqIndex[] indices) {
		this.indices = indices;
	}

	public double storageCost() {
		double storageCost = 0;
		for (SeqIndex index : indices)
			storageCost += index.storageCost;
		return storageCost;
	}

	public SeqConfiguration combine(SeqConfiguration b) {
		HashSet<SeqIndex> hash = new HashSet<SeqIndex>();
		Vector<SeqIndex> vs = new Vector<SeqIndex>();
		for (SeqIndex index : indices) {
			if (hash.contains(index))
				continue;
			vs.add(index);
		}
		for (SeqIndex index : b.indices) {
			if (hash.contains(index))
				continue;
			vs.add(index);
		}
		return new SeqConfiguration(vs.toArray(new SeqIndex[vs.size()]));
	}
	
    public HashSet<Index> getIndexes(DatabaseSystem db,
            DB2Optimizer optimizer) throws Exception {
        double cost = 0;
        HashSet<Index> allIndexes = new HashSet<Index>();
        for (SeqIndex index : indices)
                allIndexes.add(index.inumIndex.loadIndex(db));
        return allIndexes;
    }

    public HashSet<Index> getIndexes(DatabaseSystem db) throws Exception {
        double cost = 0;
        HashSet<Index> allIndexes = new HashSet<Index>();
        for (SeqIndex index : indices)
                allIndexes.add(index.inumIndex.loadIndex(db));
        return allIndexes;
    }
    
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (SeqIndex i : indices) {
			if (sb.length() > 0)
				sb.append(",");
			sb.append(i.name);
		}
		return sb.toString();
	}

}
