package edu.ucsc.dbtune.seq.def;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

public class SeqConfiguration {
	public SeqIndex[] indices;
	public Hashtable<SeqConfiguration, Double> transitionCostCache = new Hashtable<SeqConfiguration, Double>();

	public SeqConfiguration(SeqIndex[] indices) {
		this.indices = indices;
	}

	public int storageCost() {
		int storageCost = 0;
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
