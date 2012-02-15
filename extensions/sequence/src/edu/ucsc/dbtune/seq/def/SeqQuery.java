package edu.ucsc.dbtune.seq.def;

import java.util.Hashtable;
import java.util.Vector;

public class SeqQuery {
	public int id; //for sorting after split and merge
	public String name;
	public String sql;
	public int costWithoutIndex;
	public SeqIndex[] relevantIndices;
	public Vector<SeqQueryCostWithIndex> costsWithIndices = new Vector<SeqQueryCostWithIndex>();
	public Hashtable<SeqConfiguration, Double> costCache = new Hashtable<SeqConfiguration, Double>();
	
	public int groupId; //for split

	public SeqQuery(int id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return name;
	}
}
