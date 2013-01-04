package edu.ucsc.dbtune.seq;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Vector;

import edu.ucsc.dbtune.seq.def.SeqConfiguration;
import edu.ucsc.dbtune.seq.def.SeqIndex;
import edu.ucsc.dbtune.seq.def.SeqQuery;
import edu.ucsc.dbtune.seq.def.SeqQuerySet;
import edu.ucsc.dbtune.seq.def.SeqStep;
import edu.ucsc.dbtune.seq.def.SeqStepConf;

public class SeqGreedySeq {
	SeqCost cost;
	SeqQuerySet[] queries;
	public Vector<SeqStepConf[]> P = new Vector<SeqStepConf[]>();
	public HashSet<SeqConfiguration>[] C;
	public SeqStepConf[] r, s, t;

    @SuppressWarnings({"unchecked"})
	public SeqGreedySeq(SeqCost cost, SeqQuerySet[] queries, SeqIndex[] indices) throws SQLException {
		this.cost = cost;
		this.queries = queries;
		SeqIndex[] index = new SeqIndex[1];
        C = (HashSet<SeqConfiguration>[]) new HashSet[queries.length];
		for (int i = 0; i < C.length; i++)
			C[i] = new HashSet<SeqConfiguration>();
		for (int i = 0; i < indices.length; i++) {
			index[0] = indices[i];
			SeqConfiguration[] confs = cost.getAllConfigurations(index);
			SeqStep[] steps2 = SeqOptimal.getOptimalSteps(cost.source,
					cost.destination, queries, confs);
			SeqOptimal optimal = new SeqOptimal(cost, cost.source, cost.destination,
					queries, steps2);
			SeqStepConf[] best = optimal.getBestSteps();
			P.add(best);
		}
		for (SeqStepConf[] confs : P) {
			for (int i = 0; i < C.length; i++) {
				C[i].add(confs[i + 1].configuration);
			}
		}
	}

	public boolean run() throws SQLException {
		double minCost = Double.MAX_VALUE;
		r = null;
		for (SeqStepConf[] confs : P) {
			double cost = confs[confs.length - 1].costUtilThisStep;
			if (cost < minCost) {
				minCost = cost;
				r = confs;
			}
		}
		P.remove(r);
		for (int i = 0; i < C.length; i++) {
			C[i].add(r[i + 1].configuration);
		}
		minCost = Double.MAX_VALUE;
		s = null;
		t = null;
		for (SeqStepConf[] confs : P) {
			SeqUnionPair pair = new SeqUnionPair(cost, queries, r, confs);
			SeqStepConf[] t1 = pair.bestPath;
			double cost = t1[t1.length - 1].costUtilThisStep;
			if (cost < minCost) {
				minCost = cost;
				s = confs;
				t = t1;
			}
		}
		if (t == null)
			return false;
		if (t[t.length - 1].costUtilThisStep >= r[r.length - 1].costUtilThisStep)
			return false;
		P.remove(s);
		P.add(t);
		return true;
	}

	public SeqStep[] steps;
	public SeqStepConf[] bestPath;

	public void finish() throws SQLException {
		int n = queries.length;
		steps = new SeqStep[n + 2];
		steps[0] = new SeqStep(null, cost.source);
		steps[steps.length - 1] = new SeqStep(null, cost.destination);
		for (int i = 0; i < n; i++) {
			steps[1 + i] = new SeqStep(queries[i], C[i]
					.toArray(new SeqConfiguration[C[i].size()]));
		}
		SeqOptimal optimal = new SeqOptimal(cost, cost.source, cost.destination,
				queries, steps);
		bestPath = optimal.getBestSteps();
	}
}
