package edu.ucsc.dbtune.seq;

import java.sql.SQLException;
import java.util.Vector;

import edu.ucsc.dbtune.seq.def.*;

public class SeqCostBasedPruning {
	public SeqStep[] steps;
	public SeqStepConf[] bestPath;

	public SeqOptimal[] pruningResults;
	public SeqIndex[] pruningIndices;

	public SeqCostBasedPruning(SeqCost cost, SeqQuery[] queries, Vector<SeqIndex> indices) throws SQLException {
		this(cost, queries, indices.toArray(new SeqIndex[indices.size()]));
	}

	public SeqCostBasedPruning(SeqCost cost, SeqQuery[] queries, SeqIndex[] indices) throws SQLException {
		int n = queries.length;
		SeqIndex[] index = new SeqIndex[1];
		Vector<SeqIndex>[] usedIndices = new Vector[n];
		for (int i = 0; i < n; i++) {
			usedIndices[i] = new Vector<SeqIndex>();
		}
		pruningResults = new SeqOptimal[indices.length];
		pruningIndices = new SeqIndex[indices.length];
		for (int i = 0; i < indices.length; i++) {
			index[0] = indices[i];
			SeqConfiguration[] confs = cost.getAllConfigurations(index);
			SeqStep[] steps2 = SeqOptimal.getOptimalSteps(cost.source,
					cost.destination, queries, confs);
			SeqOptimal optimal = new SeqOptimal(cost, cost.source, cost.destination,
					queries, steps2);
			SeqStepConf[] best = optimal.getBestSteps();
			for (int j = 1; j < best.length - 1; j++) {
				if (best[j].configuration.indices.length > 0) {
					if (best[j].configuration.indices[0] != index[0])
						throw new Error();
					usedIndices[j - 1].add(index[0]);
				}
			}
			pruningIndices[i] = indices[i];
			pruningResults[i] = optimal;
		}
		steps = new SeqStep[n + 2];
		steps[0] = new SeqStep(null, cost.source);
		steps[steps.length - 1] = new SeqStep(null, cost.destination);
		for (int i = 0; i < n; i++) {
			SeqConfiguration[] confs = cost.getAllConfigurations(usedIndices[i]);
			steps[1 + i] = new SeqStep(queries[i], confs);
		}
		SeqOptimal optimal = new SeqOptimal(cost, cost.source, cost.destination,
				queries, steps);
		bestPath = optimal.getBestSteps();
	}
}
