package edu.ucsc.dbtune.seq;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

import edu.ucsc.dbtune.seq.def.*;
import edu.ucsc.dbtune.seq.utils.Rt;

public class SeqMerge {
	public SeqQuery[] queries;
	public SeqIndex[][] indices;
	public int[] storageCosts;

	public SeqMerge(SeqCost cost, SeqSplitGroup[] groups) throws SQLException {
		class Item {
			SeqQuery query;
			SeqIndex[] indices;
		}
		Vector<Item> vector = new Vector<Item>();
		Vector<SeqIndex> allIndices = new Vector<SeqIndex>();
		int groupId = 0;
		for (SeqSplitGroup g : groups) {
			groupId++;
			SeqQuery lastQuery = null;
			for (int i = 1; i < g.bestPath.length - 1; i++) {
				SeqStepConf step = g.bestPath[i];
				Item item = new Item();
				item.query = step.step.query;
				item.indices = step.configuration.indices;
				for (SeqIndex index : item.indices) {
					index.groupId = groupId;
					allIndices.add(index);
				}
				if (lastQuery != null && lastQuery.id >= item.query.id)
					throw new Error();
				lastQuery = item.query;
				vector.add(item);
			}
		}
		Collections.sort(vector, new Comparator<Item>() {
			public int compare(Item o1, Item o2) {
				return o1.query.id - o2.query.id;
			}
		});
		queries = new SeqQuery[vector.size()];
		for (int i = 0; i < vector.size(); i++)
			queries[i] = vector.get(i).query;
		Hashtable<SeqQuery, Integer> hash = new Hashtable<SeqQuery, Integer>();
		for (int i = 0; i < queries.length; i++) {
			hash.put(queries[i], i);
		}
		HashSet<SeqIndex>[] indices = new HashSet[vector.size()];
		for (int i = 0; i < indices.length; i++) {
			indices[i] = new HashSet<SeqIndex>();
		}
		for (SeqSplitGroup g : groups) {
			for (int i = 1; i < g.bestPath.length - 1; i++) {
				SeqStepConf step = g.bestPath[i];
				SeqStepConf next = i < g.bestPath.length - 2 ? g.bestPath[i + 1]
						: null;
				int curQueryPos = hash.get(step.step.query);
				for (SeqIndex index : step.configuration.indices)
					indices[curQueryPos].add(index);
				if (next != null) {
					int nextQueryPos = hash.get(next.step.query);
					if (nextQueryPos > curQueryPos + 1) {
						// need to fill in between queries
						HashSet<SeqIndex> nextContains = new HashSet<SeqIndex>();
						for (SeqIndex i2 : next.configuration.indices)
							nextContains.add(i2);
						for (SeqIndex index : step.configuration.indices) {
							if (nextContains.contains(index)) {
								for (int j = curQueryPos + 1; j < nextQueryPos; j++)
									indices[j].add(index);
							}
						}
					}
				}
			}
		}
		this.indices = new SeqIndex[indices.length][];
		this.storageCosts = new int[indices.length];
		for (int i = 0; i < indices.length; i++) {
			this.indices[i] = indices[i].toArray(new SeqIndex[indices[i].size()]);
			int storageCost = 0;
			for (SeqIndex index : this.indices[i]) {
				storageCost += index.storageCost;
			}
			this.storageCosts[i] = storageCost;
		}
		for (int start = 0; start < indices.length; start++) {
			if (this.storageCosts[start] > cost.storageConstraint) {
				int end = start + 1;
				for (; end < indices.length; end++) {
					if (this.storageCosts[end] <= cost.storageConstraint)
						break;
				}
				Rt.p(start+" "+end);
				SeqIndex[] source = start > 0 ? this.indices[start - 1]
						: cost.source;
				SeqIndex[] destination = end < indices.length - 1 ? this.indices[end + 1]
						: cost.destination;
				SeqQuery[] subQueries = new SeqQuery[end - start];
				SeqStep[] steps = new SeqStep[subQueries.length + 2];
				steps[0] = new SeqStep(null, source);
				steps[steps.length - 1] = new SeqStep(null, destination);
				for (int i = 0; i < subQueries.length; i++) {
					subQueries[i] = this.queries[start + i];
					SeqConfiguration[] confs = cost
							.getAllConfigurations(this.indices[start + i]);
					steps[1 + i] = new SeqStep(queries[i], confs);
				}
				SeqOptimal optimal = new SeqOptimal(cost, source, destination,
						subQueries, steps);
				SeqStepConf[] confs = optimal.getBestSteps();
				for (int i = 0; i < subQueries.length; i++) {
					this.indices[start + i] = confs[i + 1].configuration.indices;
				}
				start = end - 1;
			}
		}
	}
}
