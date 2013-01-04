package edu.ucsc.dbtune.seq;

import java.util.Vector;

import edu.ucsc.dbtune.seq.def.*;

public class SeqSplit {
	public SeqSplitGroup[] groups;

	public SeqSplit(SeqCost cost, SeqQuerySet[] queries) {
		for (SeqIndex i : cost.indicesV) {
			i.sameGroup.clear();
			i.usedByQuery.clear();
			i.markUsed = false;
			i.groupId = -1;
		}
		for (SeqQuerySet set : queries) {
		    SeqQuery q=set.queries[0];
			q.groupId = -1;
			for (SeqIndex i : q.relevantIndices) {
				i.usedByQuery.add(q);
			}
			if (q.relevantIndices.length > 1) {
				for (int i = 0; i < q.relevantIndices.length; i++) {
					for (int j = i + 1; j < q.relevantIndices.length; j++) {
						q.relevantIndices[i].sameGroup
								.add(q.relevantIndices[j]);
						q.relevantIndices[j].sameGroup
								.add(q.relevantIndices[i]);
					}
				}
			}
		}
		int groupId = 0;
		for (SeqIndex i : cost.indicesV) {
			if (i.markUsed)
				continue;
			groupId++;
			for (SeqQuery q : i.usedByQuery) {
				if (q.groupId != -1)
					throw new Error();
				q.groupId = groupId;
			}
			if (i.groupId != -1)
				throw new Error();
			i.groupId = groupId;
			i.markUsed = true;
			for (SeqIndex index : i.sameGroup) {
				if (index.markUsed)
					continue;
				for (SeqQuery q : index.usedByQuery) {
					if (q.groupId != -1)
						throw new Error();
					q.groupId = groupId;
				}
				if (index.groupId != -1)
					throw new Error();
				index.groupId = groupId;
				index.markUsed = true;
			}
		}
		groups = new SeqSplitGroup[groupId];
		for (int i = 1; i <= groupId; i++) {
			Vector<SeqQuery> qs = new Vector<SeqQuery>();
			for (SeqQuerySet set : queries) {
			    SeqQuery q=set.queries[0];
				if (q.groupId == i)
					qs.add(q);
			}
			Vector<SeqIndex> is = new Vector<SeqIndex>();
			for (SeqIndex index : cost.indicesV)
				if (index.groupId == i)
					is.add(index);
			SeqSplitGroup g = new SeqSplitGroup();
			g.queries = qs.toArray(new SeqQuerySet[qs.size()]);
			g.indices = is.toArray(new SeqIndex[is.size()]);
			groups[i - 1] = g;
		}
		for (SeqIndex i : cost.indicesV) {
			i.sameGroup.clear();
			i.usedByQuery.clear();
			i.markUsed = false;
		}
	}
}
