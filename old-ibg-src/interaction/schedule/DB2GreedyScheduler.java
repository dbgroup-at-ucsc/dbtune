package interaction.schedule;

import interaction.util.BitSet;

public abstract class DB2GreedyScheduler {
	public static <IBG extends IBGScheduleInfo.Graph> IndexSchedule schedule(BitSet indexes, IBGScheduleInfo<IBG> ibgInfo) {
		IBG[] ibgs = ibgInfo.ibgs();
		IBGScheduleInfo.Searcher<IBG> searcher = ibgInfo.searcher();
		BitSet tempBitSet = new BitSet();
		IndexSchedule schedule = new IndexSchedule();
		double prevCost;
		IndexInfo[] infos;
		
		infos = getInfo(indexes, ibgs);
		prevCost = searcher.findCost(ibgs, tempBitSet);
		for (IndexInfo info : infos) {
			double currentCost, conditionalBenefit;
			tempBitSet.set(info.id);
			
			currentCost = searcher.findCost(ibgs, tempBitSet);
			conditionalBenefit = prevCost - currentCost;
			schedule.append(info.id, conditionalBenefit);
			prevCost = currentCost;
		}
		
		return schedule;
	}
	
	private static IndexInfo[] getInfo(BitSet indexes, IBGScheduleInfo.Graph[] ibgs) {
		int indexCount = indexes.cardinality();
		IndexInfo[] array;
		int a;
		
		array = new IndexInfo[indexCount];
		a = 0;
		for (int id = indexes.nextSetBit(0); id >= 0; id = indexes.nextSetBit(id+1)) {
			double benefit = 0;
			for (IBGScheduleInfo.Graph ibg : ibgs) {
				IBGScheduleInfo.Node rootNode = ibg.rootIBGInfoNode();
				if (rootNode.usedSetContains(id))
					benefit += ibg.emptyCost() - rootNode.cost();
			}
			
			array[a] = new IndexInfo(id, benefit);
			++a;
		}
		
		assert(a == indexCount);
		java.util.Arrays.sort(array, new IICompare());
		return array;
	}
	
	private static class IICompare implements java.util.Comparator<IndexInfo> {
		public int compare(IndexInfo o1, IndexInfo o2) {
			return (int) Math.signum(o2.benefit - o1.benefit);
		}
	}
	
	private static class IndexInfo {
		final int id;
		final double benefit;
		
		IndexInfo(int id0, double ben0) {
			id = id0; benefit = ben0;
		}
	}
}
