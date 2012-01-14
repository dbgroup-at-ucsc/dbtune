package interaction.schedule;

import interaction.util.BitSet;

public abstract class ReverseGreedyScheduler {
	public static <IBG extends IBGScheduleInfo.Graph> IndexSchedule schedule(BitSet indexes, IBGScheduleInfo<IBG> ibgInfo) {
		IBG[] ibgs = ibgInfo.ibgs();
		IBGScheduleInfo.Searcher<IBG> finder = ibgInfo.searcher();
		BitSet remaining = indexes.clone();
		BitSet sofar = new BitSet();
		double prevCost = finder.findCost(ibgs, remaining);
		IndexSchedule schedule = new IndexSchedule();
		while (!remaining.isEmpty()) {
			int bestID = -1;
			double bestCost = Double.POSITIVE_INFINITY;
			
			for (int i = remaining.nextSetBit(0); i >= 0; i = remaining.nextSetBit(i+1)) {
				remaining.clear(i);
				double cost = finder.findCost(ibgs, remaining);
				remaining.set(i);
				if (cost < bestCost) {
					bestID = i;
					bestCost = cost;
				}
			}
			assert(bestID >= 0);
			schedule.prepend(bestID, bestCost - prevCost);
			remaining.clear(bestID);
			sofar.set(bestID);
			prevCost = bestCost;
		}
		
		return schedule;
	}
}
