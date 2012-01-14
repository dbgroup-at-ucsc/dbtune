package interaction.schedule;

import interaction.util.BitSet;

public abstract class GreedyScheduler {
	public static <IBG extends IBGScheduleInfo.Graph>IndexSchedule schedule(BitSet indexes, IBGScheduleInfo<IBG> ibgInfo) {
		IBG[] ibgs = ibgInfo.ibgs();
		IBGScheduleInfo.Searcher<IBG> finder = ibgInfo.searcher();
		BitSet remaining = indexes.clone();
		BitSet sofar = new BitSet();
		double prevCost = finder.findCost(ibgs, sofar);
		IndexSchedule schedule = new IndexSchedule();
		while (!remaining.isEmpty()) {
			int bestID = -1;
			double bestCost = Double.POSITIVE_INFINITY;
			
			System.out.print(" *** Greedy benefits: ");
			
			for (int i = remaining.nextSetBit(0); i >= 0; i = remaining.nextSetBit(i+1)) {
				sofar.set(i);
				double cost = finder.findCost(ibgs, sofar);
				sofar.clear(i);
				if (cost < bestCost) {
					bestID = i;
					bestCost = cost;
				}
				
				System.out.printf("%d(%f) ", i, prevCost - cost);
			}
			
			System.out.println();
			
			assert(bestID >= 0);
			schedule.append(bestID, prevCost - bestCost);
			remaining.clear(bestID);
			sofar.set(bestID);
			prevCost = bestCost;
		}
		
		return schedule;
	}
}
