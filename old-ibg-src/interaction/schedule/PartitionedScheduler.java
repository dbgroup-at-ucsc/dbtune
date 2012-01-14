package interaction.schedule;

import interaction.util.BitSet;

public class PartitionedScheduler {
	public static <IBG extends IBGScheduleInfo.Graph> IndexSchedule schedule(BitSet indexes, IBGScheduleInfo<IBG> ibgInfo, BitSet[] stablePartitioning) {
		IndexSchedule[] subSchedules;
		int partitionCount, singletonCount, subscheduleCount;
		int s;
		
		partitionCount = stablePartitioning.length;
		singletonCount = 0;
		for (BitSet bs : stablePartitioning)
			if (bs.cardinality() == 1)
				++singletonCount;
		subscheduleCount = partitionCount - singletonCount + 1;
		
		subSchedules = new IndexSchedule[subscheduleCount];
		subSchedules[0] = scheduleSingletons(ibgInfo, stablePartitioning);
		s = 1;
		for (BitSet bs : stablePartitioning)
			if (bs.cardinality() > 1)
				subSchedules[s++] = OptimalScheduler.schedule(bs, ibgInfo);	

		System.out.println("Optimal subschedules: ");
		for (int i = 0; i < subscheduleCount; i++) {
			System.out.print("\t");
			subSchedules[i].print();
		}
		
		return MergeScheduler.schedule(ibgInfo, subSchedules);
	}
	
	private static <IBG extends IBGScheduleInfo.Graph> IndexSchedule scheduleSingletons(IBGScheduleInfo<IBG> ibgInfo, BitSet[] stablePartitioning) {
		BitSet singletons = new BitSet();
		for (BitSet bs : stablePartitioning)
			if (bs.cardinality() == 1)
				singletons.set(bs.nextSetBit(0));
		return GreedyScheduler.schedule(singletons, ibgInfo);
	}
}
