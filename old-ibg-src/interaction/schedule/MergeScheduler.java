package interaction.schedule;

import interaction.util.BitSet;

import java.util.Comparator;
import java.util.ListIterator;

public abstract class MergeScheduler {
	/*
	 * Modifies schedules in place and returns a merged schedule 
	 * (which might be one of the modified input schedules).
	 */
	public static <IBG extends IBGScheduleInfo.Graph> IndexSchedule schedule(IBGScheduleInfo<IBG> ibgInfo, IndexSchedule[] subs) {
		return new Workspace<IBG>(ibgInfo, subs).schedule();
	}
	
	private static class Workspace<IBG extends IBGScheduleInfo.Graph> {
		IBG[] ibgs;
		IBGScheduleInfo<IBG> ibgInfo;
		IndexSchedule[] subs;
		Memo memo;
		IBGScheduleInfo.Searcher<IBG> finder;
		int indexCount;
		BitSet tempBitSet;
		double emptyCost;
		
		Workspace(IBGScheduleInfo<IBG> ibgInfo0, IndexSchedule[] subs0) {
			subs = subs0;
			finder = ibgInfo0.searcher();
			
			// compute number of indices
			indexCount = 0;
			for (IndexSchedule sub : subs0) {
				indexCount += sub.indexCount();
			}
			
			tempBitSet = new BitSet();
			memo = new Memo(indexCount);
			ibgs = ibgInfo0.ibgs();
			ibgInfo = ibgInfo0;  
			emptyCost = finder.findCost(ibgs, new BitSet());
		}
	
		IndexSchedule schedule() {
			int remaining;
			
			remaining = subs.length;
			while (remaining > 1) {
				int i;
				sort(subs, remaining);
				for (i = 0; i < remaining/2; i++) {
					merge(subs[i], subs[remaining-i-1]);
				}	
				remaining = (remaining+1)/2;
			}
			return subs[0];
		}
		
		/*
		 * Does an individual merge operation
		 * 
		 * The first schedule is modified in place to represent the result of the merge
		 * The other argument is a Memo structure that we can reuse
		 */
		private void merge(IndexSchedule sch1, IndexSchedule sch2) {
			int n1 = sch1.indexCount();
			int n2 = sch2.indexCount();
			int n = n1+n2;
			int i1, i2;
			
			// initialize the memo
			memo.set(0, 0, 0.0, -1, 0);
			
			{
				double totalBenefit1 = 0;
				i1 = 1;
				for (IndexScheduleItem item : sch1.items()) {
					// compute the penalty of sch1[1..i]
					double penalty = memo.penalty(i1-1,0) + (item.conditionalBenefit * (i1-1));
					totalBenefit1 += item.conditionalBenefit;
					memo.set(i1, 0, penalty, 1, totalBenefit1);
					++i1;
				}
			}
			
			{
				double totalBenefit2 = 0;
				i2 = 1;
				for (IndexScheduleItem item : sch2.items()) {
					// compute the penalty of sch2[1..i]
					double penalty = memo.penalty(0,i2-1) + (item.conditionalBenefit * (i2-1));
					totalBenefit2 += item.conditionalBenefit;
					memo.set(0, i2, penalty, 2, totalBenefit2);
					++i2;
				}
			}
			
			// do the recurrence
			tempBitSet.clear();
			i1 = 1;
			for (IndexScheduleItem item1 : sch1.items()) {
				tempBitSet.set(item1.id);
				i2 = 1;
				for (IndexScheduleItem item2 : sch2.items()) {
					double penalty1, penalty2, conditionalBenefit1, conditionalBenefit2, totalBenefit;
					
					// recompute conditional benefits without assuming independence
					tempBitSet.set(item2.id);
					totalBenefit = emptyCost - finder.findCost(ibgs, tempBitSet);
					conditionalBenefit1 = totalBenefit - memo.totalBenefit(i1-1,i2);
					conditionalBenefit2 = totalBenefit - memo.totalBenefit(i1,i2-1);
					penalty1 = memo.penalty(i1-1,i2) + (conditionalBenefit1 * (i1+i2-1));
					penalty2 = memo.penalty(i1,i2-1) + (conditionalBenefit2 * (i1+i2-1));
					
					if (penalty1 <= penalty2) {
						memo.set(i1, i2, penalty1, 1, totalBenefit);
					}
					else {
						memo.set(i1, i2, penalty2, 2, totalBenefit);
					}
					++i2;
				}
				
				for (IndexScheduleItem item2 : sch2.items()) {
					tempBitSet.clear(item2.id);
				}
				
				++i1;
			}
			
			// process the result, and overwrite sch1
			int[] result = memo.getResult(n1, n2);
			ListIterator<IndexScheduleItem> iter1 = sch1.itemListIterator();
			ListIterator<IndexScheduleItem> iter2 = sch2.itemListIterator();
			i1 = i2 = 0;
			for (int i = 0; i < n; i++) {
				IndexScheduleItem item;
				switch (result[i]) {
					case 1: 
						item = iter1.next();
						item.conditionalBenefit = memo.totalBenefit(i1+1,i2) - memo.totalBenefit(i1,i2);
						++i1;
						// leave item where it is in sch1
						break;
					case 2: 
						item = iter2.next();
						item.conditionalBenefit = memo.totalBenefit(i1,i2+1) - memo.totalBenefit(i1,i2);
						++i2;
						// add the item from sch2 to sch1
						iter1.add(item);
						break;
					default:
						assert(false);
				}
			}
		}
		
		/*
		 * Sort schedules by increasing size
		 */
		private static void sort(IndexSchedule[] scheds, int prefix) {
			java.util.Arrays.sort(scheds, 0, prefix, new Comparator<IndexSchedule>() {
				public int compare(IndexSchedule s1, IndexSchedule s2) {
					return s1.indexCount() - s2.indexCount();
				} } );
		}
		
		private static class Memo {
			/*
			 * representation of optimal merge.
			 * 
			 * penalty[i1][i2] is the minimum penalty from merging the first i1 elements of schedule 1
			 * and the first i2 elements of schedule 2.
			 * 
			 * last[i1][i2] = 1 (resp. 2) if the optimal schedule ends with schedule 1 (resp 2)
			 */
			double[][] penalty; // minimum penalty merging first i1 and i2 form each schedule
			int[][] last; 
			double[][] totalBenefit;
			private int[] result;
			
			/*
			 * n is the size of all subschedules together
			 */
			Memo(int n) {
				penalty = new double[n+1][n+1];
				last = new int[n+1][n+1];
				totalBenefit = new double[n+1][n+1];
				result = new int[n];
			}
	
			final double penalty(int i1, int i2) {
				return penalty[i1][i2];
			}
			
			final double totalBenefit(int i1, int i2) {
				return totalBenefit[i1][i2];
			}
	
			final void set(int i1, int i2, double p, int sch, double ben) {
				penalty[i1][i2] = p;
				last[i1][i2] = sch;
				totalBenefit[i1][i2] = ben;
			}
			
			public int[] getResult(int n1, int n2) {
				int i1 = n1, i2 = n2, i = n1+n2-1;
				while (i >= 0) {
					switch (last[i1][i2]) {
						case 1: 
							result[i] = 1;
							--i1;
							break;
						case 2: 
							result[i] = 2;
							--i2;
							break;
						default:
							assert(false);
					}
					--i;
				}
				assert(i1 == 0);
				assert(i2 == 0);
				return result;
			}
		}
	}
}
