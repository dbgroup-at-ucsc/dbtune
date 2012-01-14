package interaction.schedule;

import interaction.util.BitSet;
import interaction.util.befs.Emit;
import interaction.util.befs.bb.BranchAndBound;

public abstract class OptimalScheduler {
	public static <IBG extends IBGScheduleInfo.Graph> IndexSchedule schedule(BitSet indexes, IBGScheduleInfo<IBG> ibgInfo) {
		return new Workspace<IBG>(indexes, ibgInfo).schedule();
	}
	
	private static class Workspace<IBG extends IBGScheduleInfo.Graph> {
		// we need a separate node finder for each thread
		ThreadLocalSearcher nodeFinder = new ThreadLocalSearcher();
			
		BitSet rootBitSet;
		IBG[] ibgs;
		IBGScheduleInfo<IBG> ibgInfo;
		double rootCost;
		double emptyCost;
		
		public Workspace(BitSet config0, IBGScheduleInfo<IBG> ibgInfo0) {
			rootBitSet = config0;
			ibgInfo = ibgInfo0;
			ibgs = ibgInfo0.ibgs();
			
			IBGScheduleInfo.Searcher<IBG> finder = nodeFinder.get();
			rootCost = finder.findCost(ibgs, rootBitSet);
			emptyCost = finder.findCost(ibgs, new BitSet());
		}

		public IndexSchedule schedule() {
			// run branch and bound
			double benefit = emptyCost - rootCost;
			SubProblem sub = (SubProblem) BranchAndBound.solve(new SubProblem(rootBitSet, benefit, 0, null), 2);
			
			// follow leaf to root path to construct solution
			IndexSchedule sched = new IndexSchedule();
			if (sub.remaining.isEmpty())
				return sched;
			
			double prevBenefit = 0;
			BitSet prevBitSet = new BitSet();
			BitSet tempBitSet = new BitSet();
			while (sub != null) {
				// figure out id
				tempBitSet.set(sub.remaining);
				tempBitSet.andNot(prevBitSet);
				int id = tempBitSet.nextSetBit(0);
				assert(tempBitSet.nextSetBit(id+1) < 0);
				
				// figure out conditional benefit
				double currentBenefit = sub.configBenefit;
				double conditionalBenefit = currentBenefit - prevBenefit;
				
				// update schedule
				sched.append(id, conditionalBenefit);
				
				// continue up the tree
				prevBitSet = sub.remaining;
				prevBenefit = currentBenefit;
				sub = sub.parent;
			}
			
			return sched;
		}
		
		/*
		 * An instance of this class represents a scenario where the suffix
		 * of the schedule has been determined and there is a set of 
		 * remaining indices that we must schedule for the prefix
		 */
		private class SubProblem implements interaction.util.befs.bb.Problem {
			final BitSet remaining;
			final double configBenefit; // absolute benefit of prefix (remaining indices)
			final double totalPenalty; // penalty of suffix (everything except remaining)
			final SubProblem parent;
			
			SubProblem(BitSet config0, double configBenefit0, double totalPenalty0, SubProblem parent0) {
				assert(configBenefit0 >= 0);
				remaining = config0;
				configBenefit = configBenefit0;
				totalPenalty = totalPenalty0;
				parent = parent0;
			}
			
			public float lowerBound() {
				return (float) totalPenalty;
			}

			public float upperBound() {
				return (float) (totalPenalty + configBenefit * (remaining.cardinality() - 1));
			}
			
			public void branch(Emit<interaction.util.befs.bb.Problem> emitter) { 
				for (int i = remaining.nextSetBit(0); i >= 0; i = remaining.nextSetBit(i+1)) {
					BitSet subConfig;
					double subPenalty, subBenefit, subCost, conditionalBenefit;
					
					subConfig = remaining.clone();
					subConfig.clear(i);
					subCost = nodeFinder.get().findCost(ibgs, subConfig);
					subBenefit = emptyCost - subCost;
					conditionalBenefit = configBenefit - subBenefit;
					subPenalty = totalPenalty + (remaining.cardinality()-1) * conditionalBenefit;
					emitter.emit(new SubProblem(subConfig, subBenefit, subPenalty, this));
				}
			}

			public Object getSolution() {
				return this;
			}

			public boolean isFinal() {
				return remaining.cardinality() <= 1;
			}
		}
		
		public class Solution {
			final int id;
			final Solution next;
			
			Solution(int id, Solution next) {
				this.id = id;
				this.next = next;
			}
		}
		
		class ThreadLocalSearcher extends ThreadLocal<IBGScheduleInfo.Searcher<IBG>> {
			public IBGScheduleInfo.Searcher<IBG> initialValue() {
				return ibgInfo.searcher();
			}
		}
	}
}
