package interaction;

import static interaction.cand.Generation.Strategy.*;
import interaction.cand.Generation;
import interaction.db.DB2IndexSet;
import interaction.ibg.log.BasicLog;
import interaction.ibg.log.InteractionBank;
import interaction.ibg.serial.SerialIBGCoveringNodeFinder;
import interaction.ibg.serial.SerialIBGMonotonicEnforcer;
import interaction.ibg.serial.SerialIndexBenefitGraph;
import interaction.schedule.DB2GreedyScheduler;
import interaction.schedule.GreedyScheduler;
import interaction.schedule.IBGScheduleInfo;
import interaction.schedule.IndexSchedule;
import interaction.schedule.PartitionedScheduler;
import interaction.schedule.ReverseGreedyScheduler;
import interaction.util.BitSet;
import interaction.util.Files;
//import static interaction.ibg.AnalysisMode.PARALLEL;
import static interaction.ibg.AnalysisMode.SERIAL;

import java.io.File;
import java.io.IOException;

public class ScheduleMain {
	public static void main(String[] args) {
		// process arguments
		if (args.length > 0) {
			System.out.println("No arguments are accepted");
		}
		
		// do the steps
		try {
			runSteps();
		} catch (Throwable t) {	
			t.printStackTrace();
		} finally { 	
			// ensures that all threads exit
			System.exit(0);
		}
	}

	private static void runSteps() throws IOException, ClassNotFoundException {
		Generation.Strategy[] strategies = new Generation.Strategy[] {
//				UNION_OPTIMAL,
				OPTIMAL_1C,
//				FULL_BUDGET,
//				HALF_BUDGET
		};
		double[] thresholds = new double[] {
				0.01,
				0.1,
				1.0
		};
		
		
		
		for (Generation.Strategy s : strategies) {
			File ibgFile = Configuration.ibgFile(s);
			SerialIndexBenefitGraph[] ibgs = (SerialIndexBenefitGraph[]) Files.readObjectFromFile(ibgFile);
			SerialIBGMonotonicEnforcer monotonize = new SerialIBGMonotonicEnforcer();
			for (SerialIndexBenefitGraph ibg : ibgs) {
				monotonize.fix(ibg);
			}
			
//			{
//				SerialIBGCoveringNodeFinder searcher = new SerialIBGCoveringNodeFinder();
//				BitSet tempBitSet = new BitSet();
//				double emptyCost = searcher.findCost(ibgs, tempBitSet);
//								
//				infos = getInfo(indexes, ibgs);
//				prevCost = searcher.findCost(ibgs, tempBitSet);
//				for (IndexInfo info : infos) {
//					double currentCost, conditionalBenefit;
//					tempBitSet.set(info.id);
//					
//					currentCost = searcher.findCost(ibgs, tempBitSet);
//					conditionalBenefit = prevCost - currentCost;
//					schedule.append(info.id, conditionalBenefit);
//					prevCost = currentCost;
//				}
//			}
			
			
			
			File analysisFile = Configuration.analysisFile(s, SERIAL);
			BasicLog basicLog = (BasicLog) Files.readObjectFromFile(analysisFile);
			File candidateFile = Configuration.candidateFile(s);
			DB2IndexSet candidateSet = (DB2IndexSet) Files.readObjectFromFile(candidateFile);
			
//			randomSchedules(candidateSet, ibgs, basicLog.getInteractionBank(), 0.02);
			/*
			IBGScheduleInfo<SerialIndexBenefitGraph> ibgInfo = new SerialIndexBenefitGraph.ScheduleInfo(ibgs);
			for (double t : thresholds) {
				allSchedules(candidateSet, ibgInfo, basicLog.getInteractionBank(), t);
			}
			*/
		}
	}
	
	public static void allSchedules(DB2IndexSet candidateSet, IBGScheduleInfo<? extends IBGScheduleInfo.Graph> ibgs, InteractionBank bank, double threshold) {
		IndexSchedule schedule;
		long start, end;
		BitSet rootBitSet = candidateSet.bitSet();
		
		System.out.println();
		System.out.println("Configuration: ");
		System.out.println(candidateSet);
		
		start = System.currentTimeMillis();
		schedule = DB2GreedyScheduler.schedule(rootBitSet, ibgs);
		end = System.currentTimeMillis();
		System.out.println();
		System.out.print("DB2GreedyScheduler: ");
		schedule.print();
		System.out.println("  (" + ((end-start)/1000.0) + " seconds)");
		
		start = System.currentTimeMillis();
		schedule = GreedyScheduler.schedule(rootBitSet, ibgs);
		end = System.currentTimeMillis();
		System.out.println();
		System.out.print("GreedyScheduler: ");
		schedule.print();
		System.out.println("  (" + ((end-start)/1000.0) + " seconds)");
		
		start = System.currentTimeMillis();
		schedule = ReverseGreedyScheduler.schedule(rootBitSet, ibgs);
		end = System.currentTimeMillis();
		System.out.println();
		System.out.print("ReverseGreedyScheduler: ");
		schedule.print();
		System.out.println("  (" + ((end-start)/1000.0) + " seconds)");
		
		start = System.currentTimeMillis();
		schedule = PartitionedScheduler.schedule(rootBitSet, ibgs, bank.stablePartitioning(threshold));
		end = System.currentTimeMillis();
		System.out.println();
		System.out.print("PartitionedScheduler: ");
		schedule.print();
		System.out.println("  (" + ((end-start)/1000.0) + " seconds)");
		
	//	start = System.currentTimeMillis();
	//	schedule = OptimalScheduler.schedule(rootBitSet, ibgs);	
	//	end = System.currentTimeMillis();
	//	System.out.println();
	//	System.out.print("OptimalScheduler: ");
	//	schedule.print();
	//	System.out.println("  (" + ((end-start)/1000.0) + " seconds)");
	}
	
	public static void randomSchedules(DB2IndexSet candidateSet, SerialIndexBenefitGraph ibgs[], InteractionBank bank, double threshold) {
		java.util.Random rand = new java.util.Random(); 
		IndexSchedule schedule;
		long start, end;
		BitSet rootBitSet = candidateSet.bitSet();
		int queryCount = ibgs.length;
		boolean[] selectedQueries = new boolean[queryCount];
		
		for (int i = 0; i < 500; i++) {
			// choose subset of queries
			int selectedCount = 0;
			for (int j = 0; j < queryCount; j++) {
				boolean selected = rand.nextBoolean();
				selectedQueries[j] = selected;
				if (selected)
					selectedCount++;
			}
			
			// build subset
			SerialIndexBenefitGraph[] subset = new SerialIndexBenefitGraph[selectedCount];
			int p = 0;
			for (int j = 0; j < queryCount; j++)
				if (selectedQueries[j])
					subset[p++] = ibgs[j];
			assert(p == selectedCount);
			/*
			SerialIndexBenefitGraph.ScheduleInfo ibgInfo = new SerialIndexBenefitGraph.ScheduleInfo(subset);
			double greedy = GreedyScheduler.schedule(rootBitSet, ibgInfo).penalty();
			double partitioned = PartitionedScheduler.schedule(rootBitSet, ibgInfo, bank.stablePartitioning(threshold)).penalty();
			
			System.out.print((greedy/partitioned) + "\t");
			for (int j = 0; j < queryCount; j++)
				if (selectedQueries[j])
					System.out.print((j+1) + " ");
			System.out.println();
			*/
		}
		
	}
}
