package interaction.ibg.parallel;

import interaction.db.DB2IndexSet;
import interaction.ibg.log.InteractionLogger;

public class RRAnalysisThread implements Runnable {
	private IBGAnalyzer[] analyzers;
	private int firstPending;
	private int[] nextPending;
	private int pendingCount;
	private InteractionLogger logger;
	
	public RRAnalysisThread(IBGAnalyzer[] analyzers0, DB2IndexSet candidateSet0, InteractionLogger logger0) {
		analyzers = analyzers0.clone();
		firstPending = 0;
		nextPending = new int[analyzers0.length];
		for (int i = 0; i < nextPending.length; i++)
			nextPending[i] = i+1;
		pendingCount = analyzers0.length;
		logger = logger0;
	}
	
	@Override
	public void run() {
		while (pendingCount > 0) {
			int blockedCount = pass(false);
			if (pendingCount == 0) 
				return;
			if (blockedCount == pendingCount)
				pass(true);
		}
	}
	// make one pass over the analyzers, optionally waiting for unexpanded nodes
	// if we can't analyze a graph without waiting, and "wait" is false, we consider
	// this graph blocked. The function returns the number of blocked graphs.
	private int pass(boolean wait) {
		int blockedCount = 0;
		int previous = -1;
		for (int i = firstPending; i < analyzers.length && pendingCount > 0; i = nextPending[i]) {
			switch (analyzers[i].analysisStep(logger, wait)) {
				case SUCCESS:
					previous = i;
					break;
				case DONE:
					if (previous == -1) 
						firstPending = nextPending[i];
					else
						nextPending[previous] = nextPending[i];
					--pendingCount;
//					Debug.println("finished analysis of " + analyzers[i].ibg.xacts.get(0).id);
					break;
				case BLOCKED:
					assert(!wait);
					++blockedCount;
					previous = i;
			}
		}
		return blockedCount;
	}
//			if (pendingCount == 0)
//				return;
//			if (blockedCount == pendingCount)
//				continue;
//			
//			for (int i = 0; i < pending.length && pendingCount > 0; i++) {
//				if (pending[i] == null)
//					continue;
//				switch (pending[i].analysisStep(bank, true)) {
//					case SUCCESS:
//						break;
//					case DONE:
//						pending[i] = null;
//						--pendingCount;
//						break;
//					case BLOCKED:
//						assert(false); // we told the analysis to wait for an unexpanded node
//				}		
//			}
//		}
//	}

}
