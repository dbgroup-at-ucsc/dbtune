package interaction.util.befs.bb;

import interaction.util.befs.BestFirstSearch;

/*
 * Entry point for branch and bound. Coordinates the workers
 * with their shared state and the BeFS engine.
 */
public class BranchAndBound {
	/*
	 * Initialize the workers and their shared state.
	 * Then create a BestFirstSearch engine and run it.
	 */
	public static Object solve(Problem prob, int numThreads) {
		State state;
		BBWorker[] workers;
		BestFirstSearch<Problem> befsEngine;
		
		state = new State();
		state.bestCost = prob.upperBound();
		
		workers = new BBWorker[numThreads];
		for (int i = 0; i < numThreads; i++)
			workers[i] = new BBWorker(state);
		
		befsEngine = new BestFirstSearch<Problem>(workers);
		befsEngine.run(new Problem[] {prob});
		
		return state.bestSolution;
	}
}
