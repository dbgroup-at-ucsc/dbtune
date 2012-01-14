package interaction.util.befs.bb;

class State {
	// current best solution 
	volatile Object bestSolution = null; 
	volatile float bestCost = Float.POSITIVE_INFINITY;
	final Object solutionLock = new Object();
}
