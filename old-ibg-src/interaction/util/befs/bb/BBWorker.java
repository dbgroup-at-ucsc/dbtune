package interaction.util.befs.bb;

import interaction.util.befs.TerminateSignal;
import interaction.util.befs.Worker;

public class BBWorker extends Worker<Problem> {
	private final State state;
	private final ProblemQueue queue;
	
	public BBWorker(State state) {
		this.state = state;
		this.queue = new ProblemQueue();
	}

	/*
	 * Fetch a subproblem. If it is a leaf, add the solution to the
	 * considered solutions. Otherwise, add the children to the
	 * considered subproblems.
	 */
	@Override
	public void process(Problem prob) throws TerminateSignal {
		if (prob.isFinal()) {
			Object solution = prob.getSolution();
			float bound = prob.lowerBound();
			synchronized (state.solutionLock) {
				if (bound < state.bestCost || state.bestSolution == null) {
					state.bestSolution = solution;
					state.bestCost = bound;
				}
			}
		}
		else 
			prob.branch(this);
	}

	@Override
	public void emit(Problem prob) {
		if (prob.lowerBound() < state.bestCost || state.bestSolution == null)
			super.emit(prob);
	}
	
	@Override
	public ProblemQueue queue() {
		return queue;
	}
}