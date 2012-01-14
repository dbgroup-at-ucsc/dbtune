package interaction.util.befs.bb;

import interaction.util.befs.Emit;

public interface Problem {
	/* emit all the subproblems via callback */
	public abstract void branch(Emit<Problem> emitter);
	
	/* give a lower bound on the cost of a solution to this problem */
	public abstract float lowerBound();
	
	/* 
	 * Give an upper bound on the cost of a solution to the problem.
	 * This bound must be sound in the sense that a solution must
	 * exist with a cost below the resturned value. An implementation
	 * may optionally return infinity, which does not require the 
	 * existence of a solution.
	 */
	public abstract float upperBound();
	
	/*
	 * If isFinal returns true, this function returns a solution to 
	 * this problem with a cost equal to lowerBound. 
	 */
	public abstract Object getSolution();

	/*
	 * Return true if this problem has exactly one solution. In this
	 * case getSolution() should also return the solution. If
	 * there is no solution, isFinal returns false and branch makes
	 * no callbacks. 
	 */
	public abstract boolean isFinal();
}
