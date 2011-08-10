/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */

package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.metadata.Index;

public class BcIndexInfo {
	/*
	 * State of the index, materialized or not
	 */
	public enum State { MATERIALIZED, HYPOTHETICAL }
	State state = State.HYPOTHETICAL;
	
    /* 
     * The basic values Delta, Delta_min and Delta_max. We store Delta for
     * convenience although it is redundant with orig_cost and new_cost below.
     */
	private double delta = 0;
	private double deltaMin = 0;
	private double deltaMax = 0;
	
    /*
     * delta = SUM(origCost) - SUM(newCost) + updateDelta
     * O^i is stored in orig_cost[i] for i=0,1,2, similarly for new_cost.
     * The value of updateDelta is O^U - N^U in the notation of 
     * the ICDE '07 paper. We expect updateDelta to be non-positive
     * since N^U includes maintenance overhead and O^U does not
     */
	private double[] origCost = new double[3];
	private double[] newCost = new double[3];
	private double updateDelta = 0;
	
	/*
	 * add the given values to O^i and N^i where i = level.
	 * 
	 * The caller must eventually update deltaMin and deltaMax!
	 * 
	 */
	void addCosts(int level, double costO, double costN) {
		origCost[level] += costO;
		newCost[level] += costN;
		delta = delta + (costO - costN);
	}


	void setCost(int level, double costO, double costN) {
		double addCostO = costO - origCost[level];
		double addCostN = costN - newCost[level];
		addCosts(level, addCostO, addCostN);
	}

	/*
	 * add the given values to O^U and N^U
	 * 
	 * The caller must eventually update deltaMin and deltaMax!
	 * 
	 */
	void addUpdateCosts(double value) {
		updateDelta -= value;
		delta = delta - value;
	}
	
	/*
	 * 
	 */
	void updateDeltaMinMax() {
		deltaMin = Math.min(deltaMin, delta);
		deltaMax = Math.max(deltaMax, delta);
	}
	
	void initDeltaMin() {
		deltaMin = delta;
	}
	
	void initDeltaMax() {
		deltaMax = delta;
	}

	public double origCost(int level) { return origCost[level]; }
	public double newCost(int level) { return newCost[level]; }


	public double residual(double creationCost) {
		return creationCost - (deltaMax - delta);
	}
	
	public double benefit(double creationCost) {
		return (delta - deltaMin) - creationCost;
	}
	
	public String toString(Index idx) {
		return "   DELTA = " + delta + "\n" +
		       "         = " + origCost[0] + " - " + newCost[0] + "\n" +
		       "         = " + origCost[1] + " - " + newCost[1] + "\n" +
		       "         = " + origCost[2] + " - " + newCost[2] + "\n" +
		       "         = " + updateDelta + "\n" +
		       "DELTAMIN = " + deltaMin + "\n" +
		       "DELTAMAX = " + deltaMax + "\n" +
		       "CREATION = " + idx.getCreationCost() + "\n" +
		       "    SIZE = " + idx.getMegaBytes() + "\n" +
		       (state == State.MATERIALIZED ?
		        "   RESID = " + residual(idx.getCreationCost()) :
		        "     BEN = " + benefit(idx.getCreationCost()));
		
	}
}
