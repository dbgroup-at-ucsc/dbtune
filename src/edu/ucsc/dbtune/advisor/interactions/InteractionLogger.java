/* ************************************************************************** *
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
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied  *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.advisor.interactions;

import edu.ucsc.dbtune.metadata.Configuration;

public class InteractionLogger
{
	private InteractionBank bank; 
	
	public InteractionLogger(Configuration candidateSet0) {
		bank = new InteractionBank(candidateSet0);
	}
	
	/*
	 * Assign interaction with an exact value
	 */
	public final void assignInteraction(int id1, int id2, double newValue) {		
		bank.assignInteraction(id1, id2, newValue);
	}

	public void assignBenefit(int id, double newValue) {
		bank.assignBenefit(id, newValue);
	}

	public final InteractionBank getInteractionBank() {
		return bank;
	}
}
