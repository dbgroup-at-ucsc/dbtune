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

package edu.ucsc.dbtune.advisor.interactions;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.util.ToStringBuilder;

public class InteractionLogger {
    private final InteractionBank bank;

    /**
     * construct an {@link InteractionLogger} for a particular
     * {@link IndexBenefitGraph}.
     *
     * @param maxId
     *      maximum id that an index can have
     */
    public InteractionLogger(int maxId) {
        this(new InteractionBank(maxId));
    }

    /**
     * construct an {@link InteractionLogger} for a particular
     * {@link IndexBenefitGraph}.
     * @param bank
     *      an {@link InteractionBank} part of logger.
     */
    InteractionLogger(InteractionBank bank){
        this.bank = bank;
    }

    
    /**
     * Assign interaction with an exact value
     * @param id1
     *      identifier of first index.
     * @param id2
     *      identifier of a second index.
     * @param newValue
     *      new value of interaction.
     */
    public final void assignInteraction(int id1, int id2, double newValue) {        
        bank.assignInteraction(id1, id2, newValue);
    }

    /**
     * assign benefit to a particular index
     * @param id
     *      index's identifier.
     * @param newValue
     *      benefit value.
     */
    public void assignBenefit(int id, double newValue) {
        bank.assignBenefit(id, newValue);
    }

    /**
     * @return an {@link InteractionBank} part of logger.
     */
    public final InteractionBank getInteractionBank() {
        return bank;
    }

    @Override
    public String toString() {
        return new ToStringBuilder<InteractionLogger>(this)
               .add("bank", getInteractionBank())
               .toString();
    }
}
