package edu.ucsc.dbtune.advisor.interactions;

import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;

public class InteractionLogger
{
    private InteractionBank bank; 
    
    public InteractionLogger(Set<Index> candidateSet0)
    {
        bank = new InteractionBank(candidateSet0);
    }
    
    /*
     * Assign interaction with an exact value
     */
    public final void assignInteraction(Index id1, Index id2, double newValue)
    {       
        bank.assignInteraction(id1, id2, newValue);
    }

    public void assignBenefit(Index id, double newValue)
    {
        bank.assignBenefit(id, newValue);
    }

    public final InteractionBank getInteractionBank()
    {
        return bank;
    }
}
