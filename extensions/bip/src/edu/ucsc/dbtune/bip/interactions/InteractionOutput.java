package edu.ucsc.dbtune.bip.interactions;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.advisor.interactions.IndexInteraction;

public class InteractionOutput extends IndexTuningOutput 
{
    private List<IndexInteraction> interactions;
    
    public InteractionOutput()
    {
        interactions = new ArrayList<IndexInteraction>();
    }
    
    /**
     * Add a pair of interaction indexes into the list
     * 
     * @param pair
     *      A pair of indexes that are found to interact with each other
     */
    public void addInteraction(IndexInteraction pair)
    {
        interactions.add(pair);
    }
    
    public List<IndexInteraction> getInteractions()
    {
        return interactions;
    }

    @Override
    public String toString() 
    {
        StringBuilder strInteractions = new StringBuilder("========= List of interactions ========\n");        
        for (IndexInteraction pair : interactions)
            strInteractions.append(pair.toString());
        
        return strInteractions.toString();
    }
}
