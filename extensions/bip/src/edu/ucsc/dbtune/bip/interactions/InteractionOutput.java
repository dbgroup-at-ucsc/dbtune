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
    public void add(IndexInteraction pair)
    {
        interactions.add(pair);
    }
    
    public List<IndexInteraction> get()
    {
        return interactions;
    }

    public int size()
    {
        return interactions.size();
    }
    
    @Override
    public String toString() 
    {
        StringBuilder strInteractions = new 
            StringBuilder("-- List of interactions: " + interactions.size() + "\n");
        
        for (IndexInteraction pair : interactions)
            strInteractions.append(pair.toString());
        
        return strInteractions.toString();
    }
}
