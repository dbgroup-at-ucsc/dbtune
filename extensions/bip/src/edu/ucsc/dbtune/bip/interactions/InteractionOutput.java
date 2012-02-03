package edu.ucsc.dbtune.bip.interactions;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.bip.core.BIPOutput;
import edu.ucsc.dbtune.advisor.interactions.IndexInteraction;

public class InteractionOutput extends BIPOutput 
{
    private List<IndexInteraction> listInteractions;
    
    public InteractionOutput()
    {
        listInteractions = new ArrayList<IndexInteraction>();
    }
    
    /**
     * Add a pair of interaction indexes into the list
     * 
     * @param pairIndexes
     *      A pair of indexes that are found to interact with each other
     */
    public void addPairIndexInteraction(IndexInteraction pairIndexes)
    {
        listInteractions.add(pairIndexes);
    }
    
    public List<IndexInteraction> getListInteractions()
    {
        return listInteractions;
    }

    @Override
    public String toString() 
    {
        StringBuilder strInteractions = new StringBuilder("========= List of interactions ========\n");        
        for (IndexInteraction pair : listInteractions)
            strInteractions.append(pair.getFirst() + " ---- " 
                                    + pair.getSecond()
                                    + " \n");
        
        return strInteractions.toString();
    }
}
