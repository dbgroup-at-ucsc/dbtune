package edu.ucsc.dbtune.bip.interactions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
     * Compute F-measure of the the two sets
     * 
     * @param other
     *      A set of interaction pairs
     *      
     * @return 
     *      The F-measure value      
     */
    public double f_measure(Set<IndexInteraction> other)
    {
        int pa;
        int pb;
        int pc;
        Set<IndexInteraction> intersect;
        Set<IndexInteraction> left;
        Set<IndexInteraction> right;
        
        intersect = new HashSet<IndexInteraction>(interactions);
        left      = new HashSet<IndexInteraction>(intersect);
        right     = new HashSet<IndexInteraction>(other);
                
        intersect.retainAll(other);
        pa = intersect.size();

        left.removeAll(other);
        pb = left.size();
        
        right.removeAll(new HashSet<IndexInteraction>(interactions));
        pc = right.size();
     
        System.out.println("L51 (InteractionOutput), pa: " + pa + " pb: " + pb + " pc: " + pc);
        return (double) (2 * pa) / (2 * pa + pb + pc);
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
