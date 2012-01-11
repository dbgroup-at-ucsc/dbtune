package edu.ucsc.dbtune.advisor.interactions;

import edu.ucsc.dbtune.metadata.Index;

public class IndexInteraction 
{
    private Index a;
    private Index b;
    private double interactionLevel;
    
    public IndexInteraction(Index a, Index b, double interactionLevel)
    {
        this.a = a;
        this.b = b;
        this.interactionLevel = interactionLevel;
    }
    
    public Index getFirst()
    {
        return a;
    }
    
    public Index getSecond()
    {
        return b;
    }

    public double getInteractionLevel()
    {
        return interactionLevel;
    }
}
