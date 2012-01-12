package edu.ucsc.dbtune.advisor.interactions;

import edu.ucsc.dbtune.metadata.Index;

public class IndexInteraction 
{
    private Index a;
    private Index b;
    
    public IndexInteraction(Index a, Index b)
    {
        this.a = a;
        this.b = b;
    }
    
    public Index getFirst()
    {
        return a;
    }
    
    public Index getSecond()
    {
        return b;
    }
}
