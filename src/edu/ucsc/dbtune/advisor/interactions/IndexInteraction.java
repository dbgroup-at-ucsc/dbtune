package edu.ucsc.dbtune.advisor.interactions;

import edu.ucsc.dbtune.metadata.Index;

public class IndexInteraction 
{
    private Index a;
    private Index b;
    private double doi;
    
    public IndexInteraction(Index a, Index b, double doi)
    {
        this.a = a;
        this.b = b;
        this.doi = doi;
    }
    
    public Index getFirst()
    {
        return a;
    }
    
    public Index getSecond()
    {
        return b;
    }
    
    public double getLowerDegreeOfInteraction()
    {
        return doi;
    }

    @Override
    public String toString() {
        return "IndexInteraction [first=" + a + ", second=" + b + ", doi=" + doi + "]\n";
    }
    
}
