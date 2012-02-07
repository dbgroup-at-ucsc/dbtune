package edu.ucsc.dbtune.advisor.interactions;

import edu.ucsc.dbtune.metadata.Index;

public class IndexInteraction 
{
    private Index a;
    private Index b;
    private double doiBIP, doiOptimizer;
    
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
    
    /**
     * Retrieve the degree of interaction computing using convention optimizer
     * 
     * @return  
     *      The {@code doi} value
     */
    public double getDoiOptimizer()
    {
        return doiOptimizer;
    }
    
    
    /**
     * Retrieve the degree of interaction computed by BIP method
     * 
     * @return  
     *      The {@code doi} value
     */
    public double getDoiBIP()
    {
        return doiBIP;
    }
    
    
    public void setDoiOptimizer(double doi)
    {
        doiOptimizer = doi;
    }
    
    public void setDoiBIP(double doi)
    {
        doiBIP = doi;
    }
    
    @Override
    public String toString() {
        return "IndexInteraction [first=" + a + ", second=" + b + ", doiBIP=" + doiBIP + 
               ", doiOptimizer=" + doiOptimizer + "]\n";
    }
    
}
