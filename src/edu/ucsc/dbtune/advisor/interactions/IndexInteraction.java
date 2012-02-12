package edu.ucsc.dbtune.advisor.interactions;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.HashCodeUtil;

public class IndexInteraction 
{
    private Index a;
    private Index b;
    private double doiBIP, doiOptimizer;
    private int fHashCode;
    
    public IndexInteraction(Index a, Index b)
    {
        this.a = a;
        this.b = b;
        this.fHashCode = 0;
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
        return "IndexInteraction [first=" + a.getId() + ", second=" + b.getId() + ", doiBIP=" + doiBIP + 
               ", doiOptimizer=" + doiOptimizer + "]\n";
    }
    
    @Override
    public boolean equals(Object obj) 
    {
        if (!(obj instanceof IndexInteraction))
            return false;

        IndexInteraction pair = (IndexInteraction) obj;
        
        if ( (this.a.getId() != pair.a.getId())
            || (this.b.getId() != pair.b.getId()))
            return false;
        
        return true;
    }

    @Override
    public int hashCode() 
    {
        if (fHashCode == 0) {
            int result = HashCodeUtil.SEED;
            result = HashCodeUtil.hash(result, this.a.getId());
            result = HashCodeUtil.hash(result, this.b.getId());
            fHashCode = result;
        }
        
        return fHashCode;
    }
    
}
