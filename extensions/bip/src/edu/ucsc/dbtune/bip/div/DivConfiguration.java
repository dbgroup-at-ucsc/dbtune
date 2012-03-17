package edu.ucsc.dbtune.bip.div;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.metadata.Index;


public class DivConfiguration extends IndexTuningOutput
{
    private int               nReplicas;
    private List<List<Index>> indexReplicas;
    
    /**
     * Construct an object with a given number of replica
     * 
     * @param nReplicas
     *      The number of replicas to deploy the database
     */
    public DivConfiguration(int nReplicas)
    {
        this.nReplicas = nReplicas;
        indexReplicas = new ArrayList<List<Index>>();
        
        for (int r = 0; r < this.nReplicas; r++)
            indexReplicas.add(new ArrayList<Index>());
        
    }
    
    
    /**
     * Materialize an index at a particular replica
     * 
     * @param r
     *      The replica ID
     * @param index
     *      The index that is materialized
     */
    public void addIndexReplica(int r, Index index)
    {
        indexReplicas.get(r).add(index);
    }


    @Override
    public String toString() 
    {
        StringBuilder result = new StringBuilder();
        
        result.append("Divergent Configuration")
              .append("[Nreplicas=" + nReplicas)
              .append("\n");
        
                        
        for (int r = 0; r < nReplicas; r++) {
            
            result.append("Replica " + r + "-th: \n");
            
            for (Index index : indexReplicas.get(r))
                result.append(" CREATE INDEX "  + index + index.getId() + "\n");
            
            result.append("\n");
        }
        
        return result.toString();
    }
}
