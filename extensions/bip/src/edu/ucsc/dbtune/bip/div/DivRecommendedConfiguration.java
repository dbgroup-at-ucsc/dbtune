package edu.ucsc.dbtune.bip.div;

import java.util.ArrayList;
import java.util.List;
import edu.ucsc.dbtune.bip.util.BIPOutput;
import edu.ucsc.dbtune.metadata.Index;


public class DivRecommendedConfiguration  extends BIPOutput
{
    private int Nreplicas;
    private List<List<Index>> listIndexReplica;
    
    public DivRecommendedConfiguration(int nReplicas)
    {
        this.Nreplicas = nReplicas;
        listIndexReplica = new ArrayList<List<Index>>();
        for (int r = 0; r < this.Nreplicas; r++) {
            List<Index> listIndexes = new ArrayList<Index>();
            this.listIndexReplica.add(listIndexes);
        }
    }
    
    
    public void addIndexReplica(int r, Index index)
    {
        this.listIndexReplica.get(r).add(index);
    }


    @Override
    public String toString() {
        String result =  "DivRecommendedConfiguration [Nreplicas=" + Nreplicas
                        + ", listIndexReplica=\n" ;
        for (int r = 0; r < this.Nreplicas; r++) {
            result += ("Replica " + r + "-th: \n");
            for (Index index : this.listIndexReplica.get(r)) {
                result += (" CREATE INDEX "  + index.getFullyQualifiedName() + "\n");
            }
            result += "\n";
        }
        
        return result;
    }
}
