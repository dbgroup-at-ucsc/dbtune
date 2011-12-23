package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.util.MatIndex;
import edu.ucsc.dbtune.metadata.Index;

public class DivBIPTest extends BIPTestConfiguration 
{
    @Test
    public void testDivergentDesign() throws Exception
    {  
        
        DivBIP div = new DivBIP();
        int Nreplicas = 3;
        int loadfactor = 2;
        double B = 300;
       
        List<Index> divCandidateIndexes = new ArrayList<Index>();
        for (List<Index> listIndexQuery : listIndexQueries) {
            for (Index idx : listIndexQuery) {
                divCandidateIndexes.add(idx);
            }
        }
        
        List<List<MatIndex>> recommendedConfiguration = div.optimalDiv(listWorkload, listAgent, divCandidateIndexes, Nreplicas, loadfactor, B);
        String strConfiguration = div.printRecommendedConfiguration(recommendedConfiguration);
        
        System.out.println(strConfiguration);
    }
}
