package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import edu.ucsc.dbtune.bip.div.DivRecommendedConfiguration;
import edu.ucsc.dbtune.bip.div.ElasticDivBIP;
import edu.ucsc.dbtune.metadata.Index;

public class ElasticDivBIPTest extends BIPTestConfiguration 
{
    @Test
    public void testShrinkReplicaDivergentDesign() throws Exception
    {  
        ElasticDivBIP div = new ElasticDivBIP();
        int Nreplicas = 3;
        int Ndeploy = 2;
        int loadfactor = 2;
        double upperCdeploy = 200;
        Map<Index, List<Integer>> mapIndexesReplicasInitialConfiguration = new HashMap<Index, List<Integer>>(); 
        
        List<Index> divCandidateIndexes = new ArrayList<Index>();
        for (List<Index> listIndexQuery : listIndexQueries) {
            for (Index idx : listIndexQuery) {
                divCandidateIndexes.add(idx);
                System.out.println(" Index: " + idx.getFullyQualifiedName());
                // assume schema_0.table_0_index_1 is materialized at replica 0 and 1
                if (idx.getFullyQualifiedName().contains("schema_0.table_0_index_1")){
                    List<Integer> listReplicas = new ArrayList<Integer>();
                    listReplicas.add(new Integer(0));
                    listReplicas.add(new Integer(1));
                    mapIndexesReplicasInitialConfiguration.put(idx, listReplicas);
                }
            }
        }
        
        DivRecommendedConfiguration conf = div.optimalShrinkReplicaDiv(listWorkload, listPreparators, divCandidateIndexes, 
                                                                       mapIndexesReplicasInitialConfiguration, 
                                                                       Nreplicas, Ndeploy, loadfactor, upperCdeploy);
        System.out.println("In test, result: " + conf.toString());
        
    }
}
