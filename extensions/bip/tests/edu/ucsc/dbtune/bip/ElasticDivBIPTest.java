package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.ElasticDivBIP;
import edu.ucsc.dbtune.metadata.Index;

public class ElasticDivBIPTest extends BIPTestConfiguration 
{
    @Test
    public void testShrinkReplicaDivergentDesign() throws Exception
    {   
        /*
        int Nreplicas = 3;
        int Ndeploys = 2;
        int loadfactor = 2;
        double upperCdeploy = 200;
        double B = 300;
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
        ElasticDivBIP div = new ElasticDivBIP(Nreplicas, loadfactor, B, Ndeploys, upperCdeploy,
                                              mapIndexesReplicasInitialConfiguration);
        div.setCandidateIndexes(divCandidateIndexes);
        div.setMapSchemaToWorkload(mapSchemaToWorkload);
        div.setWorkloadName("testwl");
        BIPOutput result = div.solve();
        System.out.println("In test, result: " + result.toString());
        */
    }
    
    /*
    @Test
    public void testExpandReplicaDivergentDesign() throws Exception
    {  
        ElasticDivBIP div = new ElasticDivBIP();
        int Nreplicas = 3;
        int Ndeploy = 4;
        int loadfactor = 3;
        double upperCdeploy = 600;
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
        
        DivRecommendedConfiguration conf = div.optimalShrinkReplicaDiv(mapSchemaToWorkload, listPreparators, divCandidateIndexes, 
                                                                       mapIndexesReplicasInitialConfiguration, 
                                                                       Nreplicas, Ndeploy, loadfactor, upperCdeploy);
        System.out.println("In test, result: " + conf.toString());
    }
    */
}
