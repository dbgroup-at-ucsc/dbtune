package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;



import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.div.ElasticDivBIP;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.util.Environment;

import static edu.ucsc.dbtune.bip.CandidateGeneratorFunctionalTest.readCandidateIndexes;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.computeDeploymentCost;



public class ElasticDivBIPFunctionalTest extends DivTestSetting 
{   
    private static double upperCost;
    private static DivConfiguration sourceConf;
    private static DivConfiguration destinationConf;
    private static int nDeploys;
    
    /**
     * Setup for the test.
     */
    @BeforeClass
    public static void beforeClassSetUp() throws Exception
    {
        en = Environment.getInstance();
        db = newDatabaseSystem(en);
    }
    
    @Test
    public void testShrinkReplicaDivergentDesign() throws Exception
    {   
        LogListener logger = LogListener.getInstance();
        
        //double updateCost;
        //double queryCost;
        double totalCostBIP;
        
        // 1. Set common parameters
        getEnvironmentParameters();
        
        // 2. set parameters
        setParameters();
     
        candidates = readCandidateIndexes();
        
        if (!(io instanceof InumOptimizer))
            return;
        
        // compute the  upper bound cost
        computeUpperBoundDeployCost();
                
        double factors[] = {Math.pow(2, -1), Math.pow(2, -4), Math.pow(2, -8), 
                            Math.pow(2, -16), 0};
        double deployCost; 
        DivConfiguration dest = new DivConfiguration(0, 0);
        
        nReplicas = 4;
        nDeploys = 3;
        loadfactor = 2;
        
        for (double factor : factors) { 
        
            // Call elastic
            ElasticDivBIP elastic = new ElasticDivBIP();
            elastic.setCandidateIndexes(candidates);
            elastic.setWorkload(workload); 
            elastic.setOptimizer((InumOptimizer) io);
            elastic.setSpaceBudget(B);
            elastic.setLogListenter(logger);
            
            elastic.setUpperDeployCost(upperCost * factor);
            elastic.setInitialConfiguration(sourceConf);
            elastic.setNumberDeployReplicas(nDeploys);

            // process after
            DivConfiguration after = (DivConfiguration) elastic.solve();
            dest.copyAndRemoveEmptyConfiguration(after);
            
            // output the result
            deployCost = computeDeploymentCost(sourceConf, dest);
            
            // add the update-base-table-constant costs
            // this handling is tricky
            // since we actually deploy only {@nDeploys}
            totalCostBIP = elastic.getObjValue();
            
            System.out.println("\n\n\n------------ upperbound cost: " + (factor * upperCost) + "\n"
                               + " new deploy cost: " + deployCost + "\n"
                               + " TOTAL cost: " + totalCostBIP +  "\n"
                               );
                               //+ " NEW configuration: " + dest);
        }
      
    }
    
    private static void computeUpperBoundDeployCost() throws Exception
    {           
        // get the configuration        
        nReplicas = 4;
        loadfactor = 2;        
        DivBIPFunctionalTest.testDiv();        
        sourceConf = new DivConfiguration(divConf);
        
        // run with two replicas
        nReplicas = 3;
        loadfactor = 2;        
        DivBIPFunctionalTest.testDiv();        
        destinationConf = new DivConfiguration(divConf);
        
        upperCost = computeDeploymentCost(sourceConf, destinationConf);
        System.out.println(" UPPER deployment cost: " + upperCost);
    }
}
