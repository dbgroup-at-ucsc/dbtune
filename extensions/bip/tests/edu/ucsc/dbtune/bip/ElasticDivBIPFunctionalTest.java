package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;


import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.div.ElasticDivBIP;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Rt;

import static edu.ucsc.dbtune.bip.CandidateGeneratorFunctionalTest.readCandidateIndexes;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.computeDeploymentCost;

public class ElasticDivBIPFunctionalTest extends DivTestSetting 
{   
    private static double upperCost;
    private static DivConfiguration sourceConf;
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
        
        if (!(io instanceof InumOptimizer))
            return;
        
        // compute the  upper bound cost
        //computeUpperBoundDeployCost();
                
        double factors[] = {Math.pow(2, -1), Math.pow(2, -2), Math.pow(2, -3), Math.pow(2, -4), 
                            Math.pow(2, -5), Math.pow(2, -6), Math.pow(2, -7),
                            Math.pow(2, -12),
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
            
            //elastic.setUpperDeployCost(upperCost * factor);
            elastic.setInitialConfiguration(sourceConf);
            elastic.setNumberDeployReplicas(nDeploys);

            // process after
            DivConfiguration after = (DivConfiguration) elastic.solve();
                   
            if (isExportToFile)
                elastic.exportCplexToFile(en.getWorkloadsFoldername() + "/test.lp");
            
            
            
            // add the update-base-table-constant costs
            // this handling is tricky
            // since we actually deploy only {@nDeploys}
            totalCostBIP = elastic.getObjValue();
            
            dest.copyAndRemoveEmptyConfiguration(after);
            
            // output the result
            deployCost = computeDeploymentCost(sourceConf, after);
            
            
            System.out.println("\n\n\n------------ upperbound cost: " + (factor * upperCost) + "\n"
                               + " new deploy cost: " + deployCost + "\n"
                               + " TOTAL cost: " + totalCostBIP +  "\n"
                               );
                               //+ " NEW configuration: " + dest);
        }
    }
    
    
    
    /**
     * Test the elasticity aspect
     * 
     */
    public static List<Double> testElasticity(DivConfiguration initial, 
                                        List<Double> costs,
                                        int nDeploys, LogListener logger)
            throws Exception
    {   
        Optimizer io = db.getOptimizer();
        
        ElasticDivBIP elastic = new ElasticDivBIP();
        elastic.setCandidateIndexes(candidates);
        elastic.setWorkload(workload); 
        elastic.setOptimizer((InumOptimizer) io);
        elastic.setSpaceBudget(B);
        elastic.setLogListenter(logger);
        
        elastic.setInitialConfiguration(initial);
        elastic.setNumberDeployReplicas(nDeploys);
        elastic.setUpperDeployCost(costs);
        
        elastic.solve();
        Rt.p("Running time: " + logger);
        return elastic.getTotalCosts();
    }
}
