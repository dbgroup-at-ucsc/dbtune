package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.bip.CandidateGeneratorFunctionalTest.readCandidateIndexes;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;


import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.DeploymentDivBIP;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.util.Environment;

import static edu.ucsc.dbtune.bip.div.DeploymentDivBIP.OPTIMIZE_DEPLOY_COST;
import static edu.ucsc.dbtune.bip.div.DeploymentDivBIP.OPTIMIZE_TOTAL_COST;

public class DeploymentDivBIPFunctionalTest extends DivTestSetting 
{
    private static double unifDeployCost;
    private static double unifTotalCost;
        
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
    public void testDeployConstraint() throws Exception
    {   
        /*
        // 1. Set common parameters
        getEnvironmentParameters();
        
        // 2. set parameters
        setParameters();
     
        candidates = readCandidateIndexes();
        
        if (!(io instanceof InumOptimizer))
            return;
        
        // 2. Uniform design
        // 3. Call divergent design    
        B = listBudgets.get(0);
        
        // compute the  upper bound cost
        DivBIPFunctionalTest.testUniformOneSpaceBudget(B);        
        double basicUnifDeployCost = div.getDeploymentCost();
        unifTotalCost = div.getObjValue();
        System.out.println("L55, UNIF deployment cost: " + div.getDeploymentCost());
            
        // deployment constraint
        //double deployFactors[] = {Math.pow(2,-5), Math.pow(2,-4), Math.pow(2,-3), Math.pow(2,-2), Math.pow(2,-1),
          //                       1, 1.2, 1.4, 1.6, 1.8, 2, 2.4, 2.8, 3, 4, 5, 10};
        double deployFactors[] = {Math.pow(2, -2)};
        
        double totalFactors[] = {1, 0.9, 0.8, 0.7, 0.6, 0.5};
        //double totalFactors[] = {0.8};
        //double deployFactors[] = {1.8};
        
        String types[] = {OPTIMIZE_TOTAL_COST};
        List<DeploymentEntry> entries = new ArrayList<DeploymentEntry>();
        
        for (String type : types) {
            for (int n : listNumberReplicas){
                nReplicas = n;
                loadfactor = (int) Math.ceil( (double) nReplicas / 2);            
                unifDeployCost = basicUnifDeployCost;
                
                System.out.println("\n \n \n\n " + type + " number of replias = " + nReplicas);
                List<Double> result = new ArrayList<Double>();
                      
                if (type.equals(OPTIMIZE_TOTAL_COST))
                    result = testDeploy(type, deployFactors, (InumOptimizer) io);
                else if (type.equals(OPTIMIZE_DEPLOY_COST))
                    result = testDeploy(type, totalFactors, (InumOptimizer) io);
                
                DeploymentEntry entry = new DeploymentEntry(nReplicas, type, result);
                entries.add(entry);
            }
        }
        
        // write to file
        writeDeploymentInfoToFile(entries);
        */
    }
    
    protected static void writeDeploymentInfoToFile(List<DeploymentEntry> entries) 
                    throws Exception
    {
        PrintWriter   out;

        String name = en.getWorkloadsFoldername() + "/deployment.txt";
        out = new PrintWriter(new FileWriter(name), false);

        for (DeploymentEntry entry : entries)
            out.print(entry.toString());
        
        out.close();
    }

    
    protected static List<Double> testDeploy(String type, double[] factors, InumOptimizer io)
                throws Exception
    {
        List<Double> result = new ArrayList<Double>();
        
        // Run over every factor value
        for (double factor : factors) {
        
            LogListener logger = LogListener.getInstance();
            // Call elastic
            DeploymentDivBIP deploy = new DeploymentDivBIP(type);
            deploy.setCandidateIndexes(candidates);
            deploy.setWorkload(workload); 
            deploy.setOptimizer(io);
            deploy.setNumberReplicas(nReplicas);
            deploy.setLoadBalanceFactor(loadfactor);
            deploy.setSpaceBudget(B);
            deploy.setLogListenter(logger);
            
            
            if (type.equals(OPTIMIZE_TOTAL_COST))
                deploy.setUpperDeployCost(unifDeployCost * factor);
            else if (type.equals(OPTIMIZE_DEPLOY_COST))
                deploy.setUpperTotalCost(unifTotalCost * factor);
            
            IndexTuningOutput output = deploy.solve();
            System.out.println(logger.toString());
            
            if (isExportToFile)
                deploy.exportCplexToFile(en.getWorkloadsFoldername() + "/test.lp");
                    
                    
            if (output != null) {
                    
                System.out.println("CPLEX result: number of replicas: " + nReplicas + "\n" 
                                + " TOTAL cost:  " + deploy.getTotalCost() + "\n"
                                + " DEPLOYMENT cost: " + deploy.getDeploymentCost() + "\n"
                            );
                if (type.equals(OPTIMIZE_TOTAL_COST))
                    result.add(deploy.getTotalCost());
                else if (type.equals(OPTIMIZE_DEPLOY_COST))
                    result.add(deploy.getDeploymentCost());
                                        
            } else 
                System.out.println(" NO SOLUTION ");
        }
        
        return result;
    }
    
    /**
     * Keep the result
     * @author Quoc Trung Tran
     *
     */
    public class DeploymentEntry
    {
        public int nReplicas;
        public String type;
        public List<Double> result;
        
        public DeploymentEntry(int n, String t, List<Double> r)
        {
            this.nReplicas = n;
            this.type = t;
            this.result = r;
        }
        
        public String toString()
        {
            StringBuffer sb = new StringBuffer();
            sb.append(" type = " + type + "\n"
                      +" nReplicas = " + nReplicas + "\n");
            
            for (double v : result)
                sb.append(v + "\n");
            
            return sb.toString();
        }
        
    }
}
