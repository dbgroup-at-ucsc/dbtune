package edu.ucsc.dbtune.bip;


import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

import java.util.HashSet;
import java.util.Set;
import org.junit.Test;


import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;

import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;

import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;


/**
 * Test the functionality of Divergent Design using BIP.
 * 
 * @author Quoc Trung Tran
 *
 */
public class DivBIPFunctionalTest extends DivTestSetting
{   
    protected static long totalIndexSize;
    protected static Set<Index> candidates;
    
    @Test
    public void testDivergentDesign() throws Exception
    {
        // 1. Set common parameters
        setCommonParameters();
        
        if (!(io instanceof InumOptimizer))
            return;

        // 2. Generate candidate indexes
        generateCandidates();
        
        // 3. Call divergent design
        for (int i = 0; i < arrNReplicas.length; i++) {
            
            nReplicas = arrNReplicas[i];
            loadfactor = arrLoadFactor[i];
        
            if (isTestOne && nReplicas != 4)
                continue;
                
            System.out.println("----------------------------------------");
            System.out.println(" DIV-BIP, # replicas = " + nReplicas
                                + ", load factor = " + loadfactor
                                + ", space = " + B);
            testDiv();
            System.out.println("----------------------------------------");
        }
            
        
        // 4. Call Uniform design       
        nReplicas = 1;
        loadfactor = 1;
        
        System.out.println("----------------------------------------");
        System.out.println(" DIV-UNIF-BIP, # replicas = " + nReplicas
                            + ", load factor = " + loadfactor);
        
        testDiv();
        
        // get the query cost & update cost
        double queryCost;
        double updateCost;
        double totalCostUniform;
        
        // update index cost & query shell in update statement 
        updateCost = div.getUpdateCost();
        // query-only cost
        queryCost  = div.getObjValue() - updateCost;
        // add the constant update-base table cost
        updateCost += div.getTotalBaseTableUpdateCost();
        
        // UNIF for the case with more than one replica
        for (int i = 0; i < arrNReplicas.length; i++) {
            System.out.println("----------------------------------------");
            totalCostUniform = queryCost + updateCost * arrNReplicas[i];            
            System.out.println(" DIV-UNIF, # replicas: " + arrNReplicas[i] + "\n"
                                + " query cost: " + queryCost + "\n"
                                + " update base table (one replica): " + 
                                div.getTotalBaseTableUpdateCost() + "\n"
                                + " update cost (one replica): " + updateCost + "\n"
                                + " TOTAL COST: " + totalCostUniform);
            System.out.println("----------------------------------------");
        }
           
        System.out.println("----------------------------------------");
    }
    
    /**
     * Generate candidate indexes
     */
    protected static void generateCandidates() throws Exception
    {
        CandidateGenerator candGen =
            new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        candidates = candGen.generate(workload);
        
        // Calculate the total size (for solely information)
        totalIndexSize = 0;
        for (Index index : candidates)
            totalIndexSize += index.getBytes();
        
        System.out.println("Number of statements: " + workload.size() + "\n"
                            + "Number of candidate: " + candidates.size() + "\n" 
                            + "Total size: " + totalIndexSize);
    }
    
    
    /**
     * Run the BIP with the parameters set by other functions
     * @throws Exception
     */
    protected static void testDiv() throws Exception
    {
        div = new DivBIP();
        
        Optimizer io = db.getOptimizer();
                
        LogListener logger = LogListener.getInstance();
        div.setCandidateIndexes(candidates);
        div.setWorkload(workload); 
        div.setOptimizer((InumOptimizer) io);
        div.setNumberReplicas(nReplicas);
        div.setLoadBalanceFactor(loadfactor);
        div.setSpaceBudget(B);
        div.setLogListenter(logger);
        
        IndexTuningOutput output = div.solve();
        System.out.println(logger.toString());
        
        if (isExportToFile)
            div.exportCplexToFile(en.getWorkloadsFoldername() + "test.lp");
        
        double totalCostBIP;
        
        if (output != null) {
            System.out.println("CPLEX result: " 
                    + " obj value: " + div.getObjValue() + "\n"
                    + " different from optimal value: " + div.getObjectiveGap() + "\n"
                    + " base table update cost: " + div.getTotalBaseTableUpdateCost());
           
            // add the update-base-table-constant costs
            totalCostBIP = div.getObjValue() + div.getTotalBaseTableUpdateCost();            
            System.out.println(" TOTAL COST(INUM): " + totalCostBIP);
            
            // show imbalance query & replica
            div.computeImbalanceFactor();   
            
            if (isTestCost) {
                Set<Index> conf;
                DivConfiguration divConf = (DivConfiguration) output;
                conf = divConf.indexesAtReplica(0);
               
                // query cost
                computeQueryCosts(conf);
                // empty configuration
                computeQueryCosts(new HashSet<Index>());
            }
            
        } else 
            System.out.println(" NO SOLUTION ");
        
    }
}
