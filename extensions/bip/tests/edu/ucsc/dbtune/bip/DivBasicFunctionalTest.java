package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static edu.ucsc.dbtune.bip.CandidateGeneratorFunctionalTest.readCandidateIndexes;

import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;

/**
 * This test aims for the correctness of DivBIP; i.e., the cost computed by CPLEX
 * corresponds to the optimal cost by INUM
 * 
 * @author Quoc Trung Tran
 *
 */
public class DivBasicFunctionalTest extends DivTestSetting 
{
    @Test
    public void testBasicDiv() throws Exception
    {   
        // 1. Read common parameter
        getEnvironmentParameters();
        
        // 2. set parameter for DivBIP()
        setParameters();
        
        if (!(io instanceof InumOptimizer))
            return;
        
        // 3. delete QueryPlanDesc and  candidate indexes        
        File file = new File(folder + "/query-plan-desc.bin");
        if (file.exists())
            file.delete();
        
        file = new File(folder + "/candidate-optimizer.bin");
        if (file.exists())
            file.delete();
        
        
        candidates = readCandidateIndexes();
        System.out.println(" space budget: " + B
                        + " number of candidate indexes: " + candidates.size());
        
        // reset the counter in Index class
        int maxID = -1;
        for (Index index : candidates)
            if (index.getId() > maxID)
                maxID = index.getId();
        
        Index.IN_MEMORY_ID = new AtomicInteger(maxID + 1);
        // ----------------------------------------------------
        
        B = listBudgets.get(0);
        nReplicas = listNumberReplicas.get(0);
        loadfactor = (int) Math.ceil( (double) nReplicas / 2);
                        
        // basic case
        testDiv();
        
        // compare with DB2 cost
    }
    
    /**
     * Running DIV-BIP, and guarantee the query cost computed by INUM and CPLEX
     * is the same (with some small tolerance)
     * 
     * @throws Exception
     */
    private static void testDiv() throws Exception
    {
        div = new DivBIP();
        
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
            div.exportCplexToFile(en.getWorkloadsFoldername() + "/test.lp");
        
        double totalCostBIP;
        
        if (output != null) {
            System.out.println("CPLEX result: " 
                    + " obj value: " + div.getObjValue() + "\n"
                    + " different from optimal value: " + div.getObjectiveGap() + "\n"
                    + " base table update cost: " + div.getTotalBaseTableUpdateCost());
            
            // add the update-base-table-constant costs
            totalCostBIP = div.getObjValue() ; // +div.getTotalBaseTableUpdateCost();            
            System.out.println(" TOTAL COST(INUM): " + totalCostBIP);
            
            Set<Index> conf;
            DivConfiguration divConf = (DivConfiguration) output;
            List<Double> costInum;
            List<Double> costDB2;
            List<Double> costCplex;
            double ratio;
            double tolerance = 1.1;
            
            //System.out.println(" configuration: " + divConf);
            
            for (int r = 0; r < nReplicas; r++) {
                
                conf = divConf.indexesAtReplica(r);
                
                System.out.println(" replica: " + r );
                for (Index index : conf)
                    System.out.println(index.getId() + "   " + index);
                System.out.println("-----------");
                
                
                costInum = computeQueryCostsInum(conf);
                costDB2 = computeQueryCostsDB2(conf);
                costCplex = div.getQueryCostReplicaByCplex(r);
                
                
                for (int q = 0; q < costCplex.size(); q++) {
                    
                    // ONLY CHECK THE COST FOR SELECT- statement
                    if (workload.get(q).getSQLCategory().isSame(NOT_SELECT))
                        continue;
                    
                    // avoid very small value
                    // due to the approximation of CPLEX
                    if (costCplex.get(q) > 0.1 && costInum.get(q) > 0.0) {
                        ratio = (double) Math.abs(costCplex.get(q) / costInum.get(q));

                        if (ratio < 1)
                            ratio = (double) 1 / ratio;
                        
                        System.out.println("query: " + q + " cost cplex: " + costCplex.get(q)
                                + " verus. cost INUM: " + costInum.get(q)
                                + " versus. cost DB2: " + costDB2.get(q)
                                + " RATIO DB2 / INUM: " 
                                + (costDB2.get(q) / costInum.get(q))
                                );
                        
                        assertThat(ratio <= tolerance, is(true));
                    }
                }
                
               
            }
            
        } else 
            System.out.println(" NO SOLUTION ");
    }
}
