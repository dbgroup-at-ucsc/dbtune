package edu.ucsc.dbtune.bip;

import java.util.List;
import java.util.Set;

import org.junit.Test;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static edu.ucsc.dbtune.util.TestUtils.workload;
import static edu.ucsc.dbtune.bip.CandidateGeneratorFunctionalTest.readCandidateIndexes;

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
        // 1. Set common parameters
        getEnvironmentParameters();
        setParameters();

        if (!(io instanceof InumOptimizer))
            return;
        
        // 2. Generate candidate indexes
        workload = workload(en.getWorkloadsFoldername() + "/tpch-inum");
        candidates = readCandidateIndexes(); 
        nReplicas = 3;
        loadfactor = 2;
        B = Math.pow(2, 28); 
                    
        // basic case
        testDiv();
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
            totalCostBIP = div.getObjValue() + div.getTotalBaseTableUpdateCost();            
            System.out.println(" TOTAL COST(INUM): " + totalCostBIP);
            
            Set<Index> conf;
            DivConfiguration divConf = (DivConfiguration) output;
            List<Double> costInum;
            List<Double> costCplex;
            double delta;
            double tolerance = 1;
            
            for (int r = 0; r < nReplicas; r++) {
                conf = divConf.indexesAtReplica(r);
                costInum = computeQueryCostsInum(conf);
                costCplex = div.getQueryCostReplicaByCplex(r);
                
                for (int q = 0; q < costCplex.size(); q++) {
                    
                    if (costCplex.get(q) > 0) {
                        delta = Math.abs(costCplex.get(q) - costInum.get(q));
                        System.out.println(" cost cplex: " + costCplex.get(q)
                                    + " verus. cost INUM: " + costInum.get(q));
                        assertThat(delta <= tolerance, is(true));
                    }
                    
                }
            }
            
        } else 
            System.out.println(" NO SOLUTION ");
    }
}
