package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.junit.Test;


import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.util.Rt;


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
    public static int numMismatch = 0;
    
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
        
        file = new File(folder + "/candidates.bin");
        if (file.exists())
            file.delete();
        
        /*
        // reset the counter in Index class
        int maxID = -1;
        for (Index index : candidates)
            if (index.getId() > maxID)
                maxID = index.getId();
        
        Index.IN_MEMORY_ID = new AtomicInteger(maxID + 1);
        // ----------------------------------------------------
        */
        
        B = listBudgets.get(0);
        nReplicas = listNumberReplicas.get(0);
        loadfactor = (int) Math.ceil( (double) nReplicas / 2);
                        
        // basic case
        testDiv();
        
        Rt.p(" Number of mismatches between INUM cost and CPLEX costs: "
                + numMismatch);
    }
    
    /**
     * Running DIV-BIP, and guarantee the query cost computed by INUM and CPLEX
     * is the same (with some small tolerance)
     * 
     * @throws Exception
     */
    private static void testDiv() throws Exception
    {
        /*
        List<Double> inumOpt= computeQueryCostsInum(workload, candidates);
        List<Double> db2Opt= computeCostsDB2(workload, candidates);
        double inumTotal = 0.0, db2Total = 0.0;
        
        Rt.p("---TPCH10GB, 22 queries, OPTIMAL indexes");
        for (int i=0; i < inumOpt.size(); i++) {
            Rt.p(" INUM = " + inumOpt.get(i)
                    + " DB2 = " + db2Opt.get(i)
                    + " INUM / DB2 = " + inumOpt.get(i)/ db2Opt.get(i));
            inumTotal += inumOpt.get(i);
            db2Total += db2Opt.get(i);
        }
        Rt.p("INUM (OPTIMAL indexes): " + inumTotal
                + " DB2 (OPTIMAL indexes): " + db2Total
                + " INUM / DB2 = " + (inumTotal / db2Total));
        
        List<Double> inumFTS= computeQueryCostsInum(workload, new HashSet<Index>());
        List<Double> db2FTS= computeCostsDB2(workload, new HashSet<Index>());
        double inumFTSTotal = 0.0, db2FTSTotal = 0.0;
        
        Rt.p("TPCH10GB, 22 queries, FULL TABLE SCAN indexes");
        for (int i=0; i < inumFTS.size(); i++) {
            Rt.p(" INUM = " + inumFTS.get(i)
                    + " DB2 = " + db2FTS.get(i)
                    + " INUM / DB2 = " + inumFTS.get(i)/ db2FTS.get(i));
            inumFTSTotal += inumFTS.get(i);
            db2FTSTotal += db2FTS.get(i);
        }
        Rt.p("INUM (FTS indexes): " + inumFTSTotal
                + " DB2 (FTS indexes): " + db2FTSTotal
                + " INUM / DB2 = " + (inumFTSTotal / db2FTSTotal));
        System.exit(1);
        */
        
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
        Rt.p(logger.toString());
        
        if (isExportToFile)
            div.exportCplexToFile(en.getWorkloadsFoldername() + "/test.lp");
        
        double totalCostBIP;
        
        if (output != null) {
            Rt.p("CPLEX result: " 
                    + " obj value: " + div.getObjValue() + "\n"
                    + " different from optimal value: " + div.getObjectiveGap() + "\n"
                    + " base table update cost: " + div.getTotalBaseTableUpdateCost());
            
            // add the update-base-table-constant costs
            totalCostBIP = div.getObjValue() ; // +div.getTotalBaseTableUpdateCost();            
            Rt.p(" TOTAL COST(INUM): " + totalCostBIP);
            Rt.p("NODE IMBALANCE = " + div.getNodeImbalance());
            
            Set<Index> conf;
            DivConfiguration divConf = (DivConfiguration) output;
            List<Double> costInum;
            List<Double> costDB2;
            List<Double> costCplex;
            double ratio;
            double tolerance = 1.1;
            
            //Rt.p(" configuration: " + divConf);     
            for (int r = 0; r < nReplicas; r++) {
                
                conf = divConf.indexesAtReplica(r);
                
                Rt.p(" replica: " + r + " # indexes: " + conf.size());
                for (Index index : conf)
                    Rt.p(index.getId() + "   " + index);
                Rt.p("-----------");
                
                costInum = computeQueryCostsInum(workload, conf);
                costDB2 = computeCostsDB2(workload, conf);
                costCplex = div.getQueryCostReplicaByCplex(r);
                
                for (int q = 0; q < costCplex.size(); q++) {
                    
                    // ONLY CHECK THE COST FOR SELECT- statement
                    if (workload.get(q).getSQLCategory().isSame(NOT_SELECT))
                        continue;
                    
                    // avoid very small value
                    // due to the approximation of CPLEX
                    if (costCplex.get(q) > 0.0 && costInum.get(q) > 0.0) {
                        ratio = (double) Math.abs(costCplex.get(q) / costInum.get(q));

                        if (ratio < 1)
                            ratio = (double) 1 / ratio;
                        
                        Rt.p("query: " + q + " cost cplex: " + costCplex.get(q)
                                + " verus. cost INUM: " + costInum.get(q)
                                + " versus. cost DB2: " + costDB2.get(q)
                                + " RATIO DB2 / INUM: " 
                                + (costDB2.get(q) / costInum.get(q))
                                );
                        
                        if (ratio > tolerance) {
                            Rt.p(" ratio of CPLEX / INUM: " + ratio
                                    + " query = " + q
                                    + " tolerance = " + tolerance);
                            div.showStepComputeQuery(r, q);
                            showComputeQueryCostsInum(q, conf);
                            
                            numMismatch++;
                        }
                       
                    }
                }    
                
            }

        } else 
            Rt.p(" NO SOLUTION ");
    }
}
