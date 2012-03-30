package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.workload;


import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;

import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

public class DivBIPFunctionalTest extends BIPTestConfiguration 
{
    private static DatabaseSystem db;
    private static Environment    en;
    
    private static int Nreplicas;
    private static int loadfactor;
    private static double B;
    
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
    public void testDivergentDesign() throws Exception
    {  
        // parameter setting for divergent design tuning
        divergentNormal();
        
        DivBIP div = new DivBIP();
        
        Workload workload = workload(en.getWorkloadsFoldername() + "/tpcds-small");
        CandidateGenerator candGen = 
            new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        Set<Index> candidates = candGen.generate(workload);
       
        for (Index index : candidates)
            index.setBytes(1);
        System.out.println("L59, number of candidate: " + candidates.size());
        
        Optimizer io = db.getOptimizer();

        if (!(io instanceof InumOptimizer))
            throw new Exception("Expecting InumOptimizer instance");
                
        LogListener logger = LogListener.getInstance();
        div.setCandidateIndexes(candidates);
        div.setWorkload(workload); 
        div.setOptimizer((InumOptimizer) io);
        div.setNumberReplicas(Nreplicas);
        div.setLoadBalanceFactor(loadfactor);
        div.setSpaceBudget(B);
        div.setLogListenter(logger);
        
        IndexTuningOutput output = div.solve();
        div.exportCplexToFile(en.getWorkloadsFoldername() + "/tpcds-small/test.lp");
        System.out.println(logger.toString());
        if (output != null) {
            System.out.println("In test, result: " 
                    + " obj value: " + div.getObjValue()
                    + " different from optimal value: " + div.getObjectiveGap());
            
            /*
            // run on actual optimize
            Set<SQLStatement> sqls = new HashSet<SQLStatement>();
            for (int i = 0; i < workload.size(); i++)
                sqls.add(workload.get(i));
            DivergentOnOptimizer doo = new DivergentOnOptimizer();
            doo.verify(io.getDelegate(), output, sqls);
            
            double costDB2 = doo.getTotalCost() / loadfactor;
            System.out.println("L90, cost on DB2: " + costDB2
                                + " COST RATIO: " + (double) costDB2 / div.getObjValue());
            */
            /*
            // test
            DivConfiguration divconf = (DivConfiguration) output;
            Set<Index> conf = divconf.indexesAtReplica(0);
            System.out.println(" Configuration: " + conf);
            System.out.println(" query INUM DB2 INUM/DB2 ");
            double costInum, costDB2, ratio;
            
            for (int i = 0; i < workload.size(); i++){
                
                System.out.println(" ---- query " + i + "\n");
                InumPreparedSQLStatement inumStmt = (InumPreparedSQLStatement)
                                                io.prepareExplain(workload.get(i));
                ExplainedSQLStatement inumExplain = inumStmt.explain(conf);
                ExplainedSQLStatement db2Explain =  io.getDelegate().explain(workload.get(i), conf);
                                                  
                costInum = inumExplain.getTotalCost();
                costDB2  = db2Explain.getTotalCost();
                
                ratio = costInum / costDB2;
                
                if (ratio >= 1.5 || ratio <= 0.5 && i == 1) {                    
                    System.out.println(i 
                                       + " " + costInum
                                       + " " + costDB2
                                       + " " + (costInum / costDB2));
                    System.out.println(" template \n");
                    for (InumPlan plan : inumStmt.getTemplatePlans())
                        System.out.println(plan);
                    System.out.println(" instantiated INUM: \n " + inumExplain.getPlan());
                    
                    System.out.println(" instantiated DB2: \n" + db2Explain.getPlan());                    
              }
              
            }
            */
        } else {
            System.out.println(" NO SOLUTION ");
        }
    }
    
    public static void divergentUnlimitSpace()
    {
        Nreplicas = 3;
        loadfactor = 2;
        B = 70;
    }
    
    public static void optimalDesign()
    {
        Nreplicas = 1;
        loadfactor = 1;
        B = 5;
    }
    
    public static void divergentNormal()
    {
        Nreplicas = 3;
        loadfactor = 2;
        B = 3;
    }
}
