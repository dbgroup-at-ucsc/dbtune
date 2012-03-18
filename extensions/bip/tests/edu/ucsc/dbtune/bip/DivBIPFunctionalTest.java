package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.ConstraintDivBIP;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.div.DivConstraint;
import edu.ucsc.dbtune.bip.div.DivergentOnOptimizer;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class DivBIPFunctionalTest extends BIPTestConfiguration 
{
    private static DatabaseSystem db;
    private static Environment    en;
    
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
        int Nreplicas = 3;
        int loadfactor = 2;
        double B;
        DivConstraint constraint = new DivConstraint(ConstraintDivBIP.IMBALANCE_REPLICA, 2.5);
        List<DivConstraint> constraints = new ArrayList<DivConstraint>();
        constraints.add(constraint);
        ConstraintDivBIP div = new ConstraintDivBIP(constraints);
        
        Workload workload = workload(en.getWorkloadsFoldername() + "/tpcds-small");
        CandidateGenerator candGen = 
            new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        Set<Index> candidates = candGen.generate(workload);
       
        for (Index index : candidates)
            index.setBytes(1);
        System.out.println("L59, number of candidate: " + candidates.size());
        
        // At most three indexes are deployed at one replica
        B = 10;
        
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
                              + " obj value: " + div.getObjValue());
            div.costFromCplex();
            /*
            // run on actual optimize
            Set<SQLStatement> sqls = new HashSet<SQLStatement>();
            for (int i = 0; i < workload.size(); i++)
                sqls.add(workload.get(i));
            DivergentOnOptimizer doo = new DivergentOnOptimizer();
            doo.verify(io.getDelegate(), output, sqls);
            
            System.out.println("L90, cost on DB2: " + doo.getTotalCost()
                                + " COST RATIO: " + (double) doo.getTotalCost() / div.getObjValue());
            
            // test
            DivConfiguration divconf = (DivConfiguration) output;
            Set<Index> conf = divconf.indexesAtReplica(0);
            System.out.println(" query INUM DB2 INUM/DB2 ");
            double costInum, costDB2;
            for (int i = 0; i < workload.size(); i++){
                costInum = io.prepareExplain(workload.get(i)).explain(conf).getTotalCost();
                costDB2  = io.getDelegate().explain(workload.get(i), conf).getTotalCost();
                System.out.println(i 
                                   + " " + costInum
                                   + " " + costDB2
                                   + " " + (costInum / costDB2));
            }
            */
        } else {
            System.out.println(" NO SOLUTION ");
        }
    }
}
