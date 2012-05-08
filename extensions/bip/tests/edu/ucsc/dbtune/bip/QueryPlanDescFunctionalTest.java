package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


import java.util.Set;

import org.junit.Test;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.bip.core.InumQueryPlanDesc;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class QueryPlanDescFunctionalTest  
{
    protected static DatabaseSystem db;
    protected static Environment    en;
        
    @Test
    public void testPlanDescriptionGeneration() throws Exception
    {
        en = Environment.getInstance();
        db = newDatabaseSystem(en);
        
        if (!(db.getOptimizer() instanceof InumOptimizer))
            return;

        Workload workload = workload(en.getWorkloadsFoldername());
        CandidateGenerator candGen = 
            new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        Set<Index> candidates = candGen.generate(workload);
        
        assertThat(workload.size() > 0, is(true));
        SQLStatement stmt = workload.get(0);
        QueryPlanDesc desc = InumQueryPlanDesc.getQueryPlanDescInstance(stmt);
        
        Optimizer io = db.getOptimizer();
        
        if (!(io instanceof InumOptimizer))
            throw new Exception("Expecting InumOptimizer instance");
        
        // Populate the INUM space 
        desc.generateQueryPlanDesc((InumOptimizer)io, candidates);
        
        // call INUM directly
        InumPreparedSQLStatement preparedStmt;
        Set<InumPlan> templatePlans;
        
        // Get the template plans from INUM
        preparedStmt  = (InumPreparedSQLStatement) io.prepareExplain(stmt);
        templatePlans = preparedStmt.getTemplatePlans();
        
        // Number of template plans
        assert(templatePlans.size() == desc.getNumberOfTemplatePlans());
        int k = -1;
        double cost;
        
        for (InumPlan plan : templatePlans) {
            
            // find the one that is corresponding 
            boolean exist = false;
            for (k = 0; k < desc.getNumberOfTemplatePlans(); k++)
                if (desc.getInternalPlanCost(k) == plan.getInternalCost()) {
                    exist = true;
                    break;
                }
            
            assertThat(exist, is(true));
            assertThat(plan.getTables().size(), is(desc.getNumberOfSlots()));
            
            for (int i = 0; i < desc.getNumberOfSlots(); i++)
                for (Index index : desc.getIndexesAtSlot(i)) {
                    
                    cost = plan.plug(index);                    

                    if (cost == Double.POSITIVE_INFINITY)
                        cost = InumQueryPlanDesc.BIP_MAX_VALUE;

                    assertThat(cost, is(desc.getAccessCost(k, index)));
                }
            
            System.out.println("Plan \n" + plan + "\n");
        }    
        System.out.println(desc);
    }
}
