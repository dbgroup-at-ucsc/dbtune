package edu.ucsc.dbtune.bip;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.junit.Test;

import edu.ucsc.dbtune.bip.util.BIPIndexPool;
import edu.ucsc.dbtune.bip.util.QueryPlanDesc;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class QueryPlanDescTest extends BIPTestConfiguration 
{
    @Test
    public void testPlanDescriptionGeneration() throws Exception
    {   
        BIPIndexPool poolIndexes = new BIPIndexPool();
        for (Index index : candidateIndexes) {
            poolIndexes.addIndex(index);
        }
        List<SQLStatement> listStmt = new ArrayList<SQLStatement>();
        for (Entry<Schema, Workload> entry : mapSchemaToWorkload.entrySet()) {
            listStmt.add(entry.getValue().get(0));
        }
        
        for (int q = 0; q < numQ; q++) {
            QueryPlanDesc desc = new  QueryPlanDesc();
            
            desc.generateQueryPlanDesc(listPreparators.get(q), listStmt.get(q), poolIndexes);
    
            assertThat(desc.getNumSlots(), is(numSchemaTables));
            assertThat(desc.getNumPlans(), is(numPlans[q]));
            
            for (int k = 0; k < desc.getNumPlans(); k++) {
                if (q == 0){
                    assertThat(desc.getInternalPlanCost(k), is(internalCostPlan1[k]));
                } else if (q == 1) {
                    assertThat(desc.getInternalPlanCost(k), is(internalCostPlan2[k]));
                }
            }
            
            for (int i = 0; i < numSchemaTables; i++) {
                assertThat(desc.getNumIndexesEachSlot(i), is(3));
            }
            
            int p = 0;
            for (int k = 0; k < desc.getNumPlans(); k++) {
                for (int i = 0; i < numSchemaTables; i++) {
                    for (int a = 0; a < desc.getNumIndexesEachSlot(i); a++){
                        if ( (q == 0 && i <= 1) // the first query
                             || (q == 1 && i >= 1) // the second query 
                           ){
                            if (a == desc.getNumIndexesEachSlot(i) - 1) {
                                assertThat(desc.getIndexAccessCost(k, i, a), is(100.0));
                            }
                            else {
                                if (q == 0) {
                                    assertThat(desc.getIndexAccessCost(k, i, a), 
                                            is(accessCostPlan1[p]));
                                }
                                else if (q == 1) {
                                    assertThat(desc.getIndexAccessCost(k, i, a), 
                                            is(accessCostPlan2[p]));
                                }
                                p++;
                            }
                        } else {
                            // assert value 0
                            assertThat(desc.getIndexAccessCost(k, i, a), is(0.0));
                        }
                    }
                }
            }
        }
        System.out.println("The generator of query plan descriptor is tested SUCCESSFULLY.");
    }
}
