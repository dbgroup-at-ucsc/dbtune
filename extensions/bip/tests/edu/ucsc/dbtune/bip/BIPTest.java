package edu.ucsc.dbtune.bip;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.DBTuneInstances;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.inum.InumStatementPlan;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.metadata.Index; 
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

import edu.ucsc.dbtune.advisor.interactions.IndexInteraction;
import edu.ucsc.dbtune.bip.sim.MatIndex;
import edu.ucsc.dbtune.bip.sim.SimBIP;
import edu.ucsc.dbtune.bip.util.*;
import edu.ucsc.dbtune.bip.interactions.InteractionBIP;

import static org.junit.Assert.assertThat;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for the BIPTest class
 */
public class BIPTest
{ 
	/**
	 * Test case with two queries
	 * Each query consists of three (resp. two) query plans, two relations, and two indexes per relation
	 *  
	 */ 
	private static List<InumSpace> listInum;
	private static InumSpace inum;
	private static Set<InumStatementPlan> templatePlans;
	private static List<Table> listSchemaTables;
	private static Catalog cat;
	private static Schema sch;
	private static int numSchemaTables = 3;
	private static int numCandidateIndexes;
	
	private static double[] internalCostPlan1 = {140, 100, 160};
	private static double[] accessCostPlan1 = {10, 30, 20, 50, 80, 5, 90, 10, 20, 40, 30, 80};
	private static double[] sizeIndexPlan1 = {100, 80, 120, 150};
	private static double[] sizeIndexPlan2 = {200, 120, 60, 70};
	private static double[] internalCostPlan2 = {200, 150};
	private static double[] accessCostPlan2 = {20, 45, 30, 70, 80, 15, 90, 20};
    private static List<Index> candidateIndexes;
    private static int[] numPlans = {3, 2};
	private static int numQ = 2;
	private static Environment environment = Environment.getInstance();
	private static BIPAgent agent;
	
	static {
		try {
		    // 3 relations and 2 indexes per relation
			cat = DBTuneInstances.configureCatalog(1, 3, 2);
			sch = (Schema)cat.at(0);
			listInum = new ArrayList<InumSpace>();
            candidateIndexes = new ArrayList<Index>();
			listSchemaTables = new ArrayList<Table>();
			numCandidateIndexes = 0;
			
			// list of candidate indexes
			for (Index index : sch.indexes()) {
				numCandidateIndexes++;
				candidateIndexes.add(index);
			}
			
			for (Table table : sch.tables()) {
			    listSchemaTables.add(table);
			}
			
			// create two queries 
            // The first query: 1st and 2nd relation
            // The second query: 2nd and 3rd relation
			List< List<Index> > listIndexQueries = new ArrayList<List<Index>>();
			List< List<Table> > listTableQueries = new ArrayList<List<Table>>();
			
			for (int q = 0; q < numQ; q++) {
			    int idxTable = 0;
			    List<Index> listIndex = new ArrayList<Index>();
	            List<Table> listTable = new ArrayList<Table>();
			    
			    for (Table table : sch.tables()) {
			        // the first query: 1st and 2nd relation
			        if ( (q == 0 && idxTable <= 1) 
			           || (q == 1 && idxTable >= 1)){
			            listTable.add(table);
			            for (Index index : sch.indexes()) {
			                if (index.getTable().equals(table)){
			                    listIndex.add(index);
			                }
			            }
			        } 
			        idxTable++;
			    }
			    listIndexQueries.add(listIndex);
			    listTableQueries.add(listTable);
			}
						
			for (int q = 0; q < numQ; q++) {
				inum = mock(InumSpace.class);
				templatePlans = new LinkedHashSet<InumStatementPlan>();
				
				int counter = 0;
				System.out.println(" Query: " + q + " Number of template plans: " + numPlans[q]);
				for (int k = 0; k < numPlans[q]; k++) {
					InumStatementPlan plan = mock(InumStatementPlan.class);
					
					if (q == 0) {
						when(plan.getInternalCost()).thenReturn(internalCostPlan1[k]);
					}
					else if (q == 1) {
						when(plan.getInternalCost()).thenReturn(internalCostPlan2[k]);
					}
					int posIndexLocal = 0;
					for (Index index : listIndexQueries.get(q)) {	
						double val = 0.0, size = 0.0;
						
						if (q == 0) {
							val = accessCostPlan1[counter];
							size = sizeIndexPlan1[posIndexLocal++];
						}
						else if (q == 1) {
							val = accessCostPlan2[counter];
							size = sizeIndexPlan2[posIndexLocal++];
						}
						when (plan.getAccessCost(index)).thenReturn(val);
						when (plan.getMaterializedIndexSize(index)).thenReturn(size);
						counter++;
						
						System.out.println("In test, index: " + index.getName() 
											+ " access cost: " + val + " size index: " + size);					
					}
					
					
					for (Table table : listTableQueries.get(q)) {
						when (plan.getFullTableScanCost(table)).thenReturn(100.0);
					}
					when (plan.getReferencedTables()).thenReturn(listTableQueries.get(q));					
					when (plan.getSchemaTables()).thenReturn(listSchemaTables);
					templatePlans.add(plan);			   
				}
				System.out.println("Number of added template plans: " + templatePlans.size());
				when(inum.getTemplatePlans()).thenReturn(templatePlans);
								
				// Add @inum into @listInum
				listInum.add(inum);
			}
			
		}
		catch (Exception e){
			throw new RuntimeException(e);
		}
	}
	
    @Test
    public void testPlanDescriptionGeneration() throws Exception
    {    	    
    	for (int q = 0; q < numQ; q++) {
	    	QueryPlanDesc desc = new  QueryPlanDesc();
	    	desc.generateQueryPlanDesc(listInum.get(q), candidateIndexes);
	
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
    }
   
   
    @Test
    public void testInteraction() throws Exception
    {
    	try {
    		InteractionBIP bip = new InteractionBIP();
    		
    		agent = mock(BIPAgent.class);
    		when(agent.populateInumSpace()).thenReturn(listInum);    		
    		double delta = 0.4;
            List<Index> C = new ArrayList<Index>(candidateIndexes);
            List<IndexInteraction> listInteractions =
                bip.computeInteractionIndexes(agent, C, delta);
    		System.out.println(bip.printListInteraction(listInteractions));
    	} catch (SQLException e){
    		System.out.println(" error " + e.getMessage());
    	}
    }
    
    
    @Test
    public void testScheduling() throws Exception
    {
    	System.out.println("IN test, Number of candidate indexes: " + candidateIndexes.size());
    	
    	SimBIP sim = new SimBIP();
        List<Index> Sinit = new ArrayList<Index>();
        List<Index> Smat = new ArrayList<Index>();
    	int W = 4;
    	List<Double> B = new ArrayList<Double>();
    	double space = 140;
    	for (int w = 0; w < W; w++) {
    		B.add(new Double((w+1) * space));
    	}
    	
    	// Case 1: set Sinit = {}    	
    	Smat = candidateIndexes;
    	agent = mock(BIPAgent.class);
		when(agent.populateInumSpace()).thenReturn(listInum);
    	List<MatIndex> listIndex = sim.schedule(Sinit, Smat, agent, W, B);
    	String strSchedule = sim.printSchedule(listIndex, W);
    	System.out.println(strSchedule);
    }
}
