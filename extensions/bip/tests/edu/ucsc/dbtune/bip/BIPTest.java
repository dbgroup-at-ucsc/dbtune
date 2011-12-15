package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import com.ibm.db2.jcc.b.SqlException;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.DBTuneInstances;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.inum.InumStatementPlan;
import edu.ucsc.dbtune.metadata.Configuration;
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
//import static org.junit.Assert.assertThat;
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
	private static Catalog cat;
	private static Schema sch;
	private static int numRels = 2;
	private static int numIndexes;
	
	private static double[] internalCostPlan1 = {140, 100, 160};
	private static double[] accessCostPlan1 = {10, 30, 20, 50, 80, 5, 90, 10, 20, 40, 30, 80};
	private static double[] sizeIndexPlan1 = {100, 80, 120, 150};
	private static double[] sizeIndexPlan2 = {200, 120, 60, 70};
	private static double[] internalCostPlan2 = {200, 150};
	private static double[] accessCostPlan2 = {20, 45, 30, 70, 80, 15, 90, 20};
	private static List<Index> candidateIndexes; 	
	private static int[] numPlans = {3, 2};
	private static int numQ = 1;
	private static Environment environment = Environment.getInstance();
	private static BIPAgent agent;
	
	static {
		try {
			cat = DBTuneInstances.configureCatalog(2, 2, 2);
			sch = (Schema)cat.at(0);
			listInum = new ArrayList<InumSpace>();
			candidateIndexes = new ArrayList<Index>();
			numIndexes = 0;
			
			for (Index index : sch.indexes()) {
				numIndexes++;
				candidateIndexes.add(index);
			}
			
			// create two query plans
			// the last index in each query plan simulates the role of table scan
			for (int q = 0; q < numQ; q++) {
				inum = mock(InumSpace.class);
				templatePlans = new LinkedHashSet<InumStatementPlan>();
				
				int counter = 0;
				for (int k = 0; k < numPlans[q]; k++) {
					InumStatementPlan plan = mock(InumStatementPlan.class);
					
					if (q == 0) {
						when(plan.getInternalCost()).thenReturn(internalCostPlan1[k]);
					}
					else if (q == 1) {
						when(plan.getInternalCost()).thenReturn(internalCostPlan2[k]);
					}
					int posIndexLocal = 0;
					for (Index index : sch.indexes()) {	
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
					
					List<Table> listTables = new ArrayList<Table>();	
					for (Table table : sch.tables()) {
						listTables.add(table);
						when (plan.getFullTableScanCost(table)).thenReturn(100.0);
					}
					when (plan.getReferencedTables()).thenReturn(listTables);
					templatePlans.add(plan);			   
				}
				
				when(inum.getTemplatePlans()).thenReturn(templatePlans);
								
				// Add @inum into @listInum
				listInum.add(inum);
			}
			
		}
		catch (Exception e){
			throw new RuntimeException(e);
		}
	}
	
    @Before
    public void setUp() throws Exception
    {
    		
    }
    
    @After
    public void tearDown() throws Exception
    {

    }
    
    
    @Test
    public void testPlanDescriptionGeneration() throws Exception
    {    	    
    	for (int q = 0; q < numQ; q++) {
	    	QueryPlanDesc desc = new  QueryPlanDesc();
	    	desc.generateQueryPlanDesc(listInum.get(q), candidateIndexes);
	
	    	assertThat(desc.getNumSlots(), is(numRels));
	    	assertThat(desc.getNumPlans(), is(numPlans[q]));
	    	
	    	for (int k = 0; k < desc.getNumPlans(); k++) {
	    		if (q == 0){
	    			assertThat(desc.getInternalPlanCost(k), is(internalCostPlan1[k]));
	    		} else if (q == 1) {
	    			assertThat(desc.getInternalPlanCost(k), is(internalCostPlan2[k]));
	    		}
	    	}
	    	
	    	for (int i = 0; i < numRels; i++) {
	    		assertThat(desc.getNumIndexesEachSlot(i), is(3));
	    	}
	    	
	    	int p = 0;
	    	for (int k = 0; k < desc.getNumPlans(); k++) {
	    		for (int i = 0; i < numRels; i++) {
	    			for (int a = 0; a < desc.getNumIndexesEachSlot(i); a++){
	    				
	    				if (a == desc.getNumIndexesEachSlot(i) - 1) {
	    					assertThat(desc.getIndexAccessCost(k, i, a), is(100.0));
	    				}
	    				else {
		    				if (q == 0) {
		    					assertThat(desc.getIndexAccessCost(k, i, a), is(accessCostPlan1[p]));
		    				}
		    				else if (q == 1) {
		    					assertThat(desc.getIndexAccessCost(k, i, a), is(accessCostPlan2[p]));
		    				}
		    				p++;
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
    		/*
    		String file = environment.getTempDir() + "/test.wl";;
    		Reader reader = new BufferedReader(new FileReader(file));
    		Workload W = new Workload(reader);
    		*/
    		agent = mock(BIPAgent.class);
    		when(agent.populateInumSpace()).thenReturn(listInum);    		
    		double delta = 0.4;
    		Configuration C = new Configuration(candidateIndexes);    		
    		List<IndexInteraction> listInteractions = bip.getInteractionIndexes(agent, C, delta);
    		System.out.println(bip.printListInteraction(listInteractions));
    	} catch (SqlException e){
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

