package edu.ucsc.dbtune.bip;

import java.sql.Connection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import edu.ucsc.dbtune.metadata.Catalog;

import edu.ucsc.dbtune.DBTuneInstances;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.inum.InumStatementPlan;
import edu.ucsc.dbtune.metadata.Schema;

import static org.junit.Assert.assertThat;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
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
	private static InumSpace inum;
	private static Set<InumStatementPlan> templatePlans;
	private static Catalog cat;
	private static Schema sch;
	
	static {
		try {
			cat = DBTuneInstances.configureCatalog();
			sch = (Schema)cat.at(0);
			inum = mock(InumSpace.class);
			templatePlans = new LinkedHashSet<InumStatementPlan>();
			
			for (int k = 0; k < 3; k++)
			{
				InumStatementPlan p = mock(InumStatementPlan.class);
				
				if (k == 0)
				{
					when(p.getInternalCost()).thenReturn(100.0);
					when(p.getAccessCost(sch.getBaseConfiguration().toList().get(0))).thenReturn(10.0);
				}
				
				
				templatePlans.add(p); 
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
    	// list = new list;
    	// list.add(inum);
    	// bip = new BIP
    	// desc = bip.generatePlanDescription(inum);
    	// assertThat(desc.getNumberOfConstraints()), is(3));
    }
    
    @Test
    public void testRun() throws Exception
    {
    	
    }
}

