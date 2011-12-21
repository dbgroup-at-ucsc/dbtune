package edu.ucsc.dbtune.bip;


import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import edu.ucsc.dbtune.bip.util.*;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.DBTuneInstances;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.inum.InumStatementPlan;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.metadata.Index; 

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BIPTestConfiguration 
{
    /**
     * Test case with two queries
     * Each query consists of three (resp. two) query plans, two relations, and two indexes per relation
     *  
     */ 
    protected static List<InumSpace> listInum;
    protected static InumSpace inum;
    protected static Set<InumStatementPlan> templatePlans;
    protected static List<Table> listSchemaTables;
    protected static Catalog cat;
    protected static Schema sch;
    protected static int numSchemaTables = 3;
    protected static int numCandidateIndexes;
    
    protected static double[] internalCostPlan1 = {140, 100, 160};
    protected static double[] accessCostPlan1 = {10, 30, 20, 50, 80, 5, 90, 10, 20, 40, 30, 80};
    protected static double[] sizeIndexPlan1 = {100, 80, 120, 150};
    protected static double[] sizeIndexPlan2 = {200, 120, 60, 70};
    protected static double[] internalCostPlan2 = {200, 150};
    protected static double[] accessCostPlan2 = {20, 45, 30, 70, 80, 15, 90, 20};
    protected static List<Index> candidateIndexes;
    protected static int[] numPlans = {3, 2};
    protected static int numQ = 2;
    protected static BIPAgent agent;
    
    /**
     * Initialize schema  & workload
     *   - Schema: three relations, 2 indexes per relation
     *   - Workload: two queries:
     *      + The first query: references the first and second relation
     *          ~ INUM space: three plans
     *          ~ The associated costs are given in internalPlan, size, ... 1
     *      + The second query: references the second and third relation
     *          ~ INUM space: two plans
     *          ~ The associated costs are given in internalPlan, size, ... 1
     */
    static {
        try {
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
    
}
