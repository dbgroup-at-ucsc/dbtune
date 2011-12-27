package edu.ucsc.dbtune.bip;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import edu.ucsc.dbtune.bip.util.*;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.DBTuneInstances;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.inum.InumStatementPlan;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.metadata.Index; 
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BIPTestConfiguration 
{
    /**
     * Test case with two schemas:
     *      + Each schema has three relations
     *      + Each relation has two candidate indexes
     *      
     * Workload:
     *      + Two queries, each query belongs to a different schema
     *      + Each query involves two relations
     *      + The first query has three plans (in the INUM space)
     *      + The second query has two plans (in the INUM space)
     */ 
    protected static Catalog cat;
    protected static List<Schema> listSchema;
    protected static int numCandidateIndexes;
    
    protected static double[] internalCostPlan1 = {140, 100, 160};
    protected static double[] accessCostPlan1 = {10, 30, 20, 50, 80, 5, 90, 10, 20, 40, 30, 80};
    protected static double[] internalCostPlan2 = {200, 150};
    protected static double[] accessCostPlan2 = {20, 45, 30, 70, 80, 15, 90, 20};
    protected static long[] sizeIndexPlan1 = {100, 80, 120, 150};
    protected static long[] sizeIndexPlan2 = {200, 120, 60, 70};
    protected static double[] timeCreateIndexPlan1 = {100, 80, 120, 150};
    protected static double[] timeCreateIndexPlan2 = {200, 120, 60, 70};
    protected static List<Index> candidateIndexes;
    protected static int[] numPlans = {3, 2};
    protected static int numQ = 2;
    protected static int numSch = 2;
    protected static int numSchemaTables = 3;
    protected static Environment environment = Environment.getInstance();
    protected static Map<Schema, Workload> mapSchemaToWorkload = new HashMap<Schema, Workload>(); 
    protected static List<BIPPreparatorSchema> listPreparators = new ArrayList<BIPPreparatorSchema>();
    
    protected static List< List<Index> > listIndexQueries = new ArrayList<List<Index>>();
    protected static List< List<Table> > listTableQueries = new ArrayList<List<Table>>();            
    
    static {
        try {
            cat = DBTuneInstances.configureCatalog(numSch, numSchemaTables, 2); 
            listSchema = cat.schemas(); 
            
            // create two workloads, each corresponds to one schema and contains one query
            for (int q = 0; q < 2; q++) {
                String workloadName = environment.getTempDir() + "/testwl" + q + ".wl";
                PrintWriter writer = new PrintWriter(new FileWriter(workloadName));
                String sql = "SELECT * FROM R";
                writer.println(sql);
                writer.close();
                
                BufferedReader reader = new BufferedReader(new FileReader(workloadName));
                Workload wl = new Workload(reader);
                mapSchemaToWorkload.put(listSchema.get(q), wl);
            }
            
            // list of candidate indexes
            candidateIndexes = new ArrayList<Index>();
            numCandidateIndexes = 0;
            for (Schema sch : listSchema) {
                for (Index index : sch.indexes()) {
                    numCandidateIndexes++;
                    candidateIndexes.add(index);
                }
            }
            System.out.println("Number of candidate indexes " + numCandidateIndexes);
            int q = 0;
            for (Entry<Schema, Workload> entry : mapSchemaToWorkload.entrySet()) {
                int idxTable = 0;
                int idxLocalIndex = 0;
                List<Index> listIndex = new ArrayList<Index>();
                List<Table> listTable = new ArrayList<Table>();
                
                for (Table table : entry.getKey().tables()) {
                    // the first query: 1st and 2nd relations of the first schema
                    // the second query: 2nd and 3rd relations of the second schema
                    if ( (q == 0 && idxTable <= 1) 
                       || (q == 1 && idxTable >= 1)){
                        listTable.add(table);
                        for (Index index : candidateIndexes) {
                            if (index.getTable().equals(table)){
                                double time = 0.0;
                                long size = 0;
                                if (q == 0) {
                                    size = sizeIndexPlan1[idxLocalIndex];
                                    time = timeCreateIndexPlan1[idxLocalIndex];
                                    idxLocalIndex++;
                                } else if (q == 1) {
                                    size = sizeIndexPlan2[idxLocalIndex];
                                    time = timeCreateIndexPlan2[idxLocalIndex];
                                    idxLocalIndex++;
                                }
                                index.setCreationCost(time);
                                index.setBytes(size);
                                
                                listIndex.add(index);
                            }
                        }
                    } 
                    idxTable++;
                }
                listIndexQueries.add(listIndex);
                listTableQueries.add(listTable);
                q++;
            }
                
            q = 0;
            for (Entry<Schema, Workload> entry : mapSchemaToWorkload.entrySet()) {
                BIPPreparatorSchema preparator = mock(BIPPreparatorSchema.class);
                List<Table> listTables = new ArrayList<Table>();
                List<IndexFullTableScan> listIndexFullTableScan = new ArrayList<IndexFullTableScan>();
                for (Table table : entry.getKey().tables()) {
                    listTables.add(table);
                    IndexFullTableScan scanIdx = new IndexFullTableScan(table);
                    listIndexFullTableScan.add(scanIdx);
                }
                when (preparator.getListSchemaTables()).thenReturn(listTables);
                when (preparator.getListFullTableScanIndexes()).thenReturn(listIndexFullTableScan);
                
                // run over each statement in the workload
                for (Iterator<SQLStatement> iterStmt = entry.getValue().iterator(); iterStmt.hasNext(); ){
                    SQLStatement stmt = iterStmt.next();
                    InumSpace inum = mock(InumSpace.class);                    
                    Set<InumStatementPlan> templatePlans = new LinkedHashSet<InumStatementPlan>();
                
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
                       
                        for (Index index : listIndexQueries.get(q)) {   
                            double val = 0.0;
                            
                            if (q == 0) {
                                val = accessCostPlan1[counter];
                            }
                            else if (q == 1) {
                                val = accessCostPlan2[counter];
                            }
                            when (plan.getAccessCost(index)).thenReturn(val);
                            counter++;              
                        }
                        
                        for (Table table : listTableQueries.get(q)) {
                            when (plan.getFullTableScanCost(table)).thenReturn(100.0);
                        }
                        when (plan.getReferencedTables()).thenReturn(listTableQueries.get(q));    
                        templatePlans.add(plan);               
                    }
                    
                    when(inum.getTemplatePlans()).thenReturn(templatePlans);
                    when(preparator.populateInumSpace(stmt)).thenReturn(inum);
                }
                listPreparators.add(preparator);
                q++;
            }
            
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }
    }
    
}
