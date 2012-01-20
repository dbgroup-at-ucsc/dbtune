package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.metadata.FullTableScanIndex.getFullTableScanIndexInstance;

import java.io.FileReader;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.bip.core.BIPOutput;
import edu.ucsc.dbtune.bip.interactions.InteractionBIP;
import edu.ucsc.dbtune.metadata.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

/**
 * Test for the InteractionBIP class
 */
public class InteractionBIPTest extends BIPTestConfiguration
{  
    private static DatabaseSystem db;
    private static Environment    en;
    
    @Test
    public void testInteraction() throws Exception
    {
        en  = Environment.getInstance();
        db = newDatabaseSystem(en);
        
        System.out.println(" In test interaction ");
        String workloadFile   = en.getScriptAtWorkloadsFolder("tpch/smallworkload.sql");
        FileReader fileReader = new FileReader(workloadFile);
        Workload workload     = new Workload(fileReader);
        
        Set<Index> allIndexes = new HashSet<Index>();
        for (SQLStatement stmt : workload) {
            System.out.println("==== Query: " + stmt.getSQL());
            Set<Index> stmtIndexes = db.getOptimizer().recommendIndexes(stmt); 
            for (Index newIdx : stmtIndexes) {
                boolean exist = false;
                for (Index oldIdx : allIndexes) {
                    if (oldIdx.equalsContent(newIdx)) {
                        exist = true;
                        break;
                    }
                }
                if (exist == false) {
                    allIndexes.add(newIdx);
                }
            }
        }
         
        System.out.println("Number of indexes: " + allIndexes.size());
        for (Index index : allIndexes) {
            System.out.println("Index : " + index.columns()); 
        }
        
        
        try {
            double delta = 0.1;
            Optimizer io = db.getOptimizer();

            if (!(io instanceof InumOptimizer))
                throw new Exception("Expecting InumOptimizer instance");

            InteractionBIP bip = new InteractionBIP(delta);
            bip.setCandidateIndexes(allIndexes);
            bip.setWorkload(workload);
            bip.setOptimizer((InumOptimizer)io);
            
            BIPOutput output = bip.solve();
            System.out.println(output.toString());
        } catch (SQLException e){
            System.out.println(" error " + e.getMessage());
        }
        
        /*
        // Add full table scan indexes into the pool
        for (Table table : db.getCatalog().schemas().get(1).tables()) {
            FullTableScanIndex scanIdx = getFullTableScanIndexInstance(table);
            System.out.println(" index name " + scanIdx.getFullyQualifiedName());
            Table t1 = scanIdx.getTable();
            System.out.println(" table name: " + t1.getFullyQualifiedName());
        }
        */
    }
}
