package edu.ucsc.dbtune.bip;

import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.bip.core.BIPOutput;
import edu.ucsc.dbtune.bip.sim.SimBIP;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

/**
 * Test SimBIP.
 *
 * @author tqtrung
 */
public class SimBIPTest 
{   
    private static DatabaseSystem db;
    private static Environment    en;

    /**
     * The test has to check first just one query and one index.     
     * 
     * @throws Exception
     *      if an I/o error occurs; if a DBMS communication failure occurs
     */
    @Test
    public void testScheduling() throws Exception
    {
        en  = Environment.getInstance();
        db = newDatabaseSystem(en);
        
        System.out.println(" In test scheduling ");
        String workloadFile   = en.getScriptAtWorkloadsFolder("tpch/workload_bip.sql");
        FileReader fileReader = new FileReader(workloadFile);
        Workload workload     = new Workload(fileReader);
        
        Set<Index> allIndexes = new HashSet<Index>();
        for (SQLStatement stmt : workload) {
            //System.out.println("==== Query: " + stmt.getSQL());
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
         
        System.out.println("Number of indexes: " + allIndexes.size() + " number of statements: "
                            + workload.size());
        for (Index index : allIndexes) {
            System.out.println("Index : " + index.columns()); 
        }
        
        Set<Index> Sinit = new HashSet<Index>();
        Set<Index> Smat = new HashSet<Index>();
        Smat = allIndexes;
        
        Optimizer io = db.getOptimizer();

        if (!(io instanceof InumOptimizer))
            throw new Exception("Expecting InumOptimizer instance");
        
        LogListener logger = LogListener.getInstance();
        SimBIP bip = new SimBIP();
        bip.setOptimizer((InumOptimizer) io);
        bip.setWorkload(workload);
        bip.setLogListenter(logger);
        bip.setConfigurations(Sinit, Smat);
        bip.setNumberofIndexesEachWindow(1);
        bip.setNumberWindow(allIndexes.size());
        
        BIPOutput schedule = bip.solve();
        if (schedule != null) {
            System.out.println("Result: " + schedule.toString());
            System.out.println(logger.toString());
        }
    }
}
