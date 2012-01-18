package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import java.io.FileReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


import org.junit.Test;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;
import edu.ucsc.dbtune.bip.core.BIPOutput;
import edu.ucsc.dbtune.bip.sim.SimBIP;


/**
 * Test SimBIP
 * @author tqtrung
 *
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
        String workloadFile   = en.getScriptAtWorkloadsFolder("tpch/workload.sql");        
        System.out.println(" file " + workloadFile);
        FileReader fileReader = new FileReader(workloadFile);
        Workload workload     = new Workload(fileReader);
        
        Set<Index> allIndexes = new HashSet<Index>();
        for (SQLStatement sql : workload) {
            allIndexes.addAll(db.getOptimizer().recommendIndexes(sql));
        }
         
        for (Index index : allIndexes) {
            System.out.println(" Index " + index.getFullyQualifiedName());
        }
        
        Set<Index> Sinit = new HashSet<Index>();
        Set<Index> Smat = new HashSet<Index>();
        int W = 4;
        double timeLimit = 300;
        Smat = allIndexes;
        
        // Sinit = \emptyset
        SimBIP sim = new SimBIP(Sinit, Smat, W, timeLimit);
        Map<Schema, Workload> mapSchemaToWorkload = workload.getSchemaToWorkloadMapping();
        sim.setSchemaToWorkloadMapping(mapSchemaToWorkload);
        sim.setWorkloadName(workloadFile);
        BIPOutput schedule = sim.solve();
        System.out.println("Result: " + schedule.toString());
        
    }
}
