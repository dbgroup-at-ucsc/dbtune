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
import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.metadata.Index; 
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BIPTestConfiguration 
{
    /*
    protected static DatabaseSystem db;
    protected static Environment    en;
    
    DatabaseSystem db = DatabaseSystem.newDatabaseSystem();

        assertThat(db.getConnection() != null, is(true));
        assertThat(db.getOptimizer() != null, is(true));
        assertThat(db.getCatalog() != null, is(true));

        db.getConnection().close();
        for (SQLStatement sql : workload) {
            allIndexes.addAll(db.getOptimizer().recommendIndexes(sql));
        }
        
        workloadFile   = en.getScriptAtWorkloadsFolder("one_table/workload.sql");        
        fileReader     = new FileReader(workloadFile);
        workload       = new Workload(fileReader);
    */
}
