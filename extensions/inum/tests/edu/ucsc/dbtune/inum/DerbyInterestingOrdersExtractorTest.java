package edu.ucsc.dbtune.inum;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for DerbyInterestingOrdersExtractorTest.
 *
 * @author Ivo Jimenez
 */
public class DerbyInterestingOrdersExtractorTest
{
    private static DerbyInterestingOrdersExtractor extractor;

    /**
     * Setup for the test.
     */
    @BeforeClass
    public static void beforeClassSetUp()
    {
        
        
    }

    /**
     * Setup for each test.
     */
    @Before
    public void setUp()
    {
    }

    /**
     * @throws SQLException 
     *      when the connection cannot be established
     */
    @Test
    public void testSupported() throws Exception
    {
        Environment en  = Environment.getInstance();
        DatabaseSystem db = newDatabaseSystem(en);
        
        System.out.println(" In test derby interesting order ");
        String workloadFile   = en.getScriptAtWorkloadsFolder("tpch/smallworkload.sql");
        FileReader fileReader = new FileReader(workloadFile);
        Workload workload     = new Workload(fileReader);        
        DerbyInterestingOrdersExtractor ioE;
        List<Set<Index>> indexesPerTable;

        ioE = new DerbyInterestingOrdersExtractor(db.getCatalog(), true);
        for (Iterator<SQLStatement> iterStmt = workload.iterator(); iterStmt.hasNext(); ) {
            indexesPerTable = ioE.extract(iterStmt.next());
            System.out.println(" index per table: " + indexesPerTable.toString());
        }
        System.out.println("After extracting intersting orders. ");
    }
}
