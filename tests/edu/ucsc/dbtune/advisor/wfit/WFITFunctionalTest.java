package edu.ucsc.dbtune.advisor.wfit;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.db2.DB2Advisor;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.MetadataUtils.getCreateStatement;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
import static edu.ucsc.dbtune.util.TestUtils.workloads;

/**
 * Functional test for the WFIT use case.
 * <p>
 * Some of the documentation contained in this class refers to sections of Karl Schnaitter's 
 * Doctoral Thesis "On-line Index Selection for Physical Database Tuning". Specifically to chapter 
 * 6.
 *
 * @see <a href="http://bit.ly/wXaQC3">
 *         "On-line Index Selection for Physical Database Tuning"
 *      </a>
 * @author Ivo Jimenez
 */
public class WFITFunctionalTest
{
    private static DatabaseSystem db;
    private static Environment env;

    /**
     * @throws Exception
     *      if the workload can't be loaded
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        env = Environment.getInstance();
        db = newDatabaseSystem(env);
        loadWorkloads(db.getConnection());
    }

    /**
     * @throws Exception
     *      if the connection can't be closed
     */
    @AfterClass
    public static void afterClass() throws Exception
    {
        db.getConnection().close();
    }

    /**
     * The test has to check first just one query and one index. We need to make sure that the index 
     * gets recommended and dropped consistently to what we expect.
     *
     * @throws Exception
     *      if an i/o error occurrs; if a DBMS communication failure occurs
     */
    @Test
    public void testWFIT() throws Exception
    {
        if (!(db.getOptimizer() instanceof IBGOptimizer))
            return;

        for (Workload wl : workloads(env.getWorkloadFolders())) {

            if (!wl.getName().endsWith("tpch-10-counts"))
                continue;

            DB2Advisor db2advis = new DB2Advisor(db);
            db2advis.process(wl);
            WFIT wfit = new WFIT((IBGOptimizer) db.getOptimizer(), db2advis.getRecommendation());

            for (SQLStatement sql : wl) {
                wfit.process(sql);
                System.out.println("Recommendation: ");
                for (Index i : wfit.getRecommendation())
                    System.out.println("   " + getCreateStatement(i));
            }
        }
    }
}
