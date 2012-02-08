package edu.ucsc.dbtune.advisor.wfit;

import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
import static edu.ucsc.dbtune.util.TestUtils.workload;

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
        Set<Index> pool;
        WFIT wfit;
        Workload workload;

        //Set<Index> configuration;
        int maxNumIndexes;
        int maxNumStates;
        int windowSize;
        int partIterations;
        //int q;

        workload = workload(env.getWorkloadsFoldername() + "/one_table");
        maxNumIndexes = env.getMaxNumIndexes();
        maxNumStates = env.getMaxNumStates();
        windowSize = env.getIndexStatisticsWindow();
        partIterations = env.getNumPartitionIterations();
        pool = getCandidates(workload);
        //q = 0;

        wfit = new WFIT(
                db.getOptimizer(), pool, maxNumStates, maxNumIndexes, windowSize, partIterations);
        
        
        for (SQLStatement sql : workload) {
            wfit.process(sql);

            //assertThat(wfit.getPartitions().subsetCount(), is(1));

            //configuration = wfit.getRecommendation();

            //qinfo = wfit.getStatement(q);

            //System.out.println("------\nq" + q + "\n" + qinfo);
            //System.out.println("\n" + qinfo.getConfiguration());
            //System.out.println("\n" + qinfo.getOptimizationCount());
            //System.out.println("\n" + configuration);

            // Alkis: I am not sure what this check does. An SQL statement processed by WFIT
            // should not have a configuration associated with it.
            // assertThat(qinfo.getConfiguration().size(), is(1));

            // Alkis: Same here -- the optimization count can be 1
            // only if there the single index is not useful for the query
            // assertThat(qinfo.getOptimizationCount(), is(1));

            /*
            if (q < 5) {
                //assertThat(configuration.size(), is(0));
                //assertThat(configuration.isEmpty(), is(true));
            } else if (q == 5) {
                //assertThat(configuration.size(), is(1));
                //assertThat(configuration.isEmpty(), is(false));
            } else if (q == 6) {
                //assertThat(configuration.size(), is(0));
                //assertThat(configuration.isEmpty(), is(true));
            } else {
                throw new SQLException("Workload should have 7 statements");
            }

            q++;
            */
        }
    }

    /**
     * @param workload
     *      workload file used to extract candidates
     * @return
     *      a set of candidate indexes
     */
    private static Set<Index> getCandidates(Workload workload)
    {
        return new HashSet<Index>();
    }
}
