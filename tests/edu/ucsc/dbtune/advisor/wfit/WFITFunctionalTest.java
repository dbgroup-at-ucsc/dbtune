package edu.ucsc.dbtune.advisor.wfit;

import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.RecommendationStatistics;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.OptimizerUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import static org.junit.Assert.assertThat;

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
    private static CandidateGenerator candGen;

    /**
     * @throws Exception
     *      if the workload can't be loaded
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        env = Environment.getInstance();
        db = newDatabaseSystem(env);
        candGen =
            CandidateGenerator.Factory.newCandidateGenerator(
                    env, getBaseOptimizer(db.getOptimizer()));
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
     * gets recommended and dropped.
     *
     * @throws Exception
     *      if fails
     */
    @Test
    public void testBasic() throws Exception
    {
        if (!(db.getOptimizer() instanceof IBGOptimizer))
            return;

        WFIT wfit = new WFIT((IBGOptimizer) db.getOptimizer());

        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));

        assertThat(wfit.getRecommendation().isEmpty(), is(true));

        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        
        assertThat(wfit.getRecommendation().size(), is(1));

        wfit.process(new SQLStatement("UPDATE one_table.tbl set a = a+1 where a < 0"));

        assertThat(wfit.getRecommendation().isEmpty(), is(true));
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testOPTGeneration() throws Exception
    {
        if (!(db.getOptimizer() instanceof IBGOptimizer))
            return;

        Workload wl =
            new Workload(
                env.getWorkloadsFoldername() + 
                "/../../workloads/db2/kaizen-demo/advisor-workload.sql");

        Set<Index> candidateSet = candGen.generate(wl);

        WFIT wfit = new WFIT((IBGOptimizer) db.getOptimizer(), candidateSet);

        wl = workload(env.getWorkloadsFoldername() + "/../../workloads/db2/kaizen-demo/");

        wfit.process(wl);

        RecommendationStatistics wfitStats = wfit.getRecommendationStatistics();
        RecommendationStatistics optStats = wfit.getOptimalRecommendationStatistics();
        
        for (int i = 0; i < wfitStats.size(); i++) {
            if (i <= 1)
                assertThat(
                        wfitStats.get(i).getTotalWork(),
                        lessThanOrEqualTo(optStats.get(i).getTotalWork()));
            else
                assertThat(
                        wfitStats.get(i).getTotalWork(), 
                        greaterThan(optStats.get(i).getTotalWork()));
        }

        System.out.println("WFIT\n" + wfit.getRecommendationStatistics());
        System.out.println("OPT\n" + wfit.getOptimalRecommendationStatistics());
    }
}
