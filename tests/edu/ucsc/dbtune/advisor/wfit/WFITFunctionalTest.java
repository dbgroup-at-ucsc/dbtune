package edu.ucsc.dbtune.advisor.wfit;

import java.util.Set;

import com.google.common.collect.Iterables;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.RecommendationStatistics;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.optimizer.MySQLOptimizer;
import edu.ucsc.dbtune.optimizer.PGOptimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.OptimizerUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

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
    private static boolean isOptimizerOK;

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

        if (db.getOptimizer() instanceof IBGOptimizer &&
                (db.getOptimizer().getDelegate() instanceof DB2Optimizer ||
                 db.getOptimizer().getDelegate() instanceof MySQLOptimizer ||
                 db.getOptimizer().getDelegate() instanceof PGOptimizer))
            isOptimizerOK = true;
        else
            isOptimizerOK = false;
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
        if (!isOptimizerOK)
            return;

        db.getCatalog().dropIndexes();

        WFIT wfit = new WFIT(db);

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
        if (!isOptimizerOK)
            return;

        db.getCatalog().dropIndexes();

        Workload wl =
            new Workload(
                env.getWorkloadsFoldername() + 
                "/../../workloads/db2/kaizen-demo/advisor-workload.sql");

        Set<Index> candidateSet = candGen.generate(wl);

        WFIT wfit = new WFIT(db, candidateSet);

        wl = 
            new Workload(
                env.getWorkloadsFoldername() + "/../../workloads/db2/kaizen-demo/scenario-1.sql");

        wfit.process(wl);

        RecommendationStatistics wfitStats = wfit.getRecommendationStatistics();
        RecommendationStatistics optStats = wfit.getOptimalRecommendationStatistics();
        
        assertThat(wfitStats.getTotalWorkSum(), greaterThan(optStats.getTotalWorkSum()));

        System.out.println("WFIT\n" + wfit.getRecommendationStatistics());
        System.out.println("OPT\n" + wfit.getOptimalRecommendationStatistics());
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testCandidateSetPartitioning() throws Exception
    {
        if (!isOptimizerOK)
            return;

        db.getCatalog().dropIndexes();

        WFIT wfit = new WFIT(db);

        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE b = 2"));

        assertThat(wfit.getStablePartitioning().size(), is(1));

        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));

        assertThat(wfit.getStablePartitioning().size(), is(1));
        assertThat(Iterables.get(wfit.getStablePartitioning(), 0).size(), is(2));

        db.getCatalog().dropIndexes();

        wfit = new WFIT(db);

        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));

        assertThat(wfit.getStablePartitioning().size(), is(1));

        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE b = 2"));

        assertThat(wfit.getStablePartitioning().size(), is(2));
        assertThat(Iterables.get(wfit.getStablePartitioning(), 0).size(), is(1));
        assertThat(Iterables.get(wfit.getStablePartitioning(), 1).size(), is(1));
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testVoting() throws Exception
    {
        if (!isOptimizerOK)
            return;

        WFIT wfit;

        // test GOOD negative vote
        db.getCatalog().dropIndexes();

        wfit = new WFIT(db);

        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT c FROM one_table.tbl WHERE c = 2"));
        wfit.process(new SQLStatement("SELECT c FROM one_table.tbl WHERE c = 2"));
        wfit.process(new SQLStatement("SELECT d FROM one_table.tbl WHERE d = 2"));
        wfit.process(new SQLStatement("SELECT d FROM one_table.tbl WHERE d = 2"));

        assertThat(wfit.getRecommendation().size(), is(4));

        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+1 WHERE a BETWEEN        0 AND  500000"));
        assertThat(wfit.getRecommendation().size(), is(4));
        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+2 WHERE a BETWEEN  500000 AND  900000"));
        assertThat(wfit.getRecommendation().size(), is(4));
        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+3 WHERE a BETWEEN  9000000 AND 11000000"));
        assertThat(wfit.getRecommendation().size(), is(1));

        double totalWorkNoVoting = wfit.getRecommendationStatistics().getTotalWorkSum();

        db.getCatalog().dropIndexes();

        wfit = new WFIT(db);

        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT c FROM one_table.tbl WHERE c = 2"));
        wfit.process(new SQLStatement("SELECT c FROM one_table.tbl WHERE c = 2"));
        wfit.process(new SQLStatement("SELECT d FROM one_table.tbl WHERE d = 2"));
        wfit.process(new SQLStatement("SELECT d FROM one_table.tbl WHERE d = 2"));

        assertThat(wfit.getRecommendation().size(), is(4));

        wfit.voteDown(1);
        wfit.voteDown(2);
        wfit.voteDown(3);

        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+1 WHERE a BETWEEN        0 AND  500000"));
        assertThat(wfit.getRecommendation().size(), is(1));
        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+2 WHERE a BETWEEN  500000 AND  900000"));
        assertThat(wfit.getRecommendation().size(), is(1));
        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+3 WHERE a BETWEEN  9000000 AND 11000000"));
        assertThat(wfit.getRecommendation().size(), is(1));

        double totalWorkVoting = wfit.getRecommendationStatistics().getTotalWorkSum();

        assertThat(totalWorkNoVoting, greaterThan(totalWorkVoting));

        

        // test GOOD positive vote
        db.getCatalog().dropIndexes();

        wfit = new WFIT(db);

        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT c FROM one_table.tbl WHERE c = 2"));
        wfit.process(new SQLStatement("SELECT c FROM one_table.tbl WHERE c = 2"));
        wfit.process(new SQLStatement("SELECT d FROM one_table.tbl WHERE d = 2"));
        wfit.process(new SQLStatement("SELECT d FROM one_table.tbl WHERE d = 2"));

        assertThat(wfit.getRecommendation().size(), is(4));

        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+1 WHERE a BETWEEN        0 AND  500000"));
        assertThat(wfit.getRecommendation().size(), is(4));
        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+2 WHERE a BETWEEN  500000 AND  900000"));
        assertThat(wfit.getRecommendation().size(), is(4));
        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+3 WHERE a BETWEEN  9000000 AND 11000000"));
        assertThat(wfit.getRecommendation().size(), is(1));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT c FROM one_table.tbl WHERE c = 2"));
        wfit.process(new SQLStatement("SELECT d FROM one_table.tbl WHERE d = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT d FROM one_table.tbl WHERE d = 2"));
        wfit.process(new SQLStatement("SELECT c FROM one_table.tbl WHERE c = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        assertThat(wfit.getRecommendation().size(), is(4));

        totalWorkNoVoting = wfit.getRecommendationStatistics().getTotalWorkSum();

        db.getCatalog().dropIndexes();

        wfit = new WFIT(db);

        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT c FROM one_table.tbl WHERE c = 2"));
        wfit.process(new SQLStatement("SELECT c FROM one_table.tbl WHERE c = 2"));
        wfit.process(new SQLStatement("SELECT d FROM one_table.tbl WHERE d = 2"));
        wfit.process(new SQLStatement("SELECT d FROM one_table.tbl WHERE d = 2"));

        assertThat(wfit.getRecommendation().size(), is(4));

        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+1 WHERE a BETWEEN        0 AND  500000"));
        assertThat(wfit.getRecommendation().size(), is(4));
        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+2 WHERE a BETWEEN  500000 AND  900000"));
        assertThat(wfit.getRecommendation().size(), is(4));
        wfit.voteUp(1);
        wfit.voteUp(2);
        wfit.voteUp(3);
        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+3 WHERE a BETWEEN  9000000 AND 11000000"));
        assertThat(wfit.getRecommendation().size(), is(4));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT c FROM one_table.tbl WHERE c = 2"));
        wfit.process(new SQLStatement("SELECT c FROM one_table.tbl WHERE c = 2"));
        wfit.process(new SQLStatement("SELECT d FROM one_table.tbl WHERE d = 2"));
        wfit.process(new SQLStatement("SELECT d FROM one_table.tbl WHERE d = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT c FROM one_table.tbl WHERE c = 2"));
        wfit.process(new SQLStatement("SELECT c FROM one_table.tbl WHERE c = 2"));
        wfit.process(new SQLStatement("SELECT d FROM one_table.tbl WHERE d = 2"));
        wfit.process(new SQLStatement("SELECT d FROM one_table.tbl WHERE d = 2"));


        totalWorkVoting = wfit.getRecommendationStatistics().getTotalWorkSum();

        assertThat(totalWorkNoVoting, greaterThan(totalWorkVoting));

        // test recovery (BAD negative vote)
        db.getCatalog().dropIndexes();

        wfit = new WFIT(db);

        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));

        assertThat(wfit.getRecommendation().size(), is(1));

        wfit.voteDown(0);

        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));

        assertThat(wfit.getRecommendation().isEmpty(), is(true));

        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));

        assertThat(wfit.getRecommendation().size(), is(1));

        // test recovery (BAD positive vote)
        db.getCatalog().dropIndexes();

        wfit = new WFIT(db);

        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));
        wfit.process(new SQLStatement("SELECT b FROM one_table.tbl WHERE b = 2"));

        assertThat(wfit.getRecommendation().size(), is(2));

        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+1 WHERE a BETWEEN        0 AND  500000"));
        assertThat(wfit.getRecommendation().size(), is(2));
        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+2 WHERE a BETWEEN  500000 AND  900000"));
        assertThat(wfit.getRecommendation().size(), is(2));
        wfit.voteUp(0);
        wfit.process(
                new SQLStatement(
                    "UPDATE one_table.tbl set a = a+3 WHERE a BETWEEN  9000000 AND 11000000"));
        assertThat(wfit.getRecommendation().size(), is(1));
    }
}
