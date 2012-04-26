package edu.ucsc.dbtune.advisor.wfit;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;

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

        wfit.process(new SQLStatement("UPDATE one_table.tbl set a = 3 where a < 0"));

        assertThat(wfit.getRecommendation().isEmpty(), is(true));

        WFALog log =
            WFALog.generateDynamic(
                wfit.qinfos.toArray(new AnalyzedQuery[0]),
                wfit.recs.toArray(new BitSet[0]));

        log.dump();
    }
}
