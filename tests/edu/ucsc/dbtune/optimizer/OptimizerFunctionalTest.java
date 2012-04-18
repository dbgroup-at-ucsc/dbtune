package edu.ucsc.dbtune.optimizer;

import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
import static edu.ucsc.dbtune.util.TestUtils.workloads;

/**
 * Functional test for optimizer implementations. The optimizer being tested is specified by the 
 * {@link EnvironmentProperties#OPTIMIZER} property.
 * <p>
 * This test executes all the tests for which {@link OptimizerTest} relies on DBMS-specific mocks 
 * (eg. classes contained in {@link java.sql}).
 *
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 * @see OptimizerTest
 */
public class OptimizerFunctionalTest
{
    private static DatabaseSystem db;
    private static Optimizer opt;
    private static Environment env;

    /**
     * @throws Exception
     *      if something goes wrong
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        env = Environment.getInstance();
        db  = newDatabaseSystem(env);
        opt = db.getOptimizer();

        loadWorkloads(db.getConnection());
    }

    /**
     * @throws Exception
     *      if something goes wrong
     */
    @AfterClass
    public static void afterClass() throws Exception
    {
        db.getConnection().close();
    }

    /**
     * @throws Exception
     *      if something goes wrong
     * @see OptimizerTest#checkFTSDisabled
     */
    @Test
    public void testFTSDisabled() throws Exception
    {
        OptimizerTest.checkFTSDisabled(db.getCatalog(), opt);
    }

    /**
     * @throws Exception
     *      if something goes wrong
     * @see OptimizerTest#checkPreparedExplain
     */
    @Test
    public void testPrepared() throws Exception
    {
        OptimizerTest.checkPreparedExplain(db.getCatalog(), opt);
    }

    /**
     * @throws Exception
     *      if something goes wrong
     * @see OptimizerTest#checkExplain
     */
    @Test
    public void testExplain() throws Exception
    {
        OptimizerTest.checkExplain(opt);
    }

    /**
     * @throws Exception
     *      if something goes wrong
     * @see OptimizerTest#checkWhatIfExplain
     */
    @Test
    public void testWhatIfExplain() throws Exception
    {
        OptimizerTest.checkWhatIfExplain(db.getCatalog(), opt);
    }

    /**
     * @throws Exception
     *      if something goes wrong
     * @see OptimizerTest#checkRecommendIndexes
     */
    @Test
    public void testRecommendIndexes() throws Exception
    {
        OptimizerTest.checkRecommendIndexes(opt);
    }

    /**
     * Checks that each supported optimizer is well behaved.
     *
     * @throws Exception
     *      if something goes wrong
     * @see OptimizerTest#checkIsWellBehaved
     */
    @Test
    public void testIsWellBehaved() throws Exception
    {
        OptimizerTest.checkIsWellBehaved(opt);
    }

    /**
     * @throws Exception
     *      if something goes wrong
     * @see OptimizerTest#checkMonotonicity
     */
    @Test
    public void testMonotonicity() throws Exception
    {
        OptimizerTest.checkMonotonicity(db.getCatalog(), opt);
    }

    /**
     * @throws Exception
     *      if something goes wrong
     * @see OptimizerTest#checkSanity
     */
    @Test
    public void testSanity() throws Exception
    {
        OptimizerTest.checkSanity(db.getCatalog(), opt);
    }

    /**
     * Checks that all supported workloads can be explained through the Optimizer interface.
     *
     * @throws Exception
     *      if fails
     */
    @Test
    public void testSupported() throws Exception
    {
        CandidateGenerator candGen = CandidateGenerator.Factory.newCandidateGenerator(env, opt);
        
        for (Workload wl : workloads(env.getWorkloadFolders())) {

            final Set<Index> allIndexes = candGen.generate(wl);

            for (SQLStatement sql : wl)
                if (sql.getSQLCategory().isSame(SQLCategory.NOT_SELECT) &&
                        (opt instanceof MySQLOptimizer || opt instanceof IBGOptimizer))
                    // XXX: issue #106, #144
                    continue;
                else
                    opt.explain(sql, allIndexes);
        }
    }
}
